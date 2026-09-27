using RabbitMQ.Component.Diagnostics;

namespace RabbitMQ.Component.Internal.Subscriptions;

internal abstract class ConsumerSession
{
    public abstract long Generation { get; }
    public abstract bool IsIdle { get; }
    public abstract bool IsOpen { get; }
    public abstract Task PrepareAsync(IReadOnlyList<BindingSnapshot> bindings, CancellationToken cancellationToken);
    public abstract Task DeclareAndBindAsync(BindingSnapshot binding, CancellationToken cancellationToken);
    public abstract void Activate();
    public abstract Task CloseAsync(TimeSpan drainTimeout, CancellationToken cancellationToken);
}

internal sealed class ConsumerSession<T> : ConsumerSession
{
    private static readonly AsyncLocal<ConsumerSession<T>?> CurrentCallback = new();
    private readonly object _stateGate = new();
    private readonly SemaphoreSlim _channelGate = new(1, 1);
    private readonly ISubscriptionChannel _channel;
    private readonly SubscriptionSnapshot<T> _options;
    private readonly ErrorSink _errors;
    private readonly TaskCompletionSource _activation = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _stopping = new();
    private readonly Func<bool> _isOwnerActive;
    private TaskCompletionSource? _idle;
    private Task? _closeTask;
    private string? _consumerTag;
    private int _inflight;
    private int _closing;

    public ConsumerSession(ISubscriptionChannel channel, long generation, SubscriptionSnapshot<T> options,
        ErrorSink errors, bool startImmediately, Func<bool> isOwnerActive, Action<ConsumerSession> onFailed)
    {
        _channel = channel;
        Generation = generation;
        _options = options;
        _errors = errors;
        _isOwnerActive = isOwnerActive;
        if (startImmediately) _activation.TrySetResult();
        // Completion retains failures that raced channel creation/consumer registration. Normal local
        // shutdown completes with null; recovery never runs inline on the RabbitMQ dispatcher.
        _ = channel.Completion.ContinueWith(completed =>
        {
            if (completed.Result is not { } failure || Volatile.Read(ref _closing) != 0) return;
            Report(ErrorStage.Consume, ErrorOutcome.Unknown, failure);
            onFailed(this);
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    public override long Generation { get; }
    public override bool IsIdle => Volatile.Read(ref _inflight) == 0;
    public override bool IsOpen => _channel.IsOpen && !_channel.Completion.IsCompleted;

    public override async Task PrepareAsync(IReadOnlyList<BindingSnapshot> bindings, CancellationToken cancellationToken)
    {
        await _channel.QueueDeclareAsync(_options.Queue, cancellationToken).ConfigureAwait(false);

        foreach (BindingSnapshot binding in bindings)
        {
            await _channel.ExchangeDeclareAsync(binding.Exchange, cancellationToken).ConfigureAwait(false);
            await _channel.QueueBindAsync(_options.Queue.Name, binding, cancellationToken).ConfigureAwait(false);
        }
        await _channel.BasicQosAsync(_options.PrefetchCount, cancellationToken).ConfigureAwait(false);
        _consumerTag = await _channel.BasicConsumeAsync(_options.Queue.Name, _options.Consumer, ProcessAsync,
            cancellationToken).ConfigureAwait(false);
    }

    public override async Task DeclareAndBindAsync(BindingSnapshot binding, CancellationToken cancellationToken)
    {
        await _channelGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _channel.ExchangeDeclareAsync(binding.Exchange, cancellationToken).ConfigureAwait(false);
            await _channel.QueueBindAsync(_options.Queue.Name, binding, cancellationToken).ConfigureAwait(false);
        }
        finally { _channelGate.Release(); }
    }

    public override void Activate() => _activation.TrySetResult();

    private async Task ProcessAsync(SubscriptionDelivery delivery, CancellationToken cancellationToken)
    {
        if (!TryEnterDelivery()) return;
        CurrentCallback.Value = this;
        try
        {
            try { await _activation.Task.WaitAsync(_stopping.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
            if (Volatile.Read(ref _closing) != 0 || !_isOwnerActive()) return;

            T message = default!;
            bool decoded = false;
            try
            {
                message = _options.Codec.Decode(delivery.Body.Span);
                decoded = true;
            }
            catch (Exception exception)
            {
                Report(ErrorStage.Decoding, ErrorOutcome.Failed, exception, delivery.DeliveryTag);
            }

            if (!decoded)
            {
                if (!_options.Consumer.AutoAck) await RejectAsync(delivery.DeliveryTag).ConfigureAwait(false);
                return;
            }

            try
            {
                _options.Handler(message);
            }
            catch (Exception exception)
            {
                Report(ErrorStage.Consume, ErrorOutcome.Failed, exception, delivery.DeliveryTag);
                if (!_options.Consumer.AutoAck) await RejectAsync(delivery.DeliveryTag).ConfigureAwait(false);
                return;
            }

            if (_options.Consumer.AutoAck) return;
            try
            {
                await _channelGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                try { await _channel.BasicAckAsync(delivery.DeliveryTag, CancellationToken.None).ConfigureAwait(false); }
                finally { _channelGate.Release(); }
            }
            catch (Exception exception)
            {
                Report(ErrorStage.Consume, ErrorOutcome.Unknown, exception, delivery.DeliveryTag);
            }
        }
        finally
        {
            CurrentCallback.Value = null;
            ExitDelivery();
        }
    }

    private async Task RejectAsync(ulong deliveryTag)
    {
        try
        {
            await _channelGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try { await _channel.BasicRejectAsync(deliveryTag, CancellationToken.None).ConfigureAwait(false); }
            finally { _channelGate.Release(); }
        }
        catch (Exception exception)
        {
            Report(ErrorStage.Consume, ErrorOutcome.Unknown, exception, deliveryTag);
        }
    }

    private bool TryEnterDelivery()
    {
        lock (_stateGate)
        {
            if (_closing != 0) return false;
            _inflight++;
            return true;
        }
    }

    private void ExitDelivery()
    {
        TaskCompletionSource? idle = null;
        lock (_stateGate)
        {
            if (--_inflight == 0) idle = _idle;
        }
        idle?.TrySetResult();
    }

    private Task WaitForIdleAsync()
    {
        lock (_stateGate)
        {
            if (_inflight == 0) return Task.CompletedTask;
            return (_idle ??= new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
    }

    public override Task CloseAsync(TimeSpan drainTimeout, CancellationToken cancellationToken)
    {
        lock (_stateGate)
        {
            if (_closeTask is not null) return _closeTask;
            _closing = 1;
            return _closeTask = CloseCoreAsync(drainTimeout, cancellationToken);
        }
    }

    private async Task CloseCoreAsync(TimeSpan drainTimeout, CancellationToken cancellationToken)
    {
        _stopping.Cancel();
        _activation.TrySetCanceled(_stopping.Token);

        using var drain = new CancellationTokenSource();
        if (drainTimeout > TimeSpan.Zero) drain.CancelAfter(drainTimeout);
        else drain.Cancel();
        using var close = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, drain.Token);
        CancellationToken closeToken = close.Token;
        bool drainReported = false;

        try
        {
            if (_consumerTag is not null && _channel.IsOpen)
            {
                // Do not take the command gate here: the handler may have synchronously released its
                // own last binding and must still be able to ACK on this original channel.
                await _channel.BasicCancelAsync(_consumerTag, closeToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException exception) when (drain.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            if (drainTimeout > TimeSpan.Zero)
            {
                Report(ErrorStage.Drain, ErrorOutcome.Unknown, exception);
                drainReported = true;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception exception)
        {
            Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception);
        }

        bool calledFromHandler = ReferenceEquals(CurrentCallback.Value, this);
        if (!calledFromHandler)
        {
            Task idleTask = WaitForIdleAsync();
            try { await idleTask.WaitAsync(closeToken).ConfigureAwait(false); }
            catch (OperationCanceledException exception) when (drain.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                if (drainTimeout > TimeSpan.Zero)
                {
                    if (!drainReported) Report(ErrorStage.Drain, ErrorOutcome.Unknown, exception);
                    drainReported = true;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        }

        try
        {
            Task disposeTask = _channel.DisposeAsync().AsTask();
            try { await disposeTask.WaitAsync(closeToken).ConfigureAwait(false); }
            catch (OperationCanceledException exception) when (drain.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                if (drainTimeout > TimeSpan.Zero && !drainReported)
                    Report(ErrorStage.Drain, ErrorOutcome.Unknown, exception);
                _ = disposeTask.ContinueWith(static task => _ = task.Exception, CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        }
        catch (Exception exception) { Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception); }

        // The callback may outlive the drain deadline and still touch these synchronization objects.
        // They are intentionally retained until the session itself becomes unreachable.
    }

    private void Report(ErrorStage stage, ErrorOutcome outcome, Exception exception, ulong? deliveryTag = null) =>
        _errors.Report(new ComponentError
        {
            Role = BrokerRole.Subscription, Stage = stage, Outcome = outcome, Resource = _options.Queue.Name,
            Exception = exception, MessageType = typeof(T), DeliveryTag = deliveryTag, Generation = Generation
        });
}
