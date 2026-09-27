using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RabbitMQ.Component.Diagnostics;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Internal.Publishing;

internal interface IPublishEndpointControl
{
    Task BeginClose();
    void ForceStop();
    Task ReportDrainTimeout();
}

internal sealed class PublishEndpoint<T> : IPublishEndpoint<T>, IPublishEndpointControl
{
    private const int Initializing = 0;
    private const int Active = 1;
    private const int Closing = 2;

    private readonly PublishSnapshot<T> _snapshot;
    private readonly PublishConnection _connection;
    private readonly ErrorSink _errors;
    private readonly Action _release;
    private readonly Channel<T> _queue;
    private readonly CancellationTokenSource _forceStop = new();
    private readonly string _resource;
    private readonly object _currentLock = new();
    private readonly TaskCompletionSource _drainTimeoutReportCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private IPublishTransportChannel? _channel;
    private CancellationTokenSource? _channelClosed;
    private object? _currentMessage;
    private bool _hasCurrentMessage;
    private Task _worker = Task.CompletedTask;
    private OwnedArrayPoolBufferWriter? _output;
    private int _state = Initializing;
    private int _drainTimeoutReportStarted;

    public PublishEndpoint(PublishSnapshot<T> snapshot, PublishConnection connection, ErrorSink errors, Action release)
    {
        _snapshot = snapshot;
        _connection = connection;
        _errors = errors;
        _release = release;
        _resource = $"exchange='{(snapshot.Exchange.Name.Length == 0 ? "<default>" : snapshot.Exchange.Name)}', routingKey='{snapshot.RoutingKey}'";
        _queue = Channel.CreateBounded<T>(new BoundedChannelOptions(snapshot.BufferCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await OpenChannelAsync(initial: true, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        Volatile.Write(ref _state, Active);
        _worker = RunAsync();
    }

    public EnqueueResult TrySend(T message)
    {
        if (Volatile.Read(ref _state) != Active) return EnqueueResult.Closed;
        if (_queue.Writer.TryWrite(message)) return EnqueueResult.Accepted;
        return Volatile.Read(ref _state) == Active ? EnqueueResult.BufferFull : EnqueueResult.Closed;
    }

    public void Dispose() => _release();

    public Task BeginClose()
    {
        int previous = Interlocked.Exchange(ref _state, Closing);
        if (previous != Closing) _queue.Writer.TryComplete();
        return _worker;
    }

    public void ForceStop()
    {
        try { _forceStop.Cancel(); }
        catch (ObjectDisposedException) { }
    }

    public Task ReportDrainTimeout()
    {
        // Endpoint cleanup and manager disposal can reach the same deadline concurrently. Losing
        // callers await the winning synchronous report instead of returning after only observing
        // its idempotence flag.
        if (Interlocked.CompareExchange(ref _drainTimeoutReportStarted, 1, 0) != 0)
            return _drainTimeoutReportCompletion.Task;

        try
        {
            object? current;
            ErrorOutcome outcome;
            lock (_currentLock)
            {
                current = _hasCurrentMessage ? _currentMessage : null;
                // A deadline does not prove that an in-flight publication failed: its confirm may arrive later.
                outcome = _hasCurrentMessage ? ErrorOutcome.Unknown : ErrorOutcome.Failed;
            }
            Report(ErrorStage.Drain, outcome,
                new TimeoutException("The publishing endpoint did not drain within the configured timeout."), current);
            _drainTimeoutReportCompletion.TrySetResult();
        }
        catch (Exception exception)
        {
            _drainTimeoutReportCompletion.TrySetException(exception);
            throw;
        }

        return _drainTimeoutReportCompletion.Task;
    }

    private async Task RunAsync()
    {
        CancellationToken cancellationToken = _forceStop.Token;
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_queue.Reader.TryRead(out T? message))
                {
                    SetCurrentMessage(message);
                    try { await ProcessAsync(message, cancellationToken).ConfigureAwait(false); }
                    finally { ClearCurrentMessage(); }
                    continue;
                }

                // One signal per channel generation replaces a per-wait AsTask/WhenAny pair on the idle path.
                CancellationTokenSource? channelClosed = _channelClosed;
                if (channelClosed is null && _channel is { } activeChannel)
                    channelClosed = _channelClosed = CreateChannelClosedSignal(activeChannel);
                if (channelClosed is null || channelClosed.IsCancellationRequested)
                {
                    await InvalidateChannelAsync().ConfigureAwait(false);
                    if (Volatile.Read(ref _state) == Closing && _queue.Reader.Completion.IsCompleted) break;
                    await OpenChannelAsync(initial: false, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                bool readable;
                try { readable = await _queue.Reader.WaitToReadAsync(channelClosed.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) when (channelClosed.IsCancellationRequested)
                {
                    continue; // The loop head distinguishes forced shutdown from channel loss.
                }
                if (!readable) break;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Forced shutdown is completed below by reporting every message that was never attempted.
        }
        catch (Exception exception)
        {
            Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception);
        }
        finally
        {
            Interlocked.Exchange(ref _state, Closing);
            _queue.Writer.TryComplete();
            while (_queue.Reader.TryRead(out T? message))
                Report(ErrorStage.Drain, ErrorOutcome.Failed,
                    new OperationCanceledException("The buffered publication was not attempted before shutdown."), message);
            await InvalidateChannelAsync().ConfigureAwait(false);
            DisposeOutput();
            _forceStop.Dispose();
        }
    }

    // Pending confirms suspend this method for every message; pool its state machine box.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ProcessAsync(T message, CancellationToken cancellationToken)
    {
        IOwnedBuffer? owner = null;
        bool reusableBody = false;
        bool publishAttempted = false;
        ReadOnlyMemory<byte> body = default;
        try
        {
            try
            {
                if (_snapshot.Codec is IOwnedBufferCodec<T> ownedCodec)
                {
                    bool encodedOwned = ownedCodec.TryEncodeOwned(message, out IOwnedBuffer? candidate);
                    if (encodedOwned)
                    {
                        owner = candidate ?? throw new InvalidOperationException("Codec returned no buffer owner.");
                        Memory<byte> memory = owner.Memory;
                        int length = owner.Length;
                        if (length < 0 || length > memory.Length)
                            throw new InvalidOperationException("Codec returned an invalid payload length.");
                        body = memory[..length];
                    }
                    else
                    {
                        if (candidate is not null)
                        {
                            owner = candidate;
                            throw new InvalidOperationException("Codec returned an owner while declining the owned-buffer path.");
                        }
                        reusableBody = true;
                        body = EncodeReusable(message);
                    }
                }
                else
                {
                    reusableBody = true;
                    body = EncodeReusable(message);
                }
            }
            catch (Exception exception)
            {
                if (reusableBody) DisposeOutput(message);
                Report(ErrorStage.Encoding, ErrorOutcome.Failed, exception, message);
                return;
            }

            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_channel is not { IsOpen: true })
                {
                    // A channel can become visibly closed before its shutdown callback completes.
                    // Dispose that generation before replacing the field with a recovered channel.
                    await InvalidateChannelAsync().ConfigureAwait(false);
                    await OpenChannelAsync(initial: false, cancellationToken).ConfigureAwait(false);
                }
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (Volatile.Read(ref _drainTimeoutReportStarted) == 0)
                    Report(ErrorStage.Drain, ErrorOutcome.Failed,
                        new OperationCanceledException("The buffered publication was not attempted before shutdown."), message);
                return;
            }

            try
            {
                // From this point any transport exception is conservatively Unknown. Retrying could duplicate a publish.
                publishAttempted = true;
                await _channel!.PublishAsync(_snapshot.Exchange.Name, _snapshot.RoutingKey, body,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (DefinitePublishException exception)
            {
                Report(ErrorStage.Publish, ErrorOutcome.Failed, exception, message);
            }
            catch (Exception exception)
            {
                Report(ErrorStage.Publish, ErrorOutcome.Unknown, exception, message);
                await InvalidateChannelAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            // PublishAsync completion is the transport boundary after which the body is no longer borrowed.
            // External owners are isolated: a faulty Dispose must not terminate this endpoint's worker.
            try { owner?.Dispose(); }
            catch (Exception exception) { Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception, message); }
            if (reusableBody && publishAttempted)
            {
                try { _output!.Clear(); }
                catch (Exception exception) { Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception, message); }
            }
        }
    }

    private ReadOnlyMemory<byte> EncodeReusable(T message)
    {
        OwnedArrayPoolBufferWriter output = _output ??= new();
        _snapshot.Codec.Encode(message, output);
        return output.WrittenMemory;
    }

    private void DisposeOutput(object? originalMessage = null)
    {
        OwnedArrayPoolBufferWriter? output = _output;
        _output = null;
        if (output is null) return;
        try { output.Dispose(); }
        catch (Exception exception) { Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception, originalMessage); }
    }

    private async Task OpenChannelAsync(bool initial, CancellationToken cancellationToken)
    {
        int attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IPublishTransportChannel? candidate = null;
            try
            {
                IPublishTransportConnection connection = await _connection.GetAsync(cancellationToken).ConfigureAwait(false);
                candidate = await connection.CreateChannelAsync(cancellationToken).ConfigureAwait(false);
                if (!candidate.IsOpen) throw new InvalidOperationException("The publishing channel was not open after creation.");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (candidate is not null) await DisposeCandidateAsync(candidate).ConfigureAwait(false);
                throw;
            }
            catch (Exception exception)
            {
                if (candidate is not null) await DisposeCandidateAsync(candidate).ConfigureAwait(false);
                Report(ErrorStage.Connection, ErrorOutcome.Failed, exception);
                await Task.Delay(GetReconnectDelay(attempt++), cancellationToken).ConfigureAwait(false);
                continue;
            }

            try
            {
                await candidate.DeclareExchangeAsync(_snapshot.Exchange, cancellationToken).ConfigureAwait(false);
                _channel = candidate;
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await DisposeCandidateAsync(candidate).ConfigureAwait(false);
                throw;
            }
            catch (Exception exception)
            {
                await DisposeCandidateAsync(candidate).ConfigureAwait(false);
                Report(ErrorStage.Topology, ErrorOutcome.Failed, exception);
                if (initial) throw;
                await Task.Delay(GetReconnectDelay(attempt++), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private TimeSpan GetReconnectDelay(int attempt) => _connection.ReconnectDelay(attempt);

    private async ValueTask InvalidateChannelAsync()
    {
        IPublishTransportChannel? channel = _channel;
        CancellationTokenSource? channelClosed = _channelClosed;
        _channel = null;
        _channelClosed = null;
        channelClosed?.Dispose();
        if (channel is not null) await DisposeCandidateAsync(channel).ConfigureAwait(false);
    }

    private CancellationTokenSource CreateChannelClosedSignal(IPublishTransportChannel channel)
    {
        // Linked to forced shutdown so an idle wait also observes ForceStop directly.
        var signal = CancellationTokenSource.CreateLinkedTokenSource(_forceStop.Token);
        Task completion;
        try { completion = channel.Completion; }
        catch
        {
            signal.Dispose();
            throw;
        }
        _ = completion.ContinueWith(static (_, state) =>
        {
            try { ((CancellationTokenSource)state!).Cancel(); }
            catch (ObjectDisposedException) { } // The generation was already invalidated.
        }, signal, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return signal;
    }

    private async ValueTask DisposeCandidateAsync(IPublishTransportChannel channel)
    {
        try { await channel.DisposeAsync().ConfigureAwait(false); }
        catch (Exception exception) { Report(ErrorStage.Cleanup, ErrorOutcome.Failed, exception); }
    }

    private void SetCurrentMessage(T message)
    {
        lock (_currentLock)
        {
            _currentMessage = message;
            _hasCurrentMessage = true;
        }
    }

    private void ClearCurrentMessage()
    {
        lock (_currentLock)
        {
            _currentMessage = null;
            _hasCurrentMessage = false;
        }
    }

    private void Report(ErrorStage stage, ErrorOutcome outcome, Exception exception, object? originalMessage = null) =>
        _errors.Report(new ComponentError
        {
            Stage = stage,
            Outcome = outcome,
            Role = BrokerRole.Publish,
            Resource = _resource,
            Exception = exception,
            OriginalMessage = originalMessage,
            MessageType = typeof(T)
        });
}
