using RabbitMQ.Client.Exceptions;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Diagnostics;

namespace RabbitMQ.Component.Internal.Subscriptions;

internal sealed class SubscriptionManager : IAsyncDisposable
{
    private readonly object _stateGate = new();
    private readonly SemaphoreSlim _operations = new(1, 1);
    private readonly Dictionary<string, IManagedReceiver> _receivers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TaskCompletionSource> _queueCleanup = new(StringComparer.Ordinal);
    private readonly Dictionary<ScopedBindingKey, BindingSnapshot> _releasedBindings = new();
    private readonly HashSet<ScopedBindingKey> _knownBindings = [];
    private readonly HashSet<Task> _backgroundTasks = [];
    private readonly HashSet<ConsumerSession> _scheduledSessionRepairs = new(ReferenceEqualityComparer.Instance);
    private readonly ServiceSnapshot _options;
    private readonly ErrorSink _errors;
    private readonly ISubscriptionTransportFactory _transport;
    private readonly CancellationTokenSource _lifetime = new();
    private BrokerConnectionOptions _broker;
    private ConnectionGeneration? _current;
    private long _nextGeneration;
    private bool _disposed;

    public SubscriptionManager(ServiceSnapshot options, ErrorSink errors)
        : this(options, errors, new RabbitSubscriptionTransportFactory()) { }

    internal SubscriptionManager(ServiceSnapshot options, ErrorSink errors, ISubscriptionTransportFactory transport)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _errors = errors ?? throw new ArgumentNullException(nameof(errors));
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _broker = options.SubscriptionBroker;
    }

    public ValueTask<IReceiver<T>> OpenAsync<T>(SubscriptionSnapshot<T> options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_stateGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_receivers.TryGetValue(options.Queue.Name, out IManagedReceiver? existing))
            {
                if (existing is ManagedReceiver<T> typed && typed.IsCompatibleWith(options))
                    return new ValueTask<IReceiver<T>>(typed);
                throw new InvalidOperationException($"Queue '{options.Queue.Name}' is already open with incompatible subscription settings.");
            }

            var receiver = new ManagedReceiver<T>(this, options, _errors);
            _receivers.Add(options.Queue.Name, receiver);
            return new ValueTask<IReceiver<T>>(receiver);
        }
    }

    public async Task SwitchBrokerAsync(BrokerConnectionOptions options, CancellationToken cancellationToken)
    {
        BrokerConnectionOptions snapshot = ConfigSnapshots.Broker(options);
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        cancellationToken = linked.Token;
        await _operations.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);
            await ReplaceGenerationAsync(snapshot, expectedGeneration: null, isSwitch: true, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            Report(ErrorStage.BrokerSwitch, ErrorOutcome.Failed, "subscription-broker", exception);
            throw;
        }
        finally { _operations.Release(); }
    }

    internal async Task ActivateBindingAsync(IManagedReceiver receiver, ManagedBinding binding, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        cancellationToken = linked.Token;
        Task? queueCleanup;
        lock (_stateGate)
            queueCleanup = _queueCleanup.TryGetValue(receiver.Queue.Name, out TaskCompletionSource? pending) ? pending.Task : null;
        if (queueCleanup is not null) await queueCleanup.WaitAsync(cancellationToken).ConfigureAwait(false);

        await _operations.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);
            if (!receiver.Contains(binding)) throw new ObjectDisposedException(receiver.GetType().Name);
            ConnectionGeneration generation = await EnsureGenerationAsync(cancellationToken).ConfigureAwait(false);
            await CleanupReleasedBindingsAsync(generation.Connection, generation.Broker, cancellationToken,
                receiver.Queue.Name).ConfigureAwait(false);
            if (!generation.Sessions.TryGetValue(receiver, out ConsumerSession? session))
            {
                IReadOnlyList<BindingSnapshot> bindings = GetRepairBindings(receiver, binding);
                foreach (BindingSnapshot snapshot in bindings) RecordBinding(generation.Broker, receiver, snapshot);
                session = await receiver.CreateSessionAsync(generation.Connection, generation.Number, bindings,
                    startImmediately: false, cancellationToken: cancellationToken,
                    onFailed: failed => OnSessionFailed(generation, receiver, failed)).ConfigureAwait(false);
                bool installed = false;
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    installed = TryCommitSession(generation, receiver, session, binding);
                    if (!installed) throw new InvalidOperationException("The subscription session could not be activated.");
                }
                finally
                {
                    if (!installed) await session.CloseAsync(_options.DrainTimeout, CancellationToken.None).ConfigureAwait(false);
                }
            }
            else if (!session.IsOpen)
            {
                LocalRepairResult result = await TryRepairSessionAsync(generation, receiver, session, binding,
                    cancellationToken).ConfigureAwait(false);
                if (result == LocalRepairResult.ConnectionFailed)
                    throw new InvalidOperationException("The subscription connection failed during session repair.");
                if (result != LocalRepairResult.Repaired)
                    throw new ObjectDisposedException(receiver.GetType().Name);
                session = generation.Sessions[receiver];
            }
            else
            {
                RecordBinding(generation.Broker, receiver, binding.Snapshot);
                await session.DeclareAndBindAsync(binding.Snapshot, cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
                if (!TryCommitSession(generation, receiver, session, binding))
                    throw new InvalidOperationException("The subscription session failed during binding activation.");
            }

            lock (_stateGate) _releasedBindings.Remove(new(generation.Broker, binding.Snapshot.Key(receiver.Queue.Name)));
        }
        catch (Exception exception)
        {
            if (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                Report(ErrorStage.Topology, ErrorOutcome.Failed, receiver.Queue.Name, exception);
            ScheduleRecoveryIfBroken(receiver);
            throw;
        }
        finally { _operations.Release(); }
    }

    internal void BindingReleased(IManagedReceiver receiver, BindingSnapshot binding, bool receiverClosed)
    {
        TaskCompletionSource? cleanupCompletion = null;
        lock (_stateGate)
        {
            if (_disposed) return;
            BindingKey key = binding.Key(receiver.Queue.Name);
            foreach (ScopedBindingKey installed in _knownBindings.Where(item => item.Binding == key).ToArray())
                _releasedBindings[installed] = binding;
            if (receiverClosed)
            {
                if (_receivers.TryGetValue(receiver.Queue.Name, out IManagedReceiver? current) && ReferenceEquals(current, receiver))
                    _receivers.Remove(receiver.Queue.Name);
                cleanupCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _queueCleanup[receiver.Queue.Name] = cleanupCompletion;
            }
        }

        Schedule(() => RunBindingCleanupAsync(receiver, receiverClosed, cleanupCompletion));
    }

    private async Task RunBindingCleanupAsync(IManagedReceiver receiver, bool receiverClosed,
        TaskCompletionSource? cleanupCompletion)
    {
        try
        {
            await _operations.WaitAsync().ConfigureAwait(false);
            try
            {
                ConnectionGeneration? generation = Current;
                if (generation is null) return;
                generation.Sessions.TryGetValue(receiver, out ConsumerSession? session);
                try
                {
                    // Only this broker/vhost's outstanding tombstones are authoritative. Never send
                    // cleanup commands on a consumer channel: a missing exchange must not break its ACKs.
                    await CleanupReleasedBindingsAsync(generation.Connection, generation.Broker,
                        CancellationToken.None, receiver.Queue.Name).ConfigureAwait(false);
                }
                finally
                {
                    if (receiverClosed && session is not null)
                    {
                        generation.Sessions.Remove(receiver);
                        await session.CloseAsync(_options.DrainTimeout, CancellationToken.None).ConfigureAwait(false);
                    }
                }
            }
            finally { _operations.Release(); }
        }
        catch (Exception exception)
        {
            Report(ErrorStage.Cleanup, ErrorOutcome.Failed, receiver.Queue.Name, exception);
        }
        finally
        {
            if (cleanupCompletion is not null)
            {
                lock (_stateGate)
                {
                    if (_queueCleanup.TryGetValue(receiver.Queue.Name, out TaskCompletionSource? current) &&
                        ReferenceEquals(current, cleanupCompletion))
                        _queueCleanup.Remove(receiver.Queue.Name);
                }
                cleanupCompletion.TrySetResult();
            }
        }
    }

    private async Task<ConnectionGeneration> EnsureGenerationAsync(CancellationToken cancellationToken)
    {
        ConnectionGeneration? current = Current;
        if (current?.IsConnectionUsable == true) return current;
        if (current is not null)
        {
            // A foreground Bind can win the operation gate before the shutdown recovery task. It must
            // recover every active receiver, not install a generation containing only its own queue.
            await ReplaceGenerationAsync(_broker, current.Number, isSwitch: false, cancellationToken).ConfigureAwait(false);
            return Current ?? throw new InvalidOperationException("Subscription recovery did not install a connection generation.");
        }

        ISubscriptionConnection connection = await _transport.ConnectAsync(_broker, cancellationToken).ConfigureAwait(false);
        var generation = new ConnectionGeneration(Interlocked.Increment(ref _nextGeneration), BrokerIdentity.Create(_broker), connection);
        connection.Closed += () => OnConnectionClosedAsync(generation);
        try
        {
            generation.Commit(() =>
            {
                lock (_stateGate)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    _current = generation;
                }
            });
            return generation;
        }
        catch
        {
            generation.BeginClose();
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task ReplaceGenerationAsync(BrokerConnectionOptions broker, long? expectedGeneration, bool isSwitch,
        CancellationToken cancellationToken)
    {
        ConnectionGeneration? old = Current;
        if (expectedGeneration.HasValue && old?.Number != expectedGeneration.Value) return;

        ConnectionGeneration? candidate = null;
        var prepared = new Dictionary<IManagedReceiver, ConsumerSession>(ReferenceEqualityComparer.Instance);
        BrokerIdentity candidateBroker = BrokerIdentity.Create(broker);
        bool sameBroker = old is not null && old.Broker == candidateBroker;
        bool hasOldExclusiveConsumer = old?.HasExclusiveConsumers == true;
        if (isSwitch && sameBroker && (old!.IsClosing && hasOldExclusiveConsumer && !old.IsFullyClosed ||
                old.Sessions.Any(pair => pair.Key.ConsumerExclusive && pair.Value.IsOpen)))
            throw new InvalidOperationException(
                "Cannot replace a subscription connection on the same broker and virtual host while an exclusive consumer is active.");

        long candidateNumber = Interlocked.Increment(ref _nextGeneration);
        bool oldClosedEarly = false;
        try
        {
            ISubscriptionConnection connection = await _transport.ConnectAsync(broker, cancellationToken).ConfigureAwait(false);
            var created = new ConnectionGeneration(candidateNumber, candidateBroker, connection);
            candidate = created;
            // Subscribe before any topology or consumer work. ConnectionGeneration retains an early
            // shutdown so it cannot be lost before this candidate reaches the commit boundary.
            connection.Closed += () => OnConnectionClosedAsync(created);

            if (!isSwitch && sameBroker && hasOldExclusiveConsumer)
            {
                ConnectionGeneration oldGeneration = old!;
                oldClosedEarly = true;
                if (!oldGeneration.IsClosing)
                {
                    try { await oldGeneration.DisposeAsync(_options.DrainTimeout).ConfigureAwait(false); }
                    catch (Exception exception)
                    {
                        // A failed cleanup must not prevent later attempts from retrying candidate
                        // registration after the broker has eventually released the exclusive consumers.
                        Report(ErrorStage.Cleanup, ErrorOutcome.Unknown, "subscription-broker", exception, oldGeneration.Number);
                    }
                }
            }

            await CleanupReleasedBindingsAsync(connection, candidateBroker, cancellationToken).ConfigureAwait(false);
            List<IManagedReceiver> receivers = ActiveReceivers();
            foreach (IManagedReceiver receiver in receivers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                IReadOnlyList<BindingSnapshot> bindings = receiver.GetLiveBindings();
                if (bindings.Count == 0) continue;
                foreach (BindingSnapshot binding in bindings) RecordBinding(candidateBroker, receiver, binding);
                ConsumerSession session = await receiver.CreateSessionAsync(connection, candidateNumber, bindings,
                    startImmediately: false, cancellationToken: cancellationToken,
                    onFailed: failed => OnSessionFailed(created, receiver, failed)).ConfigureAwait(false);
                prepared.Add(receiver, session);
                created.Sessions.Add(receiver, session);
            }

            cancellationToken.ThrowIfCancellationRequested(); // Last cancellable point: no externally visible switch has occurred.

            // Dispose can run during preparation. Tombstones remove released candidate bindings on
            // short-lived administrative channels before opening any candidate delivery gate.
            foreach ((IManagedReceiver receiver, ConsumerSession session) in prepared.ToArray())
            {
                if (receiver.IsActive && receiver.GetLiveBindings().Count != 0) continue;
                prepared.Remove(receiver);
                created.Sessions.Remove(receiver);
                await session.CloseAsync(_options.DrainTimeout, CancellationToken.None).ConfigureAwait(false);
            }
            await CleanupReleasedBindingsAsync(connection, candidateBroker, CancellationToken.None).ConfigureAwait(false);

            created.Commit(() =>
            {
                lock (_stateGate)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    if (expectedGeneration.HasValue && _current?.Number != expectedGeneration.Value)
                        throw new GenerationChangedException();
                    _broker = broker;
                    _current = created; // Commit boundary. Cancellation after this point does not roll the switch back.
                }
            });

            try
            {
                if (old is not null && !oldClosedEarly)
                    await old.DisposeAsync(_options.DrainTimeout).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                // The candidate is already committed. Cleanup failure must not be reported as a rolled-back switch.
                Report(ErrorStage.Cleanup, ErrorOutcome.Unknown, "subscription-broker", exception, old?.Number);
            }
            finally
            {
                foreach (ConsumerSession session in prepared.Values) session.Activate();
            }
            candidate = null;
        }
        catch (GenerationChangedException)
        {
            if (candidate is not null) await candidate.DisposeAsync(_options.DrainTimeout).ConfigureAwait(false);
        }
        catch
        {
            if (candidate is not null) await candidate.DisposeAsync(_options.DrainTimeout).ConfigureAwait(false);
            throw;
        }
    }

    private Task OnConnectionClosedAsync(ConnectionGeneration generation)
    {
        if (!generation.MarkConnectionFailed() || _lifetime.IsCancellationRequested) return Task.CompletedTask;
        Schedule(() => RecoverConnectionAsync(generation.Number));
        return Task.CompletedTask;
    }

    private void OnSessionFailed(ConnectionGeneration generation, IManagedReceiver receiver, ConsumerSession failedSession)
    {
        if (_lifetime.IsCancellationRequested) return;
        lock (_stateGate)
        {
            if (!_scheduledSessionRepairs.Add(failedSession)) return;
        }
        Schedule(() => RepairSessionAsync(generation, receiver, failedSession));
    }

    private async Task RepairSessionAsync(ConnectionGeneration generation, IManagedReceiver receiver,
        ConsumerSession failedSession)
    {
        int attempt = 0;
        try
        {
            while (!_lifetime.IsCancellationRequested)
            {
                LocalRepairResult result;
                await _operations.WaitAsync(_lifetime.Token).ConfigureAwait(false);
                try
                {
                    result = await TryRepairSessionAsync(generation, receiver, failedSession, pendingBinding: null,
                        _lifetime.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { return; }
                catch (Exception exception)
                {
                    Report(ErrorStage.Topology, ErrorOutcome.Failed, receiver.Queue.Name, exception, generation.Number);
                    result = generation.IsConnectionUsable ? LocalRepairResult.Retry : LocalRepairResult.ConnectionFailed;
                }
                finally { _operations.Release(); }

                if (result is LocalRepairResult.Repaired or LocalRepairResult.Stale) return;
                if (result == LocalRepairResult.ConnectionFailed)
                {
                    generation.MarkConnectionFailed();
                    await RecoverConnectionAsync(generation.Number).ConfigureAwait(false);
                    return;
                }

                try { await Task.Delay(_options.ReconnectDelay(attempt++), _lifetime.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
        finally
        {
            lock (_stateGate) _scheduledSessionRepairs.Remove(failedSession);
        }
    }

    private async Task<LocalRepairResult> TryRepairSessionAsync(ConnectionGeneration generation,
        IManagedReceiver receiver, ConsumerSession failedSession, ManagedBinding? pendingBinding,
        CancellationToken cancellationToken)
    {
        if (!IsInstalledSession(generation, receiver, failedSession)) return LocalRepairResult.Stale;
        if (!generation.IsConnectionUsable) return LocalRepairResult.ConnectionFailed;
        if (!IsCurrentReceiver(receiver)) return LocalRepairResult.Stale;

        IReadOnlyList<BindingSnapshot> bindings = GetRepairBindings(receiver, pendingBinding);
        if (bindings.Count == 0) return LocalRepairResult.Stale;

        await failedSession.CloseAsync(_options.DrainTimeout, CancellationToken.None).ConfigureAwait(false);
        if (!IsInstalledSession(generation, receiver, failedSession)) return LocalRepairResult.Stale;
        if (!generation.IsConnectionUsable) return LocalRepairResult.ConnectionFailed;
        if (!IsCurrentReceiver(receiver)) return LocalRepairResult.Stale;

        await CleanupReleasedBindingsAsync(generation.Connection, generation.Broker, cancellationToken,
            receiver.Queue.Name).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (!IsInstalledSession(generation, receiver, failedSession)) return LocalRepairResult.Stale;
        if (!generation.IsConnectionUsable) return LocalRepairResult.ConnectionFailed;
        if (!IsCurrentReceiver(receiver)) return LocalRepairResult.Stale;

        bindings = GetRepairBindings(receiver, pendingBinding);
        if (bindings.Count == 0) return LocalRepairResult.Stale;
        foreach (BindingSnapshot binding in bindings) RecordBinding(generation.Broker, receiver, binding);

        ConsumerSession? candidate = null;
        try
        {
            candidate = await receiver.CreateSessionAsync(generation.Connection, generation.Number, bindings,
                startImmediately: false, cancellationToken: cancellationToken,
                onFailed: failed => OnSessionFailed(generation, receiver, failed)).ConfigureAwait(false);
            // Releases during preparation create tombstones too. Clean only this queue, using
            // administrative channels, before opening the candidate's delivery gate.
            await CleanupReleasedBindingsAsync(generation.Connection, generation.Broker, cancellationToken,
                receiver.Queue.Name).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (!IsInstalledSession(generation, receiver, failedSession) || !IsCurrentReceiver(receiver) ||
                (pendingBinding is not null && !receiver.Contains(pendingBinding)))
                return LocalRepairResult.Stale;
            if (!generation.IsConnectionUsable) return LocalRepairResult.ConnectionFailed;
            if (!candidate.IsOpen) return LocalRepairResult.Retry;
            if (!TryCommitSession(generation, receiver, candidate, pendingBinding))
                return generation.IsConnectionUsable ? LocalRepairResult.Retry : LocalRepairResult.ConnectionFailed;
            candidate = null;
            return LocalRepairResult.Repaired;
        }
        finally
        {
            if (candidate is not null)
                await candidate.CloseAsync(_options.DrainTimeout, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task RecoverConnectionAsync(long failedGeneration)
    {
        int attempt = 0;
        while (!_lifetime.IsCancellationRequested)
        {
            await _operations.WaitAsync(_lifetime.Token).ConfigureAwait(false);
            try
            {
                ConnectionGeneration? current = Current;
                if (current?.Number != failedGeneration || current.IsConnectionUsable) return;
                try
                {
                    await ReplaceGenerationAsync(_broker, failedGeneration, isSwitch: false, _lifetime.Token).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { return; }
                catch (Exception exception)
                {
                    Report(ErrorStage.Connection, ErrorOutcome.Failed, "subscription-broker", exception, failedGeneration);
                }
            }
            finally { _operations.Release(); }

            try { await Task.Delay(_options.ReconnectDelay(attempt++), _lifetime.Token).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
    }

    private void ScheduleRecoveryIfBroken(IManagedReceiver receiver)
    {
        ConnectionGeneration? current = Current;
        if (current is null) return;
        if (!current.IsConnectionUsable)
        {
            current.MarkConnectionFailed();
            Schedule(() => RecoverConnectionAsync(current.Number));
            return;
        }
        if (current.Sessions.TryGetValue(receiver, out ConsumerSession? session) && !session.IsOpen)
            OnSessionFailed(current, receiver, session);
    }

    private bool IsInstalledSession(ConnectionGeneration generation, IManagedReceiver receiver,
        ConsumerSession session) => ReferenceEquals(Current, generation) &&
        generation.Sessions.TryGetValue(receiver, out ConsumerSession? installed) && ReferenceEquals(installed, session);

    // Called only under _operations. Receiver release and manager disposal must not interleave
    // the final identity check, slot replacement, binding activation and delivery activation.
    private bool TryCommitSession(ConnectionGeneration generation, IManagedReceiver receiver,
        ConsumerSession session, ManagedBinding? pendingBinding)
    {
        lock (_stateGate)
        {
            if (!IsCurrentReceiver(receiver) || !ReferenceEquals(_current, generation) ||
                !generation.IsConnectionUsable || !session.IsOpen) return false;
            return receiver.TryActivateSession(pendingBinding, () =>
            {
                generation.Sessions[receiver] = session;
                session.Activate();
            });
        }
    }

    private bool IsCurrentReceiver(IManagedReceiver receiver)
    {
        lock (_stateGate)
            return !_disposed && _receivers.TryGetValue(receiver.Queue.Name, out IManagedReceiver? current) &&
                ReferenceEquals(current, receiver);
    }

    private static IReadOnlyList<BindingSnapshot> GetRepairBindings(IManagedReceiver receiver,
        ManagedBinding? pendingBinding)
    {
        IEnumerable<BindingSnapshot> bindings = receiver.GetLiveBindings();
        if (pendingBinding is not null && receiver.Contains(pendingBinding)) bindings = bindings.Append(pendingBinding.Snapshot);
        return bindings.DistinctBy(item => item.Key(receiver.Queue.Name)).ToArray();
    }

    private enum LocalRepairResult { Repaired, Retry, Stale, ConnectionFailed }

    private List<IManagedReceiver> ActiveReceivers()
    {
        lock (_stateGate) return _receivers.Values.Where(static receiver => receiver.IsActive).ToList();
    }

    private void RecordBinding(BrokerIdentity broker, IManagedReceiver receiver, BindingSnapshot binding)
    {
        var key = new ScopedBindingKey(broker, binding.Key(receiver.Queue.Name));
        lock (_stateGate)
        {
            // Remember attempted installs too: cancellation can occur after the broker accepted a bind.
            _knownBindings.Add(key);
            if (receiver.HasBinding(key.Binding)) _releasedBindings.Remove(key);
            else _releasedBindings[key] = binding;
        }
    }

    private void ForgetBinding(BrokerIdentity broker, string queueName, BindingSnapshot binding)
    {
        var key = new ScopedBindingKey(broker, binding.Key(queueName));
        lock (_stateGate)
        {
            _releasedBindings.Remove(key);
            _knownBindings.Remove(key);
        }
    }

    private async Task CleanupReleasedBindingsAsync(ISubscriptionConnection connection, BrokerIdentity broker,
        CancellationToken cancellationToken, string? queueName = null)
    {
        KeyValuePair<ScopedBindingKey, BindingSnapshot>[] released;
        lock (_stateGate)
        {
            released = _releasedBindings.Where(item => item.Key.Broker == broker &&
                (queueName is null || item.Key.Binding.QueueName == queueName)).ToArray();
        }
        foreach (var item in released)
        {
            lock (_stateGate)
            {
                if (_receivers.TryGetValue(item.Key.Binding.QueueName, out IManagedReceiver? receiver) &&
                    receiver.HasBinding(item.Key.Binding))
                {
                    _releasedBindings.Remove(item.Key);
                    continue;
                }
            }
            try
            {
                // Never redeclare an obsolete exchange. A missing resource may close this disposable
                // cleanup channel, but cannot poison a live consumer or its delivery acknowledgements.
                await using ISubscriptionChannel channel = await connection.CreateChannelAsync(cancellationToken).ConfigureAwait(false);
                await channel.QueueUnbindAsync(item.Key.Binding.QueueName, item.Value, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
            {
                // The queue or exchange no longer exists, so the old binding cannot exist either.
            }
            ForgetBinding(broker, item.Key.Binding.QueueName, item.Value);
        }
    }

    private ConnectionGeneration? Current { get { lock (_stateGate) return _current; } }
    private bool IsDisposed { get { lock (_stateGate) return _disposed; } }

    private void StartForcedCurrentCleanup()
    {
        Schedule(async () =>
        {
            await _operations.WaitAsync().ConfigureAwait(false);
            try
            {
                ConnectionGeneration? generation;
                lock (_stateGate) { generation = _current; _current = null; }
                if (generation is not null) await generation.DisposeAsync(TimeSpan.Zero).ConfigureAwait(false);
            }
            finally { _operations.Release(); }
        });
    }

    private void Schedule(Func<Task> work)
    {
        Task task;
        using (ExecutionContext.SuppressFlow()) task = Task.Run(work);
        Track(task);
    }

    private void Track(Task task)
    {
        lock (_stateGate)
        {
            _backgroundTasks.Add(task);
        }
        _ = task.ContinueWith(completed =>
        {
            lock (_stateGate) _backgroundTasks.Remove(completed);
            if (completed.Exception is { } failure)
                foreach (Exception exception in failure.Flatten().InnerExceptions)
                    if (exception is not OperationCanceledException)
                        Report(ErrorStage.Cleanup, ErrorOutcome.Unknown, "subscription-background", exception);
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private void Report(ErrorStage stage, ErrorOutcome outcome, string resource, Exception exception, long? generation = null) =>
        _errors.Report(new ComponentError
        {
            Role = BrokerRole.Subscription, Stage = stage, Outcome = outcome, Resource = resource,
            Exception = exception, Generation = generation
        });

    public async ValueTask DisposeAsync()
    {
        List<IManagedReceiver> receivers;
        lock (_stateGate)
        {
            if (_disposed) return;
            _disposed = true;
            receivers = _receivers.Values.ToList();
            _receivers.Clear();
        }
        _lifetime.Cancel();
        foreach (IManagedReceiver receiver in receivers) receiver.CloseFromManager();

        using var timeout = new CancellationTokenSource(_options.DrainTimeout);
        try
        {
            await _operations.WaitAsync(timeout.Token).ConfigureAwait(false);
            try
            {
                ConnectionGeneration? generation;
                lock (_stateGate) { generation = _current; _current = null; }
                if (generation is not null) await generation.DisposeAsync(_options.DrainTimeout).ConfigureAwait(false);
            }
            finally { _operations.Release(); }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            Report(ErrorStage.Drain, ErrorOutcome.Unknown, "subscriptions",
                new TimeoutException("Subscription drain timed out."));
            StartForcedCurrentCleanup();
        }
        catch (TimeoutException exception)
        {
            Report(ErrorStage.Drain, ErrorOutcome.Unknown, "subscriptions", exception);
            StartForcedCurrentCleanup();
        }

        Task[] background;
        lock (_stateGate) background = _backgroundTasks.ToArray();
        Task remaining = Task.WhenAll(background);
        try { await remaining.WaitAsync(_options.DrainTimeout).ConfigureAwait(false); }
        catch (TimeoutException exception) { Report(ErrorStage.Drain, ErrorOutcome.Unknown, "subscriptions", exception); }
        catch (OperationCanceledException) { }
        catch (Exception exception) { Report(ErrorStage.Cleanup, ErrorOutcome.Unknown, "subscriptions", exception); }

        if (remaining.IsCompleted) _lifetime.Dispose();
        else _ = remaining.ContinueWith(_ => _lifetime.Dispose(), CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }

    private sealed class ConnectionGeneration(long number, BrokerIdentity broker, ISubscriptionConnection connection)
    {
        private readonly object _gate = new();
        private bool _committed;
        private bool _failed;
        private bool _closing;
        private bool _hadExclusiveConsumers;
        private bool _fullyClosed;
        private Task? _disposeTask;

        public long Number { get; } = number;
        public BrokerIdentity Broker { get; } = broker;
        public ISubscriptionConnection Connection { get; } = connection;
        public Dictionary<IManagedReceiver, ConsumerSession> Sessions { get; } = new(ReferenceEqualityComparer.Instance);
        public bool IsClosing { get { lock (_gate) return _closing; } }
        public bool IsFullyClosed { get { lock (_gate) return _fullyClosed; } }
        public bool HasExclusiveConsumers
        {
            get
            {
                lock (_gate) return _hadExclusiveConsumers || Sessions.Keys.Any(static receiver => receiver.ConsumerExclusive);
            }
        }

        public bool IsConnectionUsable
        {
            get
            {
                lock (_gate) return _committed && !_failed && !_closing && Connection.IsOpen;
            }
        }

        public void Commit(Action commit)
        {
            lock (_gate)
            {
                EnsureCommitReadyLocked();
                commit();
                _committed = true;
            }
        }

        public bool MarkConnectionFailed()
        {
            lock (_gate)
            {
                if (_closing || _failed) return false;
                _failed = true;
                return true;
            }
        }

        public void BeginClose()
        {
            lock (_gate)
            {
                _hadExclusiveConsumers |= Sessions.Keys.Any(static receiver => receiver.ConsumerExclusive);
                _closing = true;
            }
        }

        private void EnsureCommitReadyLocked()
        {
            if (_failed || _closing || !Connection.IsOpen || Sessions.Values.Any(static session => !session.IsOpen))
                throw new InvalidOperationException($"Subscription connection generation {Number} is not usable.");
        }

        public Task DisposeAsync(TimeSpan drainTimeout)
        {
            lock (_gate) return _disposeTask ??= DisposeCoreAsync(drainTimeout);
        }

        private async Task DisposeCoreAsync(TimeSpan drainTimeout)
        {
            BeginClose();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            ConsumerSession[] sessions = Sessions.Values.ToArray();
            Sessions.Clear();
            Task[] closes = sessions.Select(session => session.CloseAsync(drainTimeout, CancellationToken.None)).ToArray();
            if (closes.Length != 0) await Task.WhenAll(closes).ConfigureAwait(false);

            Task connectionClose = Connection.DisposeAsync().AsTask();
            TimeSpan remaining = drainTimeout - stopwatch.Elapsed;
            if (remaining <= TimeSpan.Zero)
            {
                ObserveConnectionClose(connectionClose);
                throw new TimeoutException("Subscription connection cleanup exceeded the drain timeout.");
            }
            try
            {
                await connectionClose.WaitAsync(remaining).ConfigureAwait(false);
                MarkFullyClosed();
            }
            catch (TimeoutException)
            {
                ObserveConnectionClose(connectionClose);
                throw;
            }
        }

        private void ObserveConnectionClose(Task connectionClose) =>
            _ = connectionClose.ContinueWith(completed =>
            {
                if (completed.IsCompletedSuccessfully) MarkFullyClosed();
                else _ = completed.Exception;
            }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        private void MarkFullyClosed()
        {
            lock (_gate) _fullyClosed = true;
        }
    }

    private sealed class GenerationChangedException : Exception;
}

internal interface IManagedReceiver
{
    QueueSnapshot Queue { get; }
    bool ConsumerExclusive { get; }
    bool IsActive { get; }
    bool Contains(ManagedBinding binding);
    bool HasBinding(BindingKey key);
    IReadOnlyList<BindingSnapshot> GetLiveBindings();
    bool TryActivateSession(ManagedBinding? pendingBinding, Action installAndActivate);
    Task<ConsumerSession> CreateSessionAsync(ISubscriptionConnection connection, long generation,
        IReadOnlyList<BindingSnapshot> bindings, bool startImmediately, CancellationToken cancellationToken,
        Action<ConsumerSession> onFailed);
    void CloseFromManager();
}

internal sealed class ManagedReceiver<T> : IReceiver<T>, IManagedReceiver
{
    private readonly object _gate = new();
    private readonly SubscriptionManager _manager;
    private readonly SubscriptionSnapshot<T> _options;
    private readonly ErrorSink _errors;
    private readonly Dictionary<BindingKey, ManagedBinding> _bindings = new();
    private bool _closed;

    public ManagedReceiver(SubscriptionManager manager, SubscriptionSnapshot<T> options, ErrorSink errors)
    {
        _manager = manager;
        _options = options;
        _errors = errors;
    }

    public QueueSnapshot Queue => _options.Queue;
    public bool ConsumerExclusive => _options.Consumer.Exclusive;
    // Checked for every delivery; foreach uses the struct enumerator instead of boxing one as LINQ does.
    public bool IsActive
    {
        get
        {
            lock (_gate)
            {
                if (_closed) return false;
                foreach (ManagedBinding binding in _bindings.Values)
                    if (binding.IsActive) return true;
                return false;
            }
        }
    }
    public bool IsCompatibleWith(SubscriptionSnapshot<T> options) => _options.IsCompatibleWith(options);

    public async ValueTask<IQueueBinding> BindAsync(BindingConfig config, CancellationToken cancellationToken = default)
    {
        BindingSnapshot snapshot = ConfigSnapshots.Binding(config);
        ManagedBinding binding;
        bool activate;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            if (_bindings.Values.Any(existing => existing.Snapshot.Exchange.Name == snapshot.Exchange.Name &&
                    existing.Snapshot.Exchange != snapshot.Exchange))
                throw new InvalidOperationException($"Exchange '{snapshot.Exchange.Name}' is already bound with incompatible declaration settings.");
            BindingKey key = snapshot.Key(Queue.Name);
            if (_bindings.TryGetValue(key, out binding!))
            {
                binding.Reserve();
                activate = false;
            }
            else
            {
                binding = new ManagedBinding(Release, snapshot);
                _bindings.Add(key, binding);
                activate = true;
            }
        }

        if (activate)
        {
            try
            {
                await _manager.ActivateBindingAsync(this, binding, cancellationToken).ConfigureAwait(false);
                binding.ActivationSucceeded();
            }
            catch (Exception exception)
            {
                binding.ActivationFailed(exception);
            }
        }

        try
        {
            await binding.Activation.WaitAsync(cancellationToken).ConfigureAwait(false);
            return binding;
        }
        catch
        {
            RollbackReservation(binding);
            throw;
        }
    }

    private void RollbackReservation(ManagedBinding binding)
    {
        bool closed = false;
        lock (_gate)
        {
            if (!binding.RollbackReservation()) return;
            BindingKey key = binding.Snapshot.Key(Queue.Name);
            if (binding.ReferenceCount == 0 && _bindings.TryGetValue(key, out ManagedBinding? current) && ReferenceEquals(current, binding))
            {
                _bindings.Remove(key);
                if (_bindings.Count == 0) closed = _closed = true;
            }
        }
        if (binding.ReferenceCount == 0) _manager.BindingReleased(this, binding.Snapshot, closed);
    }

    internal void Release(ManagedBinding binding)
    {
        bool closed = false;
        lock (_gate)
        {
            BindingKey key = binding.Snapshot.Key(Queue.Name);
            if (!_bindings.TryGetValue(key, out ManagedBinding? current) || !ReferenceEquals(current, binding))
                throw new InvalidOperationException("The binding has already been fully released.");
            if (!binding.ReleaseReference()) return;
            _bindings.Remove(key);
            if (_bindings.Count == 0) closed = _closed = true;
        }
        _manager.BindingReleased(this, binding.Snapshot, closed);
    }

    public bool Contains(ManagedBinding binding)
    {
        lock (_gate)
            return !_closed && _bindings.TryGetValue(binding.Snapshot.Key(Queue.Name), out ManagedBinding? current) &&
                ReferenceEquals(current, binding);
    }

    public bool HasBinding(BindingKey key) { lock (_gate) return !_closed && _bindings.ContainsKey(key); }
    public IReadOnlyList<BindingSnapshot> GetLiveBindings()
    {
        lock (_gate) return _bindings.Values.Where(static item => item.IsActive).Select(static item => item.Snapshot).ToArray();
    }

    public bool TryActivateSession(ManagedBinding? pendingBinding, Action installAndActivate)
    {
        lock (_gate)
        {
            if (_closed || (pendingBinding is not null ? !Contains(pendingBinding) : !IsActive)) return false;
            pendingBinding?.ActivationSucceeded();
            installAndActivate();
            return true;
        }
    }

    public async Task<ConsumerSession> CreateSessionAsync(ISubscriptionConnection connection, long generation,
        IReadOnlyList<BindingSnapshot> bindings, bool startImmediately, CancellationToken cancellationToken,
        Action<ConsumerSession> onFailed)
    {
        ISubscriptionChannel channel = await connection.CreateChannelAsync(cancellationToken).ConfigureAwait(false);
        var session = new ConsumerSession<T>(channel, generation, _options, _errors, startImmediately,
            () => IsActive, onFailed);
        try
        {
            await session.PrepareAsync(bindings, cancellationToken).ConfigureAwait(false);
            return session;
        }
        catch
        {
            await session.CloseAsync(TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    public void CloseFromManager()
    {
        lock (_gate) _closed = true;
    }
}

internal sealed class ManagedBinding : IQueueBinding
{
    private readonly Action<ManagedBinding> _release;
    private readonly TaskCompletionSource _activation = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _references = 1;
    private int _activationState; // 0 pending, 1 successful, 2 failed

    public ManagedBinding(Action<ManagedBinding> release, BindingSnapshot snapshot)
    {
        _release = release;
        Snapshot = snapshot;
    }

    public BindingSnapshot Snapshot { get; }
    public Task Activation => _activation.Task;
    public int ReferenceCount => Volatile.Read(ref _references);
    public bool IsActive => Volatile.Read(ref _activationState) == 1;

    public void Reserve()
    {
        if (Interlocked.Increment(ref _references) <= 1) throw new InvalidOperationException("Cannot revive a released binding.");
    }

    public void ActivationSucceeded()
    {
        if (Interlocked.CompareExchange(ref _activationState, 1, 0) == 0) _activation.TrySetResult();
    }

    public void ActivationFailed(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _activationState, 2, 0) == 0) _activation.TrySetException(exception);
    }

    public bool RollbackReservation()
    {
        int remaining = Interlocked.Decrement(ref _references);
        if (remaining < 0) throw new InvalidOperationException("Binding reservation underflow.");
        return remaining == 0;
    }

    public bool ReleaseReference()
    {
        if (Volatile.Read(ref _activationState) != 1) throw new InvalidOperationException("The binding is not active.");
        int remaining = Interlocked.Decrement(ref _references);
        if (remaining < 0) throw new InvalidOperationException("The binding was disposed too many times.");
        return remaining == 0;
    }

    public void Dispose() => _release(this);
}
