using System.Diagnostics;
using RabbitMQ.Component.Diagnostics;

namespace RabbitMQ.Component.Internal.Publishing;

internal sealed class PublisherManager : IAsyncDisposable
{
    private readonly ServiceSnapshot _options;
    private readonly ErrorSink _errors;
    private readonly PublishConnection _connection;
    private readonly Func<Task>? _beforeOpenCommit;
    private readonly object _sync = new();
    private readonly Dictionary<PublishKey, Entry> _entries = [];
    private readonly HashSet<Entry> _allEntries = [];
    private readonly HashSet<Task> _backgroundCleanup = [];
    private bool _disposed;
    private Task? _deferredConnectionDisposal;

    public PublisherManager(ServiceSnapshot options, ErrorSink errors)
        : this(options, errors, new RabbitMqPublishTransportFactory()) { }

    internal PublisherManager(ServiceSnapshot options, ErrorSink errors, IPublishTransportFactory transportFactory,
        Func<Task>? beforeOpenCommit = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _errors = errors ?? throw new ArgumentNullException(nameof(errors));
        ArgumentNullException.ThrowIfNull(transportFactory);
        _connection = new PublishConnection(options, errors, transportFactory);
        _beforeOpenCommit = beforeOpenCommit;
    }

    public async ValueTask<IPublishEndpoint<T>> OpenAsync<T>(PublishSnapshot<T> snapshot,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        cancellationToken.ThrowIfCancellationRequested();

        Entry<T> entry;
        bool initialize = false;
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_entries.TryGetValue(snapshot.Key, out Entry? existing))
            {
                entry = (Entry<T>)existing;
                if (!entry.Snapshot.IsCompatibleWith(snapshot))
                    throw new InvalidOperationException("A publishing endpoint with the same message type and routing key is already open with different configuration.");
            }
            else
            {
                Entry<T>? created = null;
                var endpoint = new PublishEndpoint<T>(snapshot, _connection, _errors, () => Release(created!));
                created = new Entry<T>(snapshot, endpoint);
                entry = created;
                _entries.Add(snapshot.Key, entry);
                _allEntries.Add(entry);
                initialize = true;
            }
            entry.PendingOpens++;
        }

        if (initialize) _ = InitializeAsync(entry);

        try
        {
            await entry.Initialized.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (_beforeOpenCommit is not null)
                await _beforeOpenCommit().WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            AbandonOpen(entry);
            throw;
        }

        lock (_sync)
        {
            entry.PendingOpens--;
            if (_disposed || !_entries.TryGetValue(entry.Key, out Entry? current) || !ReferenceEquals(current, entry))
                throw new ObjectDisposedException(nameof(PublisherManager));
            entry.References++;
            return entry.Endpoint;
        }
    }

    public async ValueTask DisposeAsync()
    {
        Entry[] entries;
        Task[] cleanupAlreadyRunning;
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
            entries = _allEntries.ToArray();
            _entries.Clear();
            cleanupAlreadyRunning = _backgroundCleanup.ToArray();
        }

        var stopwatch = Stopwatch.StartNew();
        var tasks = new HashSet<Task>(cleanupAlreadyRunning);
        foreach (Entry entry in entries) tasks.Add(StartCleanup(entry, cancelInitialization: true));
        Task cleanup = Task.WhenAll(tasks);

        if (await CompletesWithinAsync(cleanup, _options.DrainTimeout - stopwatch.Elapsed).ConfigureAwait(false))
        {
            // Connection shutdown shares the drain deadline, even after every worker has exited.
            // Keep observing cleanup in the background if the transport does not finish in time.
            _deferredConnectionDisposal = DisposeConnectionAfterAsync(cleanup);
            if (!await CompletesWithinAsync(_deferredConnectionDisposal,
                    _options.DrainTimeout - stopwatch.Elapsed).ConfigureAwait(false))
            {
                _errors.Report(new ComponentError
                {
                    Stage = ErrorStage.Drain,
                    Outcome = ErrorOutcome.Unknown,
                    Role = BrokerRole.Publish,
                    Resource = "publisher-connection",
                    Exception = new TimeoutException("The publishing connection did not close within the configured drain timeout.")
                });
            }
            return;
        }

        foreach (Entry entry in entries)
        {
            if (entry.Cleanup is { IsCompleted: true }) continue;
            await entry.EndpointControl.ReportDrainTimeout().ConfigureAwait(false);
            entry.EndpointControl.ForceStop();
        }

        // A synchronous external serializer cannot be preempted safely. Return after the configured bound,
        // but retain payload/connection ownership until every worker actually exits in the background.
        _deferredConnectionDisposal = DisposeConnectionAfterAsync(cleanup);
    }

    private async Task InitializeAsync<T>(Entry<T> entry)
    {
        try
        {
            await entry.Endpoint.InitializeAsync(entry.InitializationCancellation.Token).ConfigureAwait(false);
            entry.Initialized.TrySetResult();
        }
        catch (Exception exception)
        {
            entry.Initialized.TrySetException(exception);
            Task cleanup;
            lock (_sync)
            {
                if (_entries.TryGetValue(entry.Key, out Entry? current) && ReferenceEquals(current, entry))
                    _entries.Remove(entry.Key);
                cleanup = StartCleanupUnderLock(entry, cancelInitialization: false);
            }
            _ = cleanup;
            return;
        }

        Task? orphanCleanup = null;
        lock (_sync)
        {
            if (entry.PendingOpens == 0 && entry.References == 0)
            {
                if (_entries.TryGetValue(entry.Key, out Entry? current) && ReferenceEquals(current, entry))
                    _entries.Remove(entry.Key);
                orphanCleanup = StartCleanupUnderLock(entry, cancelInitialization: false);
            }
        }
        _ = orphanCleanup;
    }

    private void AbandonOpen(Entry entry)
    {
        Task? cleanup = null;
        lock (_sync)
        {
            entry.PendingOpens--;
            if (entry.PendingOpens == 0 && entry.References == 0)
            {
                if (_entries.TryGetValue(entry.Key, out Entry? current) && ReferenceEquals(current, entry))
                    _entries.Remove(entry.Key);
                cleanup = StartCleanupUnderLock(entry, cancelInitialization: true);
            }
        }
        _ = cleanup;
    }

    private void Release(Entry entry)
    {
        Task? cleanup = null;
        lock (_sync)
        {
            if (entry.References <= 0)
                throw new InvalidOperationException("The publishing endpoint was disposed more times than it was opened.");
            entry.References--;
            if (entry.References == 0 && entry.PendingOpens == 0)
            {
                if (_entries.TryGetValue(entry.Key, out Entry? current) && ReferenceEquals(current, entry))
                    _entries.Remove(entry.Key);
                cleanup = StartCleanupUnderLock(entry, cancelInitialization: false);
            }
        }
        _ = cleanup;
    }

    private Task StartCleanup(Entry entry, bool cancelInitialization)
    {
        lock (_sync) return StartCleanupUnderLock(entry, cancelInitialization);
    }

    private Task StartCleanupUnderLock(Entry entry, bool cancelInitialization)
    {
        if (cancelInitialization)
        {
            try { entry.InitializationCancellation.Cancel(); }
            catch (ObjectDisposedException) { }
        }
        if (entry.Cleanup is not null) return entry.Cleanup;

        Task cleanup = CleanupEntryAsync(entry);
        entry.Cleanup = cleanup;
        _backgroundCleanup.Add(cleanup);
        _ = cleanup.ContinueWith(static (completed, state) =>
        {
            var tuple = ((PublisherManager Manager, Entry Entry))state!;
            lock (tuple.Manager._sync)
            {
                tuple.Manager._backgroundCleanup.Remove(completed);
                tuple.Manager._allEntries.Remove(tuple.Entry);
            }
        }, (this, entry), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return cleanup;
    }

    private async Task CleanupEntryAsync(Entry entry)
    {
        try
        {
            try { await entry.Initialized.Task.ConfigureAwait(false); }
            catch { }

            Task drain;
            try { drain = entry.EndpointControl.BeginClose(); }
            catch { return; }

            try
            {
                if (await CompletesWithinAsync(drain, _options.DrainTimeout).ConfigureAwait(false)) return;
                await entry.EndpointControl.ReportDrainTimeout().ConfigureAwait(false);
                entry.EndpointControl.ForceStop();
                // A synchronous serializer cannot be interrupted. Keep the cleanup task alive in the
                // background so its payload and channel remain owned until the worker really exits.
                await drain.ConfigureAwait(false);
            }
            catch { }
        }
        finally
        {
            entry.InitializationCancellation.Dispose();
        }
    }

    private async Task DisposeConnectionAfterAsync(Task cleanup)
    {
        try { await cleanup.ConfigureAwait(false); }
        catch { }
        try { await _connection.DisposeAsync().ConfigureAwait(false); }
        catch (Exception exception)
        {
            _errors.Report(new ComponentError
            {
                Stage = ErrorStage.Cleanup,
                Outcome = ErrorOutcome.Failed,
                Role = BrokerRole.Publish,
                Resource = "publisher-connection",
                Exception = exception
            });
        }
    }

    private static async Task<bool> CompletesWithinAsync(Task task, TimeSpan timeout)
    {
        if (task.IsCompleted)
        {
            await task.ConfigureAwait(false);
            return true;
        }
        if (timeout <= TimeSpan.Zero) return false;
        Task delay = Task.Delay(timeout);
        if (await Task.WhenAny(task, delay).ConfigureAwait(false) != task) return false;
        await task.ConfigureAwait(false);
        return true;
    }

    private abstract class Entry(PublishKey key, IPublishEndpointControl endpointControl)
    {
        public PublishKey Key { get; } = key;
        public IPublishEndpointControl EndpointControl { get; } = endpointControl;
        public CancellationTokenSource InitializationCancellation { get; } = new();
        public TaskCompletionSource Initialized { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int PendingOpens { get; set; }
        public int References { get; set; }
        public Task? Cleanup { get; set; }
    }

    private sealed class Entry<T>(PublishSnapshot<T> snapshot, PublishEndpoint<T> endpoint)
        : Entry(snapshot.Key, endpoint)
    {
        public PublishSnapshot<T> Snapshot { get; } = snapshot;
        public PublishEndpoint<T> Endpoint { get; } = endpoint;
    }
}
