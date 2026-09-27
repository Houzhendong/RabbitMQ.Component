using Microsoft.Extensions.Logging;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Internal;
using RabbitMQ.Component.Internal.Publishing;
using RabbitMQ.Component.Internal.Subscriptions;

namespace RabbitMQ.Component;

public sealed class RabbitMQService : IRabbitMQService
{
    private readonly PublisherManager _publishers;
    private readonly SubscriptionManager _subscriptions;
    private readonly object _disposeLock = new();
    private Task? _disposeTask;
    private int _disposeStarted;

    public RabbitMQService(
        RabbitMQServiceOptions options,
        ILogger<RabbitMQService>? logger = null)
    {
        ServiceSnapshot snapshot = ConfigSnapshots.Service(options);
        var errors = new ErrorSink(snapshot.ErrorObserver, logger);
        _publishers = new PublisherManager(snapshot, errors);
        _subscriptions = new SubscriptionManager(snapshot, errors);
    }

    public ValueTask<IPublishEndpoint<T>> OpenPublishEndpointAsync<T>(
        PublishConfig<T> config,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposing();
        return _publishers.OpenAsync(ConfigSnapshots.Publish(config), cancellationToken);
    }

    public ValueTask<IReceiver<T>> OpenReceiverAsync<T>(
        SubscriptionConfig<T> config,
        Action<T> handler,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposing();
        return _subscriptions.OpenAsync(ConfigSnapshots.Subscription(config, handler), cancellationToken);
    }

    public Task SwitchSubscriptionBrokerAsync(
        BrokerConnectionOptions options,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposing();
        return _subscriptions.SwitchBrokerAsync(ConfigSnapshots.Broker(options), cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposeLock)
        {
            if (_disposeTask is not null)
                return new ValueTask(_disposeTask);

            Volatile.Write(ref _disposeStarted, 1);
            _disposeTask = DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        // Invoke both managers before awaiting either one. Each manager atomically closes its own
        // admission path, and publisher disposal can then cancel a connection initialization in flight.
        Task publisherDisposal = StartDisposal(_publishers);
        Task subscriptionDisposal = StartDisposal(_subscriptions);
        List<Exception>? failures = null;

        try
        {
            await publisherDisposal.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            (failures ??= []).Add(exception);
        }

        try
        {
            await subscriptionDisposal.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            (failures ??= []).Add(exception);
        }

        if (failures is { Count: 1 })
            throw failures[0];
        if (failures is { Count: > 1 })
            throw new AggregateException("RabbitMQ component cleanup failed.", failures);
    }

    private static Task StartDisposal(IAsyncDisposable disposable)
    {
        try { return disposable.DisposeAsync().AsTask(); }
        catch (Exception exception) { return Task.FromException(exception); }
    }

    private void ThrowIfDisposing()
    {
        if (Volatile.Read(ref _disposeStarted) != 0)
            throw new ObjectDisposedException(nameof(RabbitMQService));
    }
}
