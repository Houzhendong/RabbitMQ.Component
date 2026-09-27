using RabbitMQ.Component.Configuration;

namespace RabbitMQ.Component;

public interface IRabbitMQService : IAsyncDisposable
{
    ValueTask<IPublishEndpoint<T>> OpenPublishEndpointAsync<T>(PublishConfig<T> config, CancellationToken cancellationToken = default);
    ValueTask<IReceiver<T>> OpenReceiverAsync<T>(SubscriptionConfig<T> config, Action<T> handler, CancellationToken cancellationToken = default);
    /// <summary>Switches only subscriptions; does not move queued messages or change publishing.</summary>
    Task SwitchSubscriptionBrokerAsync(BrokerConnectionOptions options, CancellationToken cancellationToken = default);
}

/// <summary>Shared instance. Each successful Open requires one Dispose; Dispose is not idempotent.</summary>
public interface IPublishEndpoint<T> : IDisposable
{
    /// <summary>Accepted means buffered, not delivered. Do not mutate the value after acceptance.</summary>
    EnqueueResult TrySend(T message);
}

/// <summary>A logical receiver, owned internally. Dispose bindings, not the receiver.</summary>
public interface IReceiver<T>
{
    ValueTask<IQueueBinding> BindAsync(BindingConfig config, CancellationToken cancellationToken = default);
}

/// <summary>Shared instance. Each successful Bind requires one Dispose; Dispose is not idempotent.</summary>
public interface IQueueBinding : IDisposable { }

public enum EnqueueResult { Accepted, BufferFull, Unavailable, Closed }
