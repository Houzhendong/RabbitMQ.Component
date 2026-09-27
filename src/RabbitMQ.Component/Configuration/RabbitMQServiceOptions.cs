using RabbitMQ.Component.Diagnostics;

namespace RabbitMQ.Component.Configuration;

public sealed class RabbitMQServiceOptions
{
    public BrokerConnectionOptions PublishBroker { get; set; } = new();
    public BrokerConnectionOptions SubscriptionBroker { get; set; } = new();
    public TimeSpan ReconnectMinDelay { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(10);
    /// <summary>Called synchronously, possibly concurrently. Exceptions are isolated. Do not use async void.</summary>
    public Action<ComponentError>? ErrorObserver { get; set; }
}
