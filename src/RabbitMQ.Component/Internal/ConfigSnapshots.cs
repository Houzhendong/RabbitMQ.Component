using System.Runtime.CompilerServices;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Diagnostics;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Internal;

internal sealed record ExchangeSnapshot(string Name, string Type, bool Durable, bool AutoDelete, AmqpTable Arguments);
internal sealed record QueueSnapshot(string Name, bool Durable, bool Exclusive, bool AutoDelete, AmqpTable Arguments);
internal sealed record ConsumerSnapshot(bool AutoAck, string ConsumerTag, bool NoLocal, bool Exclusive, AmqpTable Arguments);
internal sealed record BindingSnapshot(ExchangeSnapshot Exchange, string RoutingKey, AmqpTable Arguments)
{
    public BindingKey Key(string queueName) => new(Exchange.Name, queueName, RoutingKey, Arguments);
}
internal readonly record struct PublishKey(Type MessageType, string RoutingKey);
internal sealed record BindingKey(string ExchangeName, string QueueName, string RoutingKey, AmqpTable Arguments);
internal sealed record PublishSnapshot<T>(ExchangeSnapshot Exchange, string RoutingKey, ICodec<T> Codec, int BufferCapacity)
{
    public PublishKey Key => new(typeof(T), RoutingKey);
    public bool IsCompatibleWith(PublishSnapshot<T> other) => Exchange == other.Exchange && RoutingKey == other.RoutingKey
        && ReferenceEquals(Codec, other.Codec) && BufferCapacity == other.BufferCapacity;
}
internal sealed record SubscriptionSnapshot<T>(QueueSnapshot Queue, ConsumerSnapshot Consumer, ICodec<T> Codec,
    ushort PrefetchCount, Action<T> Handler)
{
    public bool IsCompatibleWith(SubscriptionSnapshot<T> other) => Queue == other.Queue && Consumer == other.Consumer
        && PrefetchCount == other.PrefetchCount && ReferenceEquals(Codec, other.Codec) && Handler.Equals(other.Handler);
}
internal sealed record ServiceSnapshot(BrokerConnectionOptions PublishBroker, BrokerConnectionOptions SubscriptionBroker,
    TimeSpan ReconnectMinDelay, TimeSpan ReconnectMaxDelay, TimeSpan DrainTimeout, Action<ComponentError>? ErrorObserver)
{
    public TimeSpan ReconnectDelay(int attempt)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(attempt);
        return TimeSpan.FromMilliseconds(Math.Min(ReconnectMaxDelay.TotalMilliseconds,
            ReconnectMinDelay.TotalMilliseconds * Math.Pow(2, Math.Min(attempt, 62))));
    }
}

internal static class ConfigSnapshots
{
    public static ExchangeSnapshot Exchange(ExchangeConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        AmqpTable.ValidateShortString(config.Name, nameof(config.Name));
        AmqpTable.ValidateShortString(config.Type, nameof(config.Type), allowEmpty: false);
        return new(config.Name, config.Type, config.Durable, config.AutoDelete, AmqpTable.Capture(config.Arguments));
    }

    public static QueueSnapshot Queue(QueueConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        AmqpTable.ValidateShortString(config.Name, nameof(config.Name), allowEmpty: false);
        return new(config.Name, config.Durable, config.Exclusive, config.AutoDelete, AmqpTable.Capture(config.Arguments));
    }

    public static BindingSnapshot Binding(BindingConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        var exchange = Exchange(config.Exchange);
        if (exchange.Name.Length == 0) throw new ArgumentException("The default exchange cannot be explicitly bound.", nameof(config));
        AmqpTable.ValidateShortString(config.RoutingKey, nameof(config.RoutingKey));
        return new(exchange, config.RoutingKey, AmqpTable.Capture(config.Arguments));
    }

    public static ConsumerSnapshot Consumer(ConsumerConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);
        AmqpTable.ValidateShortString(config.ConsumerTag, nameof(config.ConsumerTag));
        return new(config.AutoAck, config.ConsumerTag, config.NoLocal, config.Exclusive, AmqpTable.Capture(config.Arguments));
    }

    public static PublishSnapshot<T> Publish<T>(PublishConfig<T> config)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(config.Codec);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(config.BufferCapacity);
        AmqpTable.ValidateShortString(config.RoutingKey, nameof(config.RoutingKey));
        return new(Exchange(config.Exchange), config.RoutingKey, config.Codec, config.BufferCapacity);
    }

    public static SubscriptionSnapshot<T> Subscription<T>(SubscriptionConfig<T> config, Action<T> handler)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(config.Codec);
        ValidateSynchronous(handler, nameof(handler));
        return new(Queue(config.Queue), Consumer(config.Consumer), config.Codec, config.PrefetchCount, handler);
    }

    public static BrokerConnectionOptions Broker(BrokerConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Uri is not null)
        {
            if (!options.Uri.IsAbsoluteUri || (options.Uri.Scheme != "amqp" && options.Uri.Scheme != "amqps"))
                throw new ArgumentException("Broker URI must use amqp or amqps.", nameof(options));
        }
        else
        {
            if (string.IsNullOrWhiteSpace(options.HostName)) throw new ArgumentException("Broker host is required.", nameof(options));
            if (options.Port is < 1 or > 65535) throw new ArgumentOutOfRangeException(nameof(options.Port));
            ArgumentNullException.ThrowIfNull(options.VirtualHost);
            ArgumentNullException.ThrowIfNull(options.UserName);
            ArgumentNullException.ThrowIfNull(options.Password);
        }
        ValidateDuration(options.ConnectionTimeout, nameof(options.ConnectionTimeout));
        if (options.RequestedHeartbeat < TimeSpan.Zero || options.RequestedHeartbeat.TotalSeconds > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(options.RequestedHeartbeat));
        var snapshot = new BrokerConnectionOptions
        {
            Uri = options.Uri, HostName = options.HostName, Port = options.Port, VirtualHost = options.VirtualHost,
            UserName = options.UserName, Password = options.Password, ClientProvidedName = options.ClientProvidedName,
            RequestedHeartbeat = options.RequestedHeartbeat, ConnectionTimeout = options.ConnectionTimeout
        };
        // Validate the client's URI parser without opening a connection; do not leak URI/credentials on failure.
        try { _ = snapshot.CreateFactory(); }
        catch { throw new ArgumentException("Invalid broker connection settings.", nameof(options)); }
        return snapshot;
    }

    public static ServiceSnapshot Service(RabbitMQServiceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateDuration(options.ReconnectMinDelay, nameof(options.ReconnectMinDelay));
        ValidateDuration(options.ReconnectMaxDelay, nameof(options.ReconnectMaxDelay));
        ValidateDuration(options.DrainTimeout, nameof(options.DrainTimeout));
        if (options.ReconnectMinDelay > options.ReconnectMaxDelay) throw new ArgumentException("Reconnect delays are reversed.", nameof(options));
        if (options.ErrorObserver is not null) ValidateSynchronous(options.ErrorObserver, nameof(options.ErrorObserver));
        return new(Broker(options.PublishBroker), Broker(options.SubscriptionBroker), options.ReconnectMinDelay,
            options.ReconnectMaxDelay, options.DrainTimeout, options.ErrorObserver);
    }

    private static void ValidateDuration(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero || value.TotalMilliseconds > int.MaxValue) throw new ArgumentOutOfRangeException(name);
    }

    private static void ValidateSynchronous(Delegate callback, string name)
    {
        ArgumentNullException.ThrowIfNull(callback, name);
        if (callback.GetInvocationList().Any(item => item.Method.IsDefined(typeof(AsyncStateMachineAttribute), inherit: false)))
            throw new ArgumentException("Async void callbacks are not supported; use a synchronous callback.", name);
    }
}
