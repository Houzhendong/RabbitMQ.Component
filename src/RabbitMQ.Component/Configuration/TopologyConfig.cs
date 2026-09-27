using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Configuration;

public sealed class ExchangeConfig
{
    public string Name { get; set; } = "";
    public string Type { get; set; } = "direct";
    public bool Durable { get; set; } = true;
    public bool AutoDelete { get; set; }
    public IDictionary<string, object?>? Arguments { get; set; }
}

public sealed class QueueConfig
{
    public string Name { get; set; } = "";
    public bool Durable { get; set; } = true;
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public IDictionary<string, object?>? Arguments { get; set; }
}

public sealed class PublishConfig<T>
{
    public ExchangeConfig Exchange { get; set; } = new();
    public string RoutingKey { get; set; } = "";
    public required ICodec<T> Codec { get; set; }
    public int BufferCapacity { get; set; } = 1024;
}

public sealed class ConsumerConfig
{
    public bool AutoAck { get; set; }
    public string ConsumerTag { get; set; } = "";
    public bool NoLocal { get; set; }
    public bool Exclusive { get; set; }
    public IDictionary<string, object?>? Arguments { get; set; }
}

public sealed class SubscriptionConfig<T>
{
    public QueueConfig Queue { get; set; } = new();
    public ConsumerConfig Consumer { get; set; } = new();
    public required ICodec<T> Codec { get; set; }
    /// <summary>Zero requests unlimited prefetch, as specified by AMQP.</summary>
    public ushort PrefetchCount { get; set; } = 32;
}

public sealed class BindingConfig
{
    public ExchangeConfig Exchange { get; set; } = new();
    public string RoutingKey { get; set; } = "";
    public IDictionary<string, object?>? Arguments { get; set; }
}
