namespace RabbitMQ.Component.Diagnostics;

public enum ErrorStage { Encoding, Publish, Decoding, Consume, Topology, Connection, BrokerSwitch, Cleanup, Drain }
public enum ErrorOutcome { Failed, Unknown }
public enum BrokerRole { Publish, Subscription }

/// <summary>
/// OriginalMessage correlates a publishing failure to the caller's immutable value. It is never logged
/// by the default sink. Observers receive the exception; their own logging must redact sensitive data.
/// Resource identifies an exchange/routing key/queue, never a credential-bearing connection URI.
/// </summary>
public sealed class ComponentError
{
    public required ErrorStage Stage { get; init; }
    public required ErrorOutcome Outcome { get; init; }
    public required BrokerRole Role { get; init; }
    public required string Resource { get; init; }
    public required Exception Exception { get; init; }
    public object? OriginalMessage { get; init; }
    public Type? MessageType { get; init; }
    public ulong? DeliveryTag { get; init; }
    public long? Generation { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public override string ToString() => $"{Role}/{Stage}/{Outcome}";
}
