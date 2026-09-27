using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Component.Configuration;
using Xunit;

namespace RabbitMQ.Component.IntegrationTests;

/// <summary>Skips only when either required setting is absent, never on a connection failure.</summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class BrokerFactAttribute : FactAttribute
{
    public BrokerFactAttribute()
    {
        var missing = BrokerTestEnvironment.MissingVariables();
        if (missing.Length > 0)
            Skip = $"Set the following environment variables to run this integration test: {string.Join(", ", missing)}.";
    }
}

/// <summary>Reads existing process settings without logging or persisting credentials.</summary>
public static class BrokerTestEnvironment
{
    public const string PrimaryVariable = "RABBITMQ_PRIMARY_URI";
    public const string SecondaryVariable = "RABBITMQ_SECONDARY_URI";
    public static TimeSpan TestTimeout { get; } = TimeSpan.FromSeconds(30);
    public static TimeSpan CleanupTimeout { get; } = TimeSpan.FromSeconds(10);

    public static string[] MissingVariables() =>
        new[] { PrimaryVariable, SecondaryVariable }
            .Where(name => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name)))
            .ToArray();

    public static BrokerConnectionOptions Primary() => ReadOptions(PrimaryVariable);
    public static BrokerConnectionOptions Secondary() => ReadOptions(SecondaryVariable);

    public static BrokerConnectionOptions ReadOptions(string variable)
    {
        if (variable != PrimaryVariable && variable != SecondaryVariable)
            throw new ArgumentException("Use one of the two supported broker environment variable names.", nameof(variable));

        var value = Environment.GetEnvironmentVariable(variable);
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) ||
            (uri.Scheme != "amqp" && uri.Scheme != "amqps"))
            throw new InvalidOperationException($"{variable} must contain a valid AMQP URI.");

        return new BrokerConnectionOptions
        {
            Uri = uri,
            ClientProvidedName = "RabbitMQ.Component.IntegrationTests",
            ConnectionTimeout = TimeSpan.FromSeconds(10),
            RequestedHeartbeat = TimeSpan.FromSeconds(10)
        };
    }

    public static ConnectionFactory CreateFactory(BrokerConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        // Use the component's factory configuration, including disabled automatic recovery.
        // Do not retain the original exception: malformed URI exceptions can include its value.
        try
        {
            return options.CreateFactory();
        }
        catch (Exception)
        {
            throw new InvalidOperationException("Unable to construct the integration-test broker connection factory.");
        }
    }

    public static CancellationTokenSource CreateTimeout(TimeSpan? timeout = null) =>
        new(timeout ?? TestTimeout);

    public static string UniqueResourceName(string purpose = "resource")
    {
        if (string.IsNullOrWhiteSpace(purpose) || purpose.Length > 60 ||
            purpose.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '-' && c != '_' && c != '.'))
            throw new ArgumentException("Use a resource purpose of 1-60 ASCII letters, digits, dots, underscores or hyphens.", nameof(purpose));

        return $"rabbitmq-component-it.{purpose}.{Guid.NewGuid():N}";
    }

    public static BrokerTestTopology CreateTopology(string purpose = "topology") =>
        new(UniqueResourceName(purpose));
}

/// <summary>
/// Owns only its generated exchange and queue. Use one instance per test and broker.
/// Cleanup should use a fresh connection to the same broker so a declaration-closed channel cannot mask the test failure.
/// </summary>
public sealed class BrokerTestTopology
{
    private bool _exchangeDeclarationAttempted;
    private bool _queueDeclarationAttempted;

    internal BrokerTestTopology(string prefix)
    {
        ExchangeName = $"{prefix}.exchange";
        QueueName = $"{prefix}.queue";
        RoutingKey = $"{prefix}.route";
    }

    public string ExchangeName { get; }
    public string QueueName { get; }
    public string RoutingKey { get; }

    public async Task DeclareAsync(IChannel channel, CancellationToken cancellationToken)
    {
        // Record attempts before sending so a timeout after broker-side creation is cleaned up too.
        _exchangeDeclarationAttempted = true;
        await channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Direct,
            durable: false, autoDelete: false, cancellationToken: cancellationToken);
        _queueDeclarationAttempted = true;
        await channel.QueueDeclareAsync(QueueName, durable: true, exclusive: false,
            autoDelete: false, cancellationToken: cancellationToken);
        await channel.QueueBindAsync(QueueName, ExchangeName, RoutingKey,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Uses independent deadlines and fresh channels so a failed test channel or queue deletion
    /// cannot prevent attempting exchange deletion. Does not enumerate or purge broker resources.
    /// </summary>
    public async Task CleanupAsync(IConnection connection)
    {
        var failures = new List<Exception>();
        if (_queueDeclarationAttempted)
            await DeleteAsync(connection, queue: true, failures);
        if (_exchangeDeclarationAttempted)
            await DeleteAsync(connection, queue: false, failures);
        if (failures.Count > 0)
            throw new AggregateException("Failed to clean up this test's topology.", failures);
    }

    private async Task DeleteAsync(IConnection connection, bool queue, List<Exception> failures)
    {
        using var timeout = BrokerTestEnvironment.CreateTimeout(BrokerTestEnvironment.CleanupTimeout);
        try
        {
            await using var channel = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            if (queue)
                await channel.QueueDeleteAsync(QueueName, ifUnused: false, ifEmpty: false,
                    cancellationToken: timeout.Token);
            else
                await channel.ExchangeDeleteAsync(ExchangeName, ifUnused: false,
                    cancellationToken: timeout.Token);
        }
        catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
        {
            // An interrupted declaration may not have reached the broker, or cleanup ran already.
        }
        catch (Exception exception)
        {
            // Preserve failure, but not broker exception text that could expose connection details.
            string resource = queue ? QueueName : ExchangeName;
            failures.Add(new InvalidOperationException(
                $"Could not delete this test's {(queue ? "queue" : "exchange")} '{resource}' " +
                $"({exception.GetType().Name})."));
        }
    }
}
