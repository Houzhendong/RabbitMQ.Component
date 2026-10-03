using System.Buffers;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Component;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Serialization;

LoadLocalEnvironment();

if (args.Any(argument => argument != "--continuous"))
    throw new InvalidOperationException("Usage: dotnet run --project samples/RabbitMQ.Component.Sample -- [--continuous]");
bool continuous = args.Contains("--continuous");
Uri primaryUri = ReadBrokerUri("RABBITMQ_PRIMARY_URI");
Uri? secondaryUri = continuous ? null : ReadBrokerUri("RABBITMQ_SECONDARY_URI");
string resource = $"rabbitmq-component-sample.{Guid.NewGuid():N}";
string exchange = $"{resource}.exchange";
string queue = $"{resource}.queue";
string route = $"{resource}.route";
Exception? operationFailure = null;
Exception? cleanupFailure = null;

try
{
    if (continuous)
        await ContinuousSample.RunAsync(primaryUri, exchange, queue, route);
    else
        await RunSampleAsync(primaryUri, secondaryUri!, exchange, queue, route);
}
catch (Exception exception)
{
    operationFailure = Redact("The sample operation failed", exception);
}

try
{
    if (secondaryUri is null)
        await CleanupBrokerAsync(primaryUri, exchange, queue);
    else
        await Task.WhenAll(
            CleanupBrokerAsync(primaryUri, exchange, queue),
            CleanupBrokerAsync(secondaryUri, exchange, queue));
}
catch (Exception exception)
{
    cleanupFailure = Redact($"Could not clean sample topology '{exchange}'/'{queue}'", exception);
}

if (operationFailure is not null && cleanupFailure is not null)
    throw new AggregateException("The sample operation and exact topology cleanup both failed.", operationFailure, cleanupFailure);
if (operationFailure is not null) throw operationFailure;
if (cleanupFailure is not null) throw cleanupFailure;

static async Task RunSampleAsync(Uri primaryUri, Uri secondaryUri, string exchange, string queue, string route)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
    CancellationToken cancellationToken = timeout.Token;
    var received = Channel.CreateUnbounded<SampleMessage>();
    // Both component endpoints and raw AMQP peers use the same JSON + gzip wire format.
    var codec = new CompositeCodec<SampleMessage>(JsonMessageSerializer<SampleMessage>.Default, new GzipCompressor());
    var exchangeConfig = new ExchangeConfig
    {
        Name = exchange,
        Durable = true,
        AutoDelete = false
    };

    var services = new ServiceCollection();
    services.AddRabbitMQComponent(options =>
    {
        options.PublishBroker = new BrokerConnectionOptions { Uri = primaryUri };
        options.SubscriptionBroker = new BrokerConnectionOptions { Uri = primaryUri };
        options.DrainTimeout = TimeSpan.FromSeconds(10);
        options.ErrorObserver = error => Console.Error.WriteLine(
            "RabbitMQ component error: " +
            $"role={error.Role}, stage={error.Stage}, outcome={error.Outcome}, " +
            $"resource={error.Resource}, exceptionType={error.Exception.GetType().Name}");
    });

    await using ServiceProvider provider = services.BuildServiceProvider();
    IRabbitMQService service = provider.GetRequiredService<IRabbitMQService>();
    IReceiver<SampleMessage> receiver = await service.OpenReceiverAsync(
        new SubscriptionConfig<SampleMessage>
        {
            Queue = new QueueConfig
            {
                Name = queue,
                Durable = false,
                Exclusive = true,
                AutoDelete = true,
                Arguments = new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "classic",
                    ["x-expires"] = 60_000
                }
            },
            Codec = codec
        },
        message => received.Writer.TryWrite(message),
        cancellationToken);

    using IQueueBinding binding = await receiver.BindAsync(new BindingConfig
    {
        Exchange = exchangeConfig,
        RoutingKey = route
    }, cancellationToken);

    using IPublishEndpoint<SampleMessage> publisher = await service.OpenPublishEndpointAsync(
        new PublishConfig<SampleMessage>
        {
            Exchange = exchangeConfig,
            RoutingKey = route,
            Codec = codec
        },
        cancellationToken);

    EnsureAccepted(publisher.TrySend(new SampleMessage("before-switch")));
    SampleMessage first = await received.Reader.ReadAsync(cancellationToken);
    Console.WriteLine($"Received {first.Text} from the primary subscription broker.");

    await service.SwitchSubscriptionBrokerAsync(
        new BrokerConnectionOptions { Uri = secondaryUri },
        cancellationToken);

    await using (IConnection secondaryConnection = await CreateFactory(secondaryUri)
        .CreateConnectionAsync(cancellationToken))
    await using (IChannel secondaryChannel = await secondaryConnection
        .CreateChannelAsync(cancellationToken: cancellationToken))
    {
        var output = new ArrayBufferWriter<byte>();
        codec.Encode(new SampleMessage("after-switch"), output);
        ReadOnlyMemory<byte> body = output.WrittenMemory;

        // BasicPublishAsync borrows body's backing array. Keep output/body unchanged and alive
        // until the transport operation has actually completed; the Span-returning Encode overload
        // cannot provide memory with a lifetime that safely crosses this await.
        await secondaryChannel.BasicPublishAsync(exchange, route, body, cancellationToken);
    }

    SampleMessage second = await received.Reader.ReadAsync(cancellationToken);
    Console.WriteLine($"Received {second.Text} from the secondary subscription broker.");

    await using IConnection primaryConnection = await CreateFactory(primaryUri)
        .CreateConnectionAsync(cancellationToken);
    await using IChannel primaryChannel = await primaryConnection
        .CreateChannelAsync(cancellationToken: cancellationToken);
    // Switching closes the old subscriber and deletes its exclusive auto-delete queue.
    // Recreate that topology on the verification connection before testing the publisher.
    await primaryChannel.QueueDeclareAsync(
        queue, durable: false, exclusive: true, autoDelete: true,
        arguments: new Dictionary<string, object?>
        {
            ["x-queue-type"] = "classic",
            ["x-expires"] = 60_000
        },
        cancellationToken: cancellationToken);
    await primaryChannel.QueueBindAsync(queue, exchange, route, cancellationToken: cancellationToken);
    TaskCompletionSource<SampleMessage> primaryDelivery = await StartConsumerAsync(
        primaryChannel, queue, codec, cancellationToken);

    EnsureAccepted(publisher.TrySend(new SampleMessage("publishing-still-primary")));
    SampleMessage third = await primaryDelivery.Task.WaitAsync(cancellationToken);
    if (third.Text != "publishing-still-primary")
        throw new InvalidOperationException("The primary verification consumer received an unexpected sample message.");
    Console.WriteLine("Verified that the original publisher still delivers to the primary broker.");
}

static async Task<TaskCompletionSource<SampleMessage>> StartConsumerAsync(
    IChannel channel,
    string queue,
    ICodec<SampleMessage> codec,
    CancellationToken cancellationToken)
{
    var completion = new TaskCompletionSource<SampleMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (_, delivery) =>
    {
        try
        {
            SampleMessage message = codec.Decode(delivery.Body.Span);
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken);
            completion.TrySetResult(message);
        }
        catch (Exception exception)
        {
            completion.TrySetException(Redact("The primary verification consumer failed", exception));
        }
    };
    await channel.BasicConsumeAsync(queue, autoAck: false, consumer, cancellationToken);
    return completion;
}

static async Task CleanupBrokerAsync(Uri uri, string exchange, string queue)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    IConnection connection;
    try
    {
        connection = await CreateFactory(uri).CreateConnectionAsync(timeout.Token);
    }
    catch (Exception exception)
    {
        throw Redact($"Could not connect to clean sample topology '{exchange}'/'{queue}'", exception);
    }

    var failures = new List<Exception>();
    await using (connection)
    {
        try { await DeleteAsync(connection, queue, isQueue: true, timeout.Token); }
        catch (Exception exception) { failures.Add(exception); }
        try { await DeleteAsync(connection, exchange, isQueue: false, timeout.Token); }
        catch (Exception exception) { failures.Add(exception); }
    }
    if (failures.Count != 0)
        throw new AggregateException("Exact sample topology cleanup failed.", failures);
}

static async Task DeleteAsync(
    IConnection connection,
    string resource,
    bool isQueue,
    CancellationToken cancellationToken)
{
    try
    {
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        if (isQueue)
            await channel.QueueDeleteAsync(resource, ifUnused: false, ifEmpty: false, cancellationToken: cancellationToken);
        else
            await channel.ExchangeDeleteAsync(resource, ifUnused: false, cancellationToken: cancellationToken);
    }
    catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
    {
        // This broker may not have reached the declaration step.
    }
    catch (Exception exception)
    {
        throw Redact($"Could not delete sample-owned {(isQueue ? "queue" : "exchange")} '{resource}'", exception);
    }
}

static ConnectionFactory CreateFactory(Uri uri) => new()
{
    Uri = uri,
    AutomaticRecoveryEnabled = false,
    TopologyRecoveryEnabled = false,
    RequestedConnectionTimeout = TimeSpan.FromSeconds(10)
};

// Find the repository .env when launched from the repository, sample directory, or build output.
// Existing non-empty process environment variables take precedence over local settings.
static void LoadLocalEnvironment()
{
    string? path = FindEnvironmentFile(Directory.GetCurrentDirectory())
        ?? FindEnvironmentFile(AppContext.BaseDirectory);
    if (path is null) return;

    foreach (string line in File.ReadLines(path))
    {
        string text = line.Trim();
        if (text.Length == 0 || text.StartsWith('#')) continue;
        int separator = text.IndexOf('=');
        if (separator < 0) continue;
        string name = text[..separator].Trim();
        if (name is not ("RABBITMQ_PRIMARY_URI" or "RABBITMQ_SECONDARY_URI")) continue;
        if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name))) continue;

        string value = text[(separator + 1)..].Trim();
        if (value.Length >= 2 &&
            ((value[0] == '"' && value[^1] == '"') || (value[0] == '\'' && value[^1] == '\'')))
            value = value[1..^1];
        Environment.SetEnvironmentVariable(name, value);
    }
}

static string? FindEnvironmentFile(string startDirectory)
{
    for (DirectoryInfo? directory = new(startDirectory); directory is not null; directory = directory.Parent)
    {
        if (!File.Exists(Path.Combine(directory.FullName, "RabbitMQ.Component.sln"))) continue;
        string path = Path.Combine(directory.FullName, ".env");
        return File.Exists(path) ? path : null;
    }
    return null;
}

static Uri ReadBrokerUri(string variable)
{
    string? value = Environment.GetEnvironmentVariable(variable);
    if (!Uri.TryCreate(value, UriKind.Absolute, out Uri? uri) ||
        (uri.Scheme != "amqp" && uri.Scheme != "amqps"))
        throw new InvalidOperationException($"Set {variable} to a valid AMQP URI in the repository .env file or process environment.");
    return uri;
}

static void EnsureAccepted(EnqueueResult result)
{
    if (result != EnqueueResult.Accepted)
        throw new InvalidOperationException($"The sample message was not buffered ({result}).");
}

static InvalidOperationException Redact(string context, Exception exception) =>
    new($"{context} ({exception.GetType().Name}).");

public sealed record SampleMessage(string Text);
