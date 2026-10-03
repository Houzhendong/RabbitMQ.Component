using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Component;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Serialization;

internal static class ContinuousSample
{
    public static async Task RunAsync(Uri brokerUri, string exchange, string queue, string route)
    {
        using var shutdown = new CancellationTokenSource();
        ConsoleCancelEventHandler cancel = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            shutdown.Cancel();
        };
        Console.CancelKeyPress += cancel;
        try
        {
            await RunCoreAsync(brokerUri, exchange, queue, route, shutdown.Token);
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
        {
            Console.WriteLine("Stopped. Cleaning sample-owned topology...");
        }
        finally
        {
            Console.CancelKeyPress -= cancel;
        }
    }

    private static async Task RunCoreAsync(
        Uri brokerUri, string exchange, string queue, string route, CancellationToken cancellationToken)
    {
        long received = 0;
        long errors = 0;
        long accepted = 0;
        long notAccepted = 0;
        long sequence = 0;
        var codec = new CompositeCodec<ContinuousMessage>(
            JsonMessageSerializer<ContinuousMessage>.Default, new GzipCompressor());
        var exchangeConfig = new ExchangeConfig
        {
            Name = exchange,
            Type = "direct",
            Durable = true,
            AutoDelete = false
        };

        var services = new ServiceCollection();
        services.AddRabbitMQComponent(options =>
        {
            options.PublishBroker = new BrokerConnectionOptions { Uri = brokerUri };
            options.SubscriptionBroker = new BrokerConnectionOptions { Uri = brokerUri };
            options.ReconnectMinDelay = TimeSpan.FromMilliseconds(250);
            options.ReconnectMaxDelay = TimeSpan.FromSeconds(5);
            options.DrainTimeout = TimeSpan.FromSeconds(10);
            options.ErrorObserver = error =>
            {
                Interlocked.Increment(ref errors);
                // Do not print exception text, message bodies or connection URIs.
                Console.Error.WriteLine(
                    $"[{DateTimeOffset.UtcNow:O}] ERROR role={error.Role} stage={error.Stage} " +
                    $"outcome={error.Outcome} resource={error.Resource} " +
                    $"exceptionType={error.Exception.GetType().Name}");
            };
        });

        await using ServiceProvider provider = services.BuildServiceProvider();
        IRabbitMQService service = provider.GetRequiredService<IRabbitMQService>();
        Console.WriteLine($"Continuous mode: exchange={exchange}, queue={queue}, routingKey={route}");
        Console.WriteLine("One message/second; Ctrl+C to stop. Publishing and consuming use separate connections.");

        IReceiver<ContinuousMessage> receiver = await service.OpenReceiverAsync(
            new SubscriptionConfig<ContinuousMessage>
            {
                Queue = new QueueConfig
                {
                    Name = queue,
                    // RabbitMQ 4.3 may reject transient non-exclusive queues.
                    // Exclusive also removes the queue when its owning connection closes.
                    Durable = false,
                    Exclusive = true,
                    AutoDelete = true,
                    // Auto-delete only applies after a consumer has existed; expire unused
                    // queues as a fallback if startup fails before BasicConsume succeeds.
                    Arguments = new Dictionary<string, object?>
                    {
                        ["x-queue-type"] = "classic",
                        ["x-expires"] = 60_000
                    }
                },
                Consumer = new ConsumerConfig { AutoAck = false },
                PrefetchCount = 32,
                Codec = codec
            },
            message =>
            {
                long count = Interlocked.Increment(ref received);
                Console.WriteLine(
                    $"[{DateTimeOffset.UtcNow:O}] RECEIVED sequence={message.Sequence} " +
                    $"received={count} latencyMs={(DateTimeOffset.UtcNow - message.SentAt).TotalMilliseconds:F0}");
            },
            cancellationToken);
        using IQueueBinding binding = await receiver.BindAsync(new BindingConfig
        {
            Exchange = exchangeConfig,
            RoutingKey = route
        }, cancellationToken);
        using IPublishEndpoint<ContinuousMessage> publisher = await service.OpenPublishEndpointAsync(
            new PublishConfig<ContinuousMessage>
            {
                Exchange = exchangeConfig,
                RoutingKey = route,
                Codec = codec,
                BufferCapacity = 1024
            }, cancellationToken);

        Console.WriteLine("Ready. Find the queue's Consumer/channel in RabbitMQ management to test recovery.");
        Console.WriteLine("Queue is transient/exclusive/auto-delete. Recovery must recreate its topology; messages may be lost.");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                var message = new ContinuousMessage(++sequence, DateTimeOffset.UtcNow);
                EnqueueResult result = publisher.TrySend(message);
                if (result == EnqueueResult.Accepted) accepted++;
                else notAccepted++;
                Console.WriteLine(
                    $"[{DateTimeOffset.UtcNow:O}] SEND sequence={sequence} result={result} " +
                    $"accepted={accepted} notAccepted={notAccepted} " +
                    $"received={Interlocked.Read(ref received)} errors={Interlocked.Read(ref errors)}");
            }
        }
        finally
        {
            Console.WriteLine(
                $"Stopping: attempted={sequence} accepted={accepted} notAccepted={notAccepted} " +
                $"received={Interlocked.Read(ref received)} errors={Interlocked.Read(ref errors)}. " +
                "Accepted is local buffering, not a delivery confirmation; shutdown will drain pending work.");
        }
    }

    private sealed record ContinuousMessage(long Sequence, DateTimeOffset SentAt);
}
