using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Diagnostics;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.IntegrationTests;

public sealed class DependencyInjectionTests
{
    [Fact]
    public async Task ServiceDisposalIsIdempotentAndPreventsNewOperations()
    {
        var service = new RabbitMQService(new RabbitMQServiceOptions
        {
            DrainTimeout = TimeSpan.FromSeconds(1)
        });

        Task first = service.DisposeAsync().AsTask();
        Task second = service.DisposeAsync().AsTask();
        await Task.WhenAll(first, second);
        await service.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await service.OpenReceiverAsync(
                new SubscriptionConfig<int>
                {
                    Queue = new QueueConfig { Name = "not-created" },
                    Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default)
                },
                _ => { }));
    }

    [Fact]
    public async Task DisposeCancelsPublishInitializationWithoutBlockingOtherOperations()
    {
        var connectionFailure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TimeSpan drainTimeout = TimeSpan.FromMilliseconds(250);
        var service = new RabbitMQService(new RabbitMQServiceOptions
        {
            PublishBroker = new BrokerConnectionOptions
            {
                HostName = "127.0.0.1",
                Port = 1,
                ConnectionTimeout = TimeSpan.FromMilliseconds(100),
                RequestedHeartbeat = TimeSpan.Zero
            },
            ReconnectMinDelay = TimeSpan.FromMilliseconds(20),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(20),
            DrainTimeout = drainTimeout,
            ErrorObserver = error =>
            {
                if (error.Role == BrokerRole.Publish && error.Stage == ErrorStage.Connection)
                    connectionFailure.TrySetResult();
            }
        });

        Task<IPublishEndpoint<int>> pendingOpen = service.OpenPublishEndpointAsync(new PublishConfig<int>
        {
            Exchange = new ExchangeConfig { Name = "never-declared", Durable = false },
            RoutingKey = "route",
            Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default)
        }).AsTask();

        await connectionFailure.Task.WaitAsync(TimeSpan.FromSeconds(5));
        IReceiver<int> receiver = await service.OpenReceiverAsync(
            new SubscriptionConfig<int>
            {
                Queue = new QueueConfig { Name = "not-declared", Durable = true },
                Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default)
            },
            _ => { }).AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        Assert.NotNull(receiver);

        var elapsed = Stopwatch.StartNew();
        await service.DisposeAsync().AsTask().WaitAsync(drainTimeout + TimeSpan.FromSeconds(1));
        elapsed.Stop();
        Assert.True(elapsed.Elapsed < drainTimeout + TimeSpan.FromSeconds(1));

        Exception openFailure = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await pendingOpen.WaitAsync(TimeSpan.FromSeconds(1)));
        Assert.True(openFailure is OperationCanceledException or ObjectDisposedException,
            $"Unexpected in-flight Open failure: {openFailure.GetType().Name}");

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await service.OpenReceiverAsync(
                new SubscriptionConfig<int>
                {
                    Queue = new QueueConfig { Name = "still-not-declared", Durable = true },
                    Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default)
                },
                _ => { }));
    }

    [Fact]
    public async Task RegistrationIsSingletonAndProviderDisposesServiceAsynchronously()
    {
        var services = new ServiceCollection();
        int configureCalls = 0;
        services.AddRabbitMQComponent(options =>
        {
            configureCalls++;
            options.DrainTimeout = TimeSpan.FromSeconds(1);
        });

        ServiceProvider provider = services.BuildServiceProvider();
        IRabbitMQService first = provider.GetRequiredService<IRabbitMQService>();
        IRabbitMQService second = provider.GetRequiredService<IRabbitMQService>();
        Assert.Same(first, second);
        Assert.Equal(1, configureCalls);

        await provider.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await first.OpenPublishEndpointAsync(new PublishConfig<int>
            {
                Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default)
            }));
    }
}
