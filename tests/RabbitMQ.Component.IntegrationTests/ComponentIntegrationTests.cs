using System.Buffers;
using System.Text.Json;
using System.Threading.Channels;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Diagnostics;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.IntegrationTests;

public sealed class ComponentIntegrationTests
{
    [BrokerFact]
    public async Task SharedResourcesUseOneInstanceAndRejectConflictingConfiguration()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("shared");
        string exchange = $"{prefix}.exchange";
        string otherExchange = $"{prefix}.other";
        string queue = $"{prefix}.queue";
        var resources = new IntegrationResources(primary);
        resources.Exchange(exchange);
        resources.Exchange(otherExchange);
        resources.Queue(queue);
        RabbitMQService? service = null;

        try
        {
            service = CreateService(primary);
            var serializer = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default);
            var publish = new PublishConfig<TestMessage>
            {
                Exchange = Exchange(exchange), RoutingKey = "route", Codec = serializer
            };
            IPublishEndpoint<TestMessage> firstPublisher = await service.OpenPublishEndpointAsync(publish);
            IPublishEndpoint<TestMessage> secondPublisher = await service.OpenPublishEndpointAsync(publish);
            Assert.Same(firstPublisher, secondPublisher);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await service.OpenPublishEndpointAsync(new PublishConfig<TestMessage>
                {
                    Exchange = Exchange(otherExchange), RoutingKey = "route", Codec = serializer
                }));

            Action<TestMessage> handler = _ => { };
            var subscription = new SubscriptionConfig<TestMessage>
            {
                Queue = Queue(queue), Codec = serializer
            };
            IReceiver<TestMessage> firstReceiver = await service.OpenReceiverAsync(subscription, handler);
            IReceiver<TestMessage> secondReceiver = await service.OpenReceiverAsync(subscription, handler);
            Assert.Same(firstReceiver, secondReceiver);
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await service.OpenReceiverAsync(subscription, _ => { }));

            var bindingConfig = new BindingConfig { Exchange = Exchange(exchange), RoutingKey = "route" };
            IQueueBinding firstBinding = await firstReceiver.BindAsync(bindingConfig);
            IQueueBinding secondBinding = await secondReceiver.BindAsync(bindingConfig);
            Assert.Same(firstBinding, secondBinding);

            firstBinding.Dispose();
            secondBinding.Dispose();
            await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                await firstReceiver.BindAsync(bindingConfig));

            firstPublisher.Dispose();
            secondPublisher.Dispose();
        }
        finally
        {
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task OneQueueReceivesDifferentExchangesAndDistinctHeaderBindingsThroughOneHandler()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("bindings");
        string directExchange = $"{prefix}.direct";
        string headersExchange = $"{prefix}.headers";
        string queue = $"{prefix}.queue";
        var resources = new IntegrationResources(primary);
        resources.Exchange(directExchange);
        resources.Exchange(headersExchange);
        resources.Queue(queue);
        RabbitMQService? service = null;
        IQueueBinding? directBinding = null;
        IQueueBinding? redBinding = null;
        IQueueBinding? blueBinding = null;

        try
        {
            var delivered = Channel.CreateUnbounded<TestMessage>();
            int handlerCalls = 0;
            service = CreateService(primary);
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(queue), Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message =>
                {
                    Interlocked.Increment(ref handlerCalls);
                    delivered.Writer.TryWrite(message);
                });

            directBinding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(directExchange), RoutingKey = "direct.route"
            });
            redBinding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(headersExchange, ExchangeType.Headers),
                Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["kind"] = "red" }
            });
            blueBinding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(headersExchange, ExchangeType.Headers),
                Arguments = new Dictionary<string, object?> { ["x-match"] = "all", ["kind"] = "blue" }
            });
            Assert.NotSame(redBinding, blueBinding);

            using var timeout = BrokerTestEnvironment.CreateTimeout();
            await using IConnection connection = await OpenAsync(primary, timeout.Token);
            await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(channel, directExchange, "direct.route", new TestMessage("direct"), null, timeout.Token);
            await PublishJsonAsync(channel, headersExchange, "", new TestMessage("red"),
                new Dictionary<string, object?> { ["kind"] = "red" }, timeout.Token);
            await PublishJsonAsync(channel, headersExchange, "", new TestMessage("blue"),
                new Dictionary<string, object?> { ["kind"] = "blue" }, timeout.Token);

            TestMessage[] messages = await ReadAsync(delivered.Reader, 3, timeout.Token);
            Assert.Equal(new[] { "blue", "direct", "red" }, messages.Select(x => x.Id).Order().ToArray());
            Assert.Equal(3, Volatile.Read(ref handlerCalls));

            blueBinding.Dispose();
            blueBinding = null;
            redBinding.Dispose();
            redBinding = null;
            directBinding.Dispose();
            directBinding = null;
            await service.DisposeAsync();
            service = null;
            QueueDeclareOk state = await channel.QueueDeclarePassiveAsync(queue, timeout.Token);
            Assert.Equal((uint)0, state.MessageCount);
        }
        finally
        {
            blueBinding?.Dispose();
            redBinding?.Dispose();
            directBinding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task DeserializationAndHandlerFailuresAreRejectedToDeadLetterQueueAndReported()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("deadletter");
        string sourceExchange = $"{prefix}.source";
        string sourceQueue = $"{prefix}.source-queue";
        string deadExchange = $"{prefix}.dead";
        string deadQueue = $"{prefix}.dead-queue";
        const string deadRoute = "dead.route";
        var resources = new IntegrationResources(primary);
        resources.Exchange(sourceExchange);
        resources.Exchange(deadExchange);
        resources.Queue(sourceQueue);
        resources.Queue(deadQueue);
        RabbitMQService? service = null;
        IQueueBinding? binding = null;

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            await using IConnection connection = await OpenAsync(primary, timeout.Token);
            await using IChannel raw = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            await raw.ExchangeDeclareAsync(deadExchange, ExchangeType.Direct, durable: false, autoDelete: false,
                cancellationToken: timeout.Token);
            await raw.QueueDeclareAsync(deadQueue, durable: true, exclusive: false, autoDelete: false,
                cancellationToken: timeout.Token);
            await raw.QueueBindAsync(deadQueue, deadExchange, deadRoute, cancellationToken: timeout.Token);
            await using IChannel deadConsumerChannel = await connection.CreateChannelAsync(cancellationToken: timeout.Token);

            TaskCompletionSource<int> deadLetters = await StartConsumeCountAsync(deadConsumerChannel, deadQueue, 2, timeout.Token);
            var errors = Channel.CreateUnbounded<ComponentError>();
            service = CreateService(primary, error => errors.Writer.TryWrite(error));
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = new QueueConfig
                    {
                        Name = sourceQueue,
                        Durable = true,
                        Arguments = new Dictionary<string, object?>
                        {
                            ["x-dead-letter-exchange"] = deadExchange,
                            ["x-dead-letter-routing-key"] = deadRoute
                        }
                    },
                    Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message =>
                {
                    if (message.Id == "handler-failure") throw new TestHandlerException();
                });
            binding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(sourceExchange), RoutingKey = "source.route"
            });

            await raw.BasicPublishAsync(sourceExchange, "source.route", "{"u8.ToArray(), timeout.Token);
            await PublishJsonAsync(raw, sourceExchange, "source.route", new TestMessage("handler-failure"), null, timeout.Token);

            Assert.Equal(2, await deadLetters.Task.WaitAsync(timeout.Token));
            ComponentError[] reported = await ReadErrorsAsync(errors.Reader, 2, timeout.Token);
            Assert.Contains(reported, error => error.Stage == ErrorStage.Decoding && error.Outcome == ErrorOutcome.Failed);
            Assert.Contains(reported, error => error.Stage == ErrorStage.Consume && error.Outcome == ErrorOutcome.Failed);
        }
        finally
        {
            binding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task AutoAckHandlerFailureIsReportedWithoutDeadLetteringAndLaterDeliveryContinues()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("autoack-failure");
        string sourceExchange = $"{prefix}.source";
        string sourceQueue = $"{prefix}.source-queue";
        string deadExchange = $"{prefix}.dead";
        string deadQueue = $"{prefix}.dead-queue";
        const string sourceRoute = "source.route";
        const string deadRoute = "dead.route";
        var resources = new IntegrationResources(primary);
        resources.Exchange(sourceExchange);
        resources.Exchange(deadExchange);
        resources.Queue(sourceQueue);
        resources.Queue(deadQueue);
        RabbitMQService? service = null;
        IQueueBinding? binding = null;

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            await using IConnection connection = await OpenAsync(primary, timeout.Token);
            await using IChannel raw = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            await raw.ExchangeDeclareAsync(deadExchange, ExchangeType.Direct, durable: false, autoDelete: false,
                cancellationToken: timeout.Token);
            await raw.QueueDeclareAsync(deadQueue, durable: true, exclusive: false, autoDelete: false,
                cancellationToken: timeout.Token);
            await raw.QueueBindAsync(deadQueue, deadExchange, deadRoute, cancellationToken: timeout.Token);

            var delivered = Channel.CreateUnbounded<TestMessage>();
            var errors = Channel.CreateUnbounded<ComponentError>();
            service = CreateService(primary, error => errors.Writer.TryWrite(error));
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = new QueueConfig
                    {
                        Name = sourceQueue,
                        Durable = true,
                        Exclusive = false,
                        AutoDelete = false,
                        Arguments = new Dictionary<string, object?>
                        {
                            ["x-dead-letter-exchange"] = deadExchange,
                            ["x-dead-letter-routing-key"] = deadRoute
                        }
                    },
                    Consumer = new ConsumerConfig { AutoAck = true },
                    Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message =>
                {
                    if (message.Id == "handler-failure") throw new TestHandlerException();
                    delivered.Writer.TryWrite(message);
                }, timeout.Token);
            binding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(sourceExchange), RoutingKey = sourceRoute
            }, timeout.Token);
            await WaitForStableQueueConsumersAsync(connection,
                new Dictionary<string, uint> { [sourceQueue] = 1 }, timeout.Token);

            await PublishJsonAsync(raw, sourceExchange, sourceRoute,
                new TestMessage("handler-failure"), null, timeout.Token);
            await PublishJsonAsync(raw, sourceExchange, sourceRoute,
                new TestMessage("after-failure"), null, timeout.Token);

            Assert.Equal("after-failure", (await delivered.Reader.ReadAsync(timeout.Token)).Id);
            ComponentError reported = await ReadErrorAsync(errors.Reader,
                error => error.Stage == ErrorStage.Consume && error.Outcome == ErrorOutcome.Failed &&
                    error.Resource == sourceQueue,
                timeout.Token);
            Assert.IsType<TestHandlerException>(reported.Exception);

            QueueDeclareOk sourceState = await raw.QueueDeclarePassiveAsync(sourceQueue, timeout.Token);
            QueueDeclareOk deadState = await raw.QueueDeclarePassiveAsync(deadQueue, timeout.Token);
            Assert.Equal((uint)0, sourceState.MessageCount);
            Assert.Equal((uint)0, deadState.MessageCount);
            Assert.False(delivered.Reader.TryRead(out _));
        }
        finally
        {
            binding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task ExclusiveConsumerOnOrdinaryQueueRejectsRawSecondConsumerAndAcceptsPriorityArgument()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("exclusive-consumer");
        string exchange = $"{prefix}.exchange";
        string queue = $"{prefix}.queue";
        var resources = new IntegrationResources(primary);
        resources.Exchange(exchange);
        resources.Queue(queue);
        RabbitMQService? service = null;
        IQueueBinding? binding = null;

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            var delivered = Channel.CreateUnbounded<TestMessage>();
            service = CreateService(primary);
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(queue),
                    Consumer = new ConsumerConfig
                    {
                        Exclusive = true,
                        Arguments = new Dictionary<string, object?> { ["x-priority"] = 7 }
                    },
                    Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message => delivered.Writer.TryWrite(message), timeout.Token);
            binding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "route"
            }, timeout.Token);

            await using IConnection connection = await OpenAsync(primary, timeout.Token);
            await WaitForStableQueueConsumersAsync(connection,
                new Dictionary<string, uint> { [queue] = 1 }, timeout.Token);
            await AssertRawConsumerRejectedAsync(connection, queue, timeout.Token);

            await using IChannel raw = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(raw, exchange, "route", new TestMessage("exclusive-still-active"), null, timeout.Token);
            Assert.Equal("exclusive-still-active", (await delivered.Reader.ReadAsync(timeout.Token)).Id);
        }
        finally
        {
            binding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task DeletingOneQueueLocallyRecoversOnlyItsExclusiveReceiverWhileOtherHandlerIsBlocked()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("exclusive-local-recovery");
        string exchange = $"{prefix}.exchange";
        string firstQueue = $"{prefix}.first-queue";
        string secondQueue = $"{prefix}.second-queue";
        var resources = new IntegrationResources(primary);
        resources.Exchange(exchange);
        resources.Queue(firstQueue);
        resources.Queue(secondQueue);
        RabbitMQService? service = null;
        IQueueBinding? firstBinding = null;
        IQueueBinding? secondBinding = null;
        using var releaseSecondHandler = new ManualResetEventSlim(false);

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            var firstDelivered = Channel.CreateUnbounded<TestMessage>();
            var secondDelivered = Channel.CreateUnbounded<TestMessage>();
            var errors = Channel.CreateUnbounded<ComponentError>();
            var observedErrors = new System.Collections.Concurrent.ConcurrentQueue<ComponentError>();
            var secondSentinelEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var secondSentinelCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int secondSentinelDeliveries = 0;
            TimeSpan drainTimeout = TimeSpan.FromSeconds(20);
            TimeSpan localRecoveryDeadline = TimeSpan.FromSeconds(5);

            service = CreateService(primary, error =>
            {
                observedErrors.Enqueue(error);
                errors.Writer.TryWrite(error);
            }, drainTimeout);
            IReceiver<TestMessage> firstReceiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(firstQueue),
                    Consumer = new ConsumerConfig { Exclusive = true },
                    Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message => firstDelivered.Writer.TryWrite(message), timeout.Token);
            IReceiver<TestMessage> secondReceiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(secondQueue),
                    Consumer = new ConsumerConfig { Exclusive = true },
                    Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message =>
                {
                    if (message.Id == "second-blocked-sentinel")
                    {
                        Interlocked.Increment(ref secondSentinelDeliveries);
                        secondSentinelEntered.TrySetResult(true);
                        releaseSecondHandler.Wait();
                        secondSentinelCompleted.TrySetResult(true);
                        return;
                    }

                    secondDelivered.Writer.TryWrite(message);
                }, timeout.Token);
            firstBinding = await firstReceiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "first"
            }, timeout.Token);
            secondBinding = await secondReceiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "second"
            }, timeout.Token);

            await using IConnection connection = await OpenAsync(primary, timeout.Token);
            await WaitForStableQueueConsumersAsync(connection, new Dictionary<string, uint>
            {
                [firstQueue] = 1,
                [secondQueue] = 1
            }, timeout.Token);
            await using IChannel raw = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(raw, exchange, "first", new TestMessage("first-before-delete"), null, timeout.Token);
            Assert.Equal("first-before-delete", (await firstDelivered.Reader.ReadAsync(timeout.Token)).Id);

            await PublishJsonAsync(raw, exchange, "second", new TestMessage("second-blocked-sentinel"), null, timeout.Token);
            await secondSentinelEntered.Task.WaitAsync(timeout.Token);
            Assert.False(secondSentinelCompleted.Task.IsCompleted);

            // Delete only A's uniquely named queue through raw AMQP. A must repair its own receiver on the
            // existing connection while B's healthy handler remains blocked. Five seconds is deliberately
            // much shorter than the 20-second drain timeout, so whole-generation recovery cannot pass.
            using (var localRecoveryTimeout = BrokerTestEnvironment.CreateTimeout(localRecoveryDeadline))
            {
                await raw.QueueDeleteAsync(firstQueue, ifUnused: false, ifEmpty: false,
                    cancellationToken: localRecoveryTimeout.Token);
                await WaitForStableQueueConsumersAsync(connection, new Dictionary<string, uint>
                {
                    [firstQueue] = 1,
                    [secondQueue] = 1
                }, localRecoveryTimeout.Token, requiredConsecutiveSamples: 3);
                await PublishJsonAsync(raw, exchange, "first", new TestMessage("first-after-local-recovery"), null,
                    localRecoveryTimeout.Token);
                Assert.Equal("first-after-local-recovery",
                    (await firstDelivered.Reader.ReadAsync(localRecoveryTimeout.Token)).Id);
                await WaitForStableQueueConsumersAsync(connection,
                    new Dictionary<string, uint> { [secondQueue] = 1 }, localRecoveryTimeout.Token);
            }

            Assert.False(secondSentinelCompleted.Task.IsCompleted);
            Assert.DoesNotContain(observedErrors,
                error => IsReceiverLifecycleError(error, secondQueue));
            _ = await ReadErrorAsync(errors.Reader,
                error => error.Resource == firstQueue && error.Stage == ErrorStage.Consume &&
                    error.Outcome == ErrorOutcome.Unknown,
                timeout.Token);

            releaseSecondHandler.Set();
            await secondSentinelCompleted.Task.WaitAsync(timeout.Token);
            await PublishJsonAsync(raw, exchange, "second", new TestMessage("second-after-local-recovery"), null,
                timeout.Token);
            Assert.Equal("second-after-local-recovery", (await secondDelivered.Reader.ReadAsync(timeout.Token)).Id);
            Assert.Equal(1, Volatile.Read(ref secondSentinelDeliveries));
            await WaitForStableQueueConsumersAsync(connection,
                new Dictionary<string, uint> { [secondQueue] = 1 }, timeout.Token);
            Assert.DoesNotContain(observedErrors,
                error => IsReceiverLifecycleError(error, secondQueue));
        }
        finally
        {
            releaseSecondHandler.Set();
            secondBinding?.Dispose();
            firstBinding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task SerializationFailureAndUnroutablePublishAreReported()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("publish-errors");
        string exchange = $"{prefix}.exchange";
        var resources = new IntegrationResources(primary);
        resources.Exchange(exchange);
        RabbitMQService? service = null;
        IPublishEndpoint<TestMessage>? badCodec = null;
        IPublishEndpoint<TestMessage>? unroutable = null;

        try
        {
            var errors = Channel.CreateUnbounded<ComponentError>();
            service = CreateService(primary, error => errors.Writer.TryWrite(error));
            badCodec = await service.OpenPublishEndpointAsync(new PublishConfig<TestMessage>
            {
                Exchange = Exchange(exchange), RoutingKey = "serialization", Codec = new CompositeCodec<TestMessage>(new ThrowingSerializer())
            });
            unroutable = await service.OpenPublishEndpointAsync(new PublishConfig<TestMessage>
            {
                Exchange = Exchange(exchange), RoutingKey = "unroutable", Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
            });

            Assert.Equal(EnqueueResult.Accepted, badCodec.TrySend(new TestMessage("bad")));
            Assert.Equal(EnqueueResult.Accepted, unroutable.TrySend(new TestMessage("returned")));

            using var timeout = BrokerTestEnvironment.CreateTimeout();
            ComponentError[] reported = await ReadErrorsAsync(errors.Reader, 2, timeout.Token);
            Assert.Contains(reported, error => error.Stage == ErrorStage.Encoding && error.Outcome == ErrorOutcome.Failed);
            Assert.Contains(reported, error => error.Stage == ErrorStage.Publish && error.Outcome == ErrorOutcome.Failed);
        }
        finally
        {
            unroutable?.Dispose();
            badCodec?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    [BrokerFact]
    public async Task SwitchRestoresConsumerConfigAndActiveTopologyLeavesPublisherOnPrimaryAndDoesNotReviveDisposedBinding()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        BrokerConnectionOptions secondary = BrokerTestEnvironment.Secondary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("switch");
        string exchange = $"{prefix}.exchange";
        string queue = $"{prefix}.queue";
        var primaryResources = new IntegrationResources(primary);
        var secondaryResources = new IntegrationResources(secondary);
        foreach (IntegrationResources resources in new[] { primaryResources, secondaryResources })
        {
            resources.Exchange(exchange);
            resources.Queue(queue);
        }
        RabbitMQService? service = null;
        IQueueBinding? active = null;
        IQueueBinding? removed = null;
        IPublishEndpoint<TestMessage>? publisher = null;

        try
        {
            var delivered = Channel.CreateUnbounded<TestMessage>();
            service = CreateService(primary);
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(queue),
                    Consumer = new ConsumerConfig
                    {
                        AutoAck = false,
                        ConsumerTag = $"{prefix}.consumer",
                        NoLocal = true,
                        Exclusive = true,
                        Arguments = new Dictionary<string, object?> { ["x-priority"] = 11 }
                    },
                    Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message => delivered.Writer.TryWrite(message));
            active = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "active"
            });
            removed = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "removed"
            });
            publisher = await service.OpenPublishEndpointAsync(new PublishConfig<TestMessage>
            {
                Exchange = Exchange(exchange), RoutingKey = "active", Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
            });

            Assert.Equal(EnqueueResult.Accepted, publisher.TrySend(new TestMessage("before-switch")));
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            Assert.Equal("before-switch", (await delivered.Reader.ReadAsync(timeout.Token)).Id);

            removed.Dispose();
            removed = null;
            await service.SwitchSubscriptionBrokerAsync(secondary, timeout.Token);

            await using IConnection secondaryConnection = await OpenAsync(secondary, timeout.Token);
            await WaitForStableQueueConsumersAsync(secondaryConnection,
                new Dictionary<string, uint> { [queue] = 1 }, timeout.Token);
            // The restored consumer tag, no-local setting, and x-priority argument were accepted;
            // Exclusive remains behaviorally observable because a raw second consumer is denied.
            await AssertRawConsumerRejectedAsync(secondaryConnection, queue, timeout.Token);
            await using IChannel secondaryChannel = await secondaryConnection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(secondaryChannel, exchange, "removed", new TestMessage("must-not-arrive"), null, timeout.Token);
            await PublishJsonAsync(secondaryChannel, exchange, "active", new TestMessage("after-switch"), null, timeout.Token);
            Assert.Equal("after-switch", (await delivered.Reader.ReadAsync(timeout.Token)).Id);

            await using IConnection primaryConnection = await OpenAsync(primary, timeout.Token);
            await WaitForStableQueueConsumersAsync(primaryConnection,
                new Dictionary<string, uint> { [queue] = 0 }, timeout.Token);
            await using IChannel primaryChannel = await primaryConnection.CreateChannelAsync(cancellationToken: timeout.Token);
            TaskCompletionSource<TestMessage> primaryDelivery = await StartConsumeOneAsync(primaryChannel, queue, timeout.Token);
            Assert.Equal(EnqueueResult.Accepted, publisher.TrySend(new TestMessage("publisher-still-primary")));
            Assert.Equal("publisher-still-primary", (await primaryDelivery.Task.WaitAsync(timeout.Token)).Id);
        }
        finally
        {
            removed?.Dispose();
            active?.Dispose();
            publisher?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await Task.WhenAll(primaryResources.CleanupAsync(), secondaryResources.CleanupAsync());
        }
    }

    [BrokerFact]
    public async Task ReturningToPrimaryRemovesBindingReleasedOnSecondaryAndKeepsItRemovedOnRevisit()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        BrokerConnectionOptions secondary = BrokerTestEnvironment.Secondary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("released-binding-roundtrip");
        string exchange = $"{prefix}.exchange";
        string queue = $"{prefix}.queue";
        var primaryResources = new IntegrationResources(primary);
        var secondaryResources = new IntegrationResources(secondary);
        foreach (IntegrationResources resources in new[] { primaryResources, secondaryResources })
        {
            resources.Exchange(exchange);
            resources.Queue(queue);
        }
        RabbitMQService? service = null;
        IQueueBinding? live = null;
        IQueueBinding? obsolete = null;

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            var delivered = Channel.CreateUnbounded<TestMessage>();
            var errors = Channel.CreateUnbounded<ComponentError>();
            int handlerCalls = 0;
            service = CreateService(primary, error => errors.Writer.TryWrite(error));
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(queue), Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message =>
                {
                    Interlocked.Increment(ref handlerCalls);
                    delivered.Writer.TryWrite(message);
                }, timeout.Token);
            live = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "live"
            }, timeout.Token);
            obsolete = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "obsolete"
            }, timeout.Token);

            await using IConnection primaryConnection = await OpenAsync(primary, timeout.Token);
            await using IChannel primaryChannel = await primaryConnection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(primaryChannel, exchange, "obsolete", new TestMessage("primary-obsolete-before-release"), null, timeout.Token);
            await PublishJsonAsync(primaryChannel, exchange, "live", new TestMessage("primary-live-before-switch"), null, timeout.Token);
            Assert.Equal(new[] { "primary-obsolete-before-release", "primary-live-before-switch" },
                (await ReadAsync(delivered.Reader, 2, timeout.Token)).Select(message => message.Id).ToArray());

            await service.SwitchSubscriptionBrokerAsync(secondary, timeout.Token);
            await using IConnection secondaryConnection = await OpenAsync(secondary, timeout.Token);
            await using IChannel secondaryChannel = await secondaryConnection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(secondaryChannel, exchange, "obsolete", new TestMessage("secondary-obsolete-before-release"), null, timeout.Token);
            await PublishJsonAsync(secondaryChannel, exchange, "live", new TestMessage("secondary-live-before-release"), null, timeout.Token);
            Assert.Equal(new[] { "secondary-obsolete-before-release", "secondary-live-before-release" },
                (await ReadAsync(delivered.Reader, 2, timeout.Token)).Select(message => message.Id).ToArray());

            // Release on secondary, leaving primary's installed binding for its scoped tombstone to remove.
            obsolete.Dispose();
            obsolete = null;
            await service.SwitchSubscriptionBrokerAsync(primary, timeout.Token);

            // The same publishing channel orders the rejected route before the marker; no timing-based absence check.
            await PublishJsonAsync(primaryChannel, exchange, "obsolete", new TestMessage("primary-must-not-arrive"), null, timeout.Token);
            await PublishJsonAsync(primaryChannel, exchange, "live", new TestMessage("primary-marker"), null, timeout.Token);
            Assert.Equal("primary-marker", (await delivered.Reader.ReadAsync(timeout.Token)).Id);
            Assert.Equal(5, Volatile.Read(ref handlerCalls));

            await service.SwitchSubscriptionBrokerAsync(secondary, timeout.Token);
            await PublishJsonAsync(secondaryChannel, exchange, "obsolete", new TestMessage("secondary-must-not-arrive"), null, timeout.Token);
            await PublishJsonAsync(secondaryChannel, exchange, "live", new TestMessage("secondary-marker"), null, timeout.Token);
            Assert.Equal("secondary-marker", (await delivered.Reader.ReadAsync(timeout.Token)).Id);
            Assert.Equal(6, Volatile.Read(ref handlerCalls));
            Assert.False(delivered.Reader.TryRead(out _));
            Assert.False(errors.Reader.TryRead(out _), "The round trip should not report subscription or cleanup failures.");
        }
        finally
        {
            obsolete?.Dispose();
            live?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await Task.WhenAll(primaryResources.CleanupAsync(), secondaryResources.CleanupAsync());
        }
    }

    [BrokerFact]
    public async Task FirstVisitToSecondaryIgnoresReleasedExchangeWithIncompatibleDeclaration()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        BrokerConnectionOptions secondary = BrokerTestEnvironment.Secondary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("released-exchange-scope");
        string liveExchange = $"{prefix}.live";
        string obsoleteExchange = $"{prefix}.obsolete";
        string queue = $"{prefix}.queue";
        var primaryResources = new IntegrationResources(primary);
        var secondaryResources = new IntegrationResources(secondary);
        foreach (IntegrationResources resources in new[] { primaryResources, secondaryResources })
        {
            resources.Exchange(liveExchange);
            resources.Exchange(obsoleteExchange);
            resources.Queue(queue);
        }
        RabbitMQService? service = null;
        IQueueBinding? live = null;
        IQueueBinding? obsolete = null;

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            await using IConnection secondaryConnection = await OpenAsync(secondary, timeout.Token);
            await using IChannel secondaryChannel = await secondaryConnection.CreateChannelAsync(cancellationToken: timeout.Token);
            // This broker has never hosted the service's obsolete binding. Its same-named exchange is unrelated.
            await secondaryChannel.ExchangeDeclareAsync(obsoleteExchange, ExchangeType.Fanout,
                durable: false, autoDelete: false, cancellationToken: timeout.Token);

            var delivered = Channel.CreateUnbounded<TestMessage>();
            var errors = Channel.CreateUnbounded<ComponentError>();
            service = CreateService(primary, error => errors.Writer.TryWrite(error));
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(queue), Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message => delivered.Writer.TryWrite(message), timeout.Token);
            live = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(liveExchange), RoutingKey = "live"
            }, timeout.Token);
            obsolete = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(obsoleteExchange), RoutingKey = "obsolete"
            }, timeout.Token);
            obsolete.Dispose();
            obsolete = null;

            await service.SwitchSubscriptionBrokerAsync(secondary, timeout.Token);
            await PublishJsonAsync(secondaryChannel, obsoleteExchange, "obsolete", new TestMessage("must-not-arrive"), null, timeout.Token);
            await PublishJsonAsync(secondaryChannel, liveExchange, "live", new TestMessage("live-marker"), null, timeout.Token);
            Assert.Equal("live-marker", (await delivered.Reader.ReadAsync(timeout.Token)).Id);
            Assert.False(delivered.Reader.TryRead(out _));

            // An equivalent declaration must still succeed: recovery must not replace the unrelated exchange.
            await secondaryChannel.ExchangeDeclareAsync(obsoleteExchange, ExchangeType.Fanout,
                durable: false, autoDelete: false, cancellationToken: timeout.Token);
            Assert.False(errors.Reader.TryRead(out _), "An unrelated obsolete exchange must not affect live topology recovery.");
        }
        finally
        {
            obsolete?.Dispose();
            live?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await Task.WhenAll(primaryResources.CleanupAsync(), secondaryResources.CleanupAsync());
        }
    }

    [BrokerFact]
    public async Task FailedSwitchKeepsOldConsumerOperational()
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        BrokerConnectionOptions secondary = BrokerTestEnvironment.Secondary();
        string prefix = BrokerTestEnvironment.UniqueResourceName("switch-failure");
        string exchange = $"{prefix}.exchange";
        string queue = $"{prefix}.queue";
        var primaryResources = new IntegrationResources(primary);
        var secondaryResources = new IntegrationResources(secondary);
        foreach (IntegrationResources resources in new[] { primaryResources, secondaryResources })
        {
            resources.Exchange(exchange);
            resources.Queue(queue);
        }
        RabbitMQService? service = null;
        IQueueBinding? binding = null;

        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            await using (IConnection secondaryConnection = await OpenAsync(secondary, timeout.Token))
            await using (IChannel secondaryChannel = await secondaryConnection.CreateChannelAsync(cancellationToken: timeout.Token))
            {
                await secondaryChannel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
                    arguments: new Dictionary<string, object?> { ["x-message-ttl"] = 1_000 },
                    cancellationToken: timeout.Token);
            }

            var delivered = Channel.CreateUnbounded<TestMessage>();
            service = CreateService(primary);
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(
                new SubscriptionConfig<TestMessage>
                {
                    Queue = Queue(queue), Codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default)
                },
                message => delivered.Writer.TryWrite(message));
            binding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "route"
            });

            Exception switchFailure = await Assert.ThrowsAnyAsync<Exception>(
                () => service.SwitchSubscriptionBrokerAsync(secondary, timeout.Token));
            Assert.IsNotType<OperationCanceledException>(switchFailure);

            await using IConnection primaryConnection = await OpenAsync(primary, timeout.Token);
            await using IChannel primaryChannel = await primaryConnection.CreateChannelAsync(cancellationToken: timeout.Token);
            await PublishJsonAsync(primaryChannel, exchange, "route", new TestMessage("old-consumer-survived"), null, timeout.Token);
            Assert.Equal("old-consumer-survived", (await delivered.Reader.ReadAsync(timeout.Token)).Id);
        }
        finally
        {
            binding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await Task.WhenAll(primaryResources.CleanupAsync(), secondaryResources.CleanupAsync());
        }
    }

    [BrokerFact]
    public Task GzipCodecRoundTripsThroughBroker() => CompressedRoundTripAsync(new GzipCompressor(), "gzip");

    [BrokerFact]
    public Task BrotliCodecRoundTripsThroughBroker() => CompressedRoundTripAsync(new BrotliCompressor(), "brotli");

    [BrokerFact]
    public Task ZstdCodecRoundTripsThroughBroker() => CompressedRoundTripAsync(new ZstdCompressor(), "zstd");

    private static async Task CompressedRoundTripAsync(ICompressor compressor, string purpose)
    {
        BrokerConnectionOptions primary = BrokerTestEnvironment.Primary();
        string prefix = BrokerTestEnvironment.UniqueResourceName(purpose);
        string exchange = $"{prefix}.exchange";
        string queue = $"{prefix}.queue";
        var resources = new IntegrationResources(primary);
        resources.Exchange(exchange);
        resources.Queue(queue);
        RabbitMQService? service = null;
        IQueueBinding? binding = null;
        IPublishEndpoint<TestMessage>? publisher = null;
        try
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout();
            var delivered = Channel.CreateUnbounded<TestMessage>();
            var errors = new System.Collections.Concurrent.ConcurrentQueue<ComponentError>();
            service = CreateService(primary, errors.Enqueue);
            var codec = new CompositeCodec<TestMessage>(JsonMessageSerializer<TestMessage>.Default, compressor);
            IReceiver<TestMessage> receiver = await service.OpenReceiverAsync(new SubscriptionConfig<TestMessage>
            {
                Queue = Queue(queue), Codec = codec
            }, message => delivered.Writer.TryWrite(message), timeout.Token);
            binding = await receiver.BindAsync(new BindingConfig
            {
                Exchange = Exchange(exchange), RoutingKey = "compressed"
            }, timeout.Token);
            publisher = await service.OpenPublishEndpointAsync(new PublishConfig<TestMessage>
            {
                Exchange = Exchange(exchange), RoutingKey = "compressed", Codec = codec
            }, timeout.Token);
            var expected = new TestMessage(new string('x', 10000));
            Assert.Equal(EnqueueResult.Accepted, publisher.TrySend(expected));
            Assert.Equal(expected, await delivered.Reader.ReadAsync(timeout.Token));
            publisher.Dispose();
            publisher = null;
            binding.Dispose();
            binding = null;
            await service.DisposeAsync();
            service = null;
            Assert.Empty(errors);
        }
        finally
        {
            publisher?.Dispose();
            binding?.Dispose();
            if (service is not null) await service.DisposeAsync();
            await resources.CleanupAsync();
        }
    }

    private static RabbitMQService CreateService(
        BrokerConnectionOptions broker,
        Action<ComponentError>? observer = null,
        TimeSpan? drainTimeout = null) =>
        new(new RabbitMQServiceOptions
        {
            PublishBroker = broker,
            SubscriptionBroker = broker,
            ReconnectMinDelay = TimeSpan.FromMilliseconds(50),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(250),
            DrainTimeout = drainTimeout ?? TimeSpan.FromSeconds(5),
            ErrorObserver = observer
        });

    private static ExchangeConfig Exchange(string name, string type = ExchangeType.Direct) =>
        new() { Name = name, Type = type, Durable = false, AutoDelete = false };

    private static QueueConfig Queue(string name) =>
        new() { Name = name, Durable = true, Exclusive = false, AutoDelete = false };

    private static async Task<IConnection> OpenAsync(
        BrokerConnectionOptions options,
        CancellationToken cancellationToken)
    {
        try
        {
            return await BrokerTestEnvironment.CreateFactory(options).CreateConnectionAsync(cancellationToken);
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                $"The configured integration-test broker connection failed ({exception.GetType().Name}).");
        }
    }

    private static async Task PublishJsonAsync(
        IChannel channel,
        string exchange,
        string routingKey,
        TestMessage message,
        IDictionary<string, object?>? headers,
        CancellationToken cancellationToken)
    {
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);
        if (headers is null)
        {
            await channel.BasicPublishAsync(exchange, routingKey, body, cancellationToken);
            return;
        }

        var properties = new BasicProperties { Headers = headers };
        await channel.BasicPublishAsync(exchange, routingKey, mandatory: false, properties, body, cancellationToken);
    }

    private static async Task WaitForStableQueueConsumersAsync(
        IConnection connection,
        IReadOnlyDictionary<string, uint> expectedConsumers,
        CancellationToken cancellationToken,
        int requiredConsecutiveSamples = 3)
    {
        if (expectedConsumers.Count == 0) throw new ArgumentException("At least one queue is required.", nameof(expectedConsumers));
        if (requiredConsecutiveSamples <= 0)
            throw new ArgumentOutOfRangeException(nameof(requiredConsecutiveSamples));

        int consecutiveSamples = 0;
        while (consecutiveSamples < requiredConsecutiveSamples)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool matches = true;
            foreach ((string queue, uint expected) in expectedConsumers)
            {
                try
                {
                    await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
                    QueueDeclareOk state = await channel.QueueDeclarePassiveAsync(queue, cancellationToken);
                    if (state.ConsumerCount != expected) matches = false;
                }
                catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
                {
                    matches = false;
                }
            }

            consecutiveSamples = matches ? consecutiveSamples + 1 : 0;
            if (consecutiveSamples < requiredConsecutiveSamples)
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        }
    }

    private static async Task AssertRawConsumerRejectedAsync(
        IConnection connection,
        string queue,
        CancellationToken cancellationToken)
    {
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        var consumer = new AsyncEventingBasicConsumer(channel);
        OperationInterruptedException exception = await Assert.ThrowsAsync<OperationInterruptedException>(async () =>
            await channel.BasicConsumeAsync(queue, autoAck: true, consumerTag: "", noLocal: false,
                exclusive: false, arguments: new Dictionary<string, object?>(), consumer, cancellationToken));
        Assert.Equal((ushort)403, exception.ShutdownReason?.ReplyCode);
    }

    private static async Task<ComponentError> ReadErrorAsync(
        ChannelReader<ComponentError> reader,
        Func<ComponentError, bool> predicate,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            ComponentError error = await reader.ReadAsync(cancellationToken);
            if (predicate(error)) return error;
        }
    }

    private static bool IsReceiverLifecycleError(ComponentError error, string queue) =>
        error.Resource == queue &&
        error.Stage is ErrorStage.Consume or ErrorStage.Drain or ErrorStage.Cleanup;

    private static async Task<TestMessage[]> ReadAsync(
        ChannelReader<TestMessage> reader,
        int count,
        CancellationToken cancellationToken)
    {
        var result = new TestMessage[count];
        for (int i = 0; i < count; i++)
            result[i] = await reader.ReadAsync(cancellationToken);
        return result;
    }

    private static async Task<ComponentError[]> ReadErrorsAsync(
        ChannelReader<ComponentError> reader,
        int count,
        CancellationToken cancellationToken)
    {
        var result = new ComponentError[count];
        for (int i = 0; i < count; i++)
            result[i] = await reader.ReadAsync(cancellationToken);
        return result;
    }

    private static async Task<TaskCompletionSource<int>> StartConsumeCountAsync(
        IChannel channel,
        string queue,
        int expected,
        CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        int count = 0;
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken);
            int current = Interlocked.Increment(ref count);
            if (current == expected) completion.TrySetResult(current);
        };
        await channel.BasicConsumeAsync(queue, autoAck: false, consumer, cancellationToken);
        return completion;
    }

    private static async Task<TaskCompletionSource<TestMessage>> StartConsumeOneAsync(
        IChannel channel,
        string queue,
        CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<TestMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, delivery) =>
        {
            TestMessage message = JsonSerializer.Deserialize<TestMessage>(delivery.Body.Span)!;
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, cancellationToken);
            completion.TrySetResult(message);
        };
        await channel.BasicConsumeAsync(queue, autoAck: false, consumer, cancellationToken);
        return completion;
    }

    public sealed record TestMessage(string Id);

    private sealed class TestHandlerException : Exception { }

    private sealed class ThrowingSerializer : ISerializer<TestMessage>
    {
        public void Serialize(TestMessage message, IBufferWriter<byte> writer) => throw new FormatException("Test serialization failure.");
        public TestMessage Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
    }

    private sealed class IntegrationResources(BrokerConnectionOptions broker)
    {
        private readonly HashSet<string> _queues = [];
        private readonly HashSet<string> _exchanges = [];

        public void Queue(string name) => _queues.Add(name);
        public void Exchange(string name) => _exchanges.Add(name);

        public async Task CleanupAsync()
        {
            using var timeout = BrokerTestEnvironment.CreateTimeout(BrokerTestEnvironment.CleanupTimeout);
            IConnection connection;
            try
            {
                connection = await OpenAsync(broker, timeout.Token);
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException(
                    $"Could not connect while cleaning test-owned resources ({exception.GetType().Name}).");
            }
            await using (connection)
            {
                var failures = new List<Exception>();
                foreach (string queue in _queues)
                    await DeleteAsync(connection, queue, isQueue: true, timeout.Token, failures);
                foreach (string exchange in _exchanges)
                    await DeleteAsync(connection, exchange, isQueue: false, timeout.Token, failures);
                if (failures.Count > 0)
                    throw new AggregateException("Failed to clean up integration-test resources.", failures);
            }
        }

        private static async Task DeleteAsync(
            IConnection connection,
            string name,
            bool isQueue,
            CancellationToken cancellationToken,
            List<Exception> failures)
        {
            try
            {
                await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
                if (isQueue)
                    await channel.QueueDeleteAsync(name, ifUnused: false, ifEmpty: false, cancellationToken: cancellationToken);
                else
                    await channel.ExchangeDeleteAsync(name, ifUnused: false, cancellationToken: cancellationToken);
            }
            catch (OperationInterruptedException exception) when (exception.ShutdownReason?.ReplyCode == 404)
            {
            }
            catch (Exception exception)
            {
                failures.Add(new InvalidOperationException(
                    $"Could not delete a test-owned {(isQueue ? "queue" : "exchange")} ({exception.GetType().Name})."));
            }
        }
    }
}
