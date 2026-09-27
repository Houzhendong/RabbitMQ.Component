using System.Buffers;
using System.Collections.Concurrent;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Diagnostics;
using RabbitMQ.Component.Internal;
using RabbitMQ.Component.Internal.Subscriptions;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Tests;

public sealed class SubscriptionTests
{
    [Fact]
    public async Task OpenSameQueueSharesReceiverAndRejectsConflict()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        var serializer = new CompositeCodec<string>(new TextSerializer());
        Action<string> handler = _ => { };
        SubscriptionSnapshot<string> snapshot = Snapshot("shared", serializer, handler);

        IReceiver<string> first = await manager.OpenAsync(snapshot, default);
        IReceiver<string> second = await manager.OpenAsync(snapshot, default);

        Assert.Same(first, second);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await manager.OpenAsync(Snapshot("shared", serializer, _ => { }), default));
    }

    [Fact]
    public async Task SameQueueRejectsEveryConsumerConfigurationConflict()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        var serializer = new CompositeCodec<string>(new TextSerializer());
        Action<string> handler = _ => { };
        var original = new ConsumerSnapshot(false, "tag", false, false,
            AmqpTable.Capture(new Dictionary<string, object?> { ["x-priority"] = 1 }));
        _ = await manager.OpenAsync(Snapshot("consumer-conflict", serializer, handler, original), default);

        ConsumerSnapshot[] conflicts =
        [
            original with { AutoAck = true },
            original with { ConsumerTag = "other" },
            original with { NoLocal = true },
            original with { Exclusive = true },
            original with { Arguments = AmqpTable.Capture(new Dictionary<string, object?> { ["x-priority"] = 2 }) }
        ];
        foreach (ConsumerSnapshot conflict in conflicts)
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await manager.OpenAsync(Snapshot("consumer-conflict", serializer, handler, conflict), default));
    }

    [Fact]
    public async Task DuplicateBindingUsesOneObjectAndLastReleaseClosesReceiver()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("orders", new TextSerializer(), _ => { }), default);
        BindingConfig config = Binding("events", "created");

        IQueueBinding first = await receiver.BindAsync(config);
        IQueueBinding second = await receiver.BindAsync(config);
        FakeChannel channel = transport.Connections.Single().Channels.Single();

        Assert.Same(first, second);
        Assert.Equal(1, channel.BindCount);
        Assert.Equal(1, channel.ConsumeCount);

        first.Dispose();
        await Task.Delay(20);
        Assert.Equal(0, channel.UnbindCount);
        Assert.Equal(0, channel.CancelCount);

        second.Dispose();
        await EventuallyAsync(() => channel.CancelCount == 1 &&
            transport.Connections[0].Channels.Sum(item => item.UnbindCount) == 1);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receiver.BindAsync(config));

        IReceiver<string> replacement = await manager.OpenAsync(Snapshot("orders", new TextSerializer(), _ => { }), default);
        Assert.NotSame(receiver, replacement);
    }

    [Fact]
    public async Task PerDeliveryActivityCheckDoesNotAllocate()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        var receiver = (IManagedReceiver)await manager.OpenAsync(Snapshot("activity", new TextSerializer(), _ => { }), default);
        await ((IReceiver<string>)receiver).BindAsync(Binding("events", "created"));
        Assert.True(receiver.IsActive);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++) Assert.True(receiver.IsActive);
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    [Fact]
    public async Task ConcurrentEquivalentBindsShareOneReferenceCountedObject()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("concurrent", new TextSerializer(), _ => { }), default);

        IQueueBinding[] bindings = await Task.WhenAll(Enumerable.Range(0, 20)
            .Select(async _ => await receiver.BindAsync(Binding("events", "same"))));
        Assert.All(bindings, binding => Assert.Same(bindings[0], binding));
        FakeChannel consumer = transport.Connections.Single().Channels.Single();
        Assert.Equal(1, consumer.BindCount);

        foreach (IQueueBinding binding in bindings) binding.Dispose();
        await EventuallyAsync(() => consumer.CancelCount == 1);
    }

    [Fact]
    public async Task ReleasedBindingDoesNotCreateTopologyOnNeverVisitedBroker()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("tombstone", new TextSerializer(), _ => { }), default);
        IQueueBinding released = await receiver.BindAsync(Binding("obsolete", "released"));
        using IQueueBinding live = await receiver.BindAsync(Binding("events", "live"));
        released.Dispose();
        await EventuallyAsync(() => transport.Connections[0].Channels.Sum(channel => channel.UnbindCount) == 1);

        await manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);
        FakeChannel candidate = transport.Connections[1].Channels.Single();

        Assert.Equal(1, candidate.BindCount);
        Assert.Equal(0, candidate.UnbindCount);
        Assert.Equal("events", Assert.Single(candidate.Exchanges).Name);
    }

    [Fact]
    public async Task ReturningToVisitedBrokerCleansOnlyItsOutstandingReleasedBindings()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("return", new TextSerializer(), _ => { }), default);
        IQueueBinding obsolete = await receiver.BindAsync(Binding("obsolete", "old"));
        using IQueueBinding live = await receiver.BindAsync(Binding("events", "live"));
        await manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);
        obsolete.Dispose();
        await EventuallyAsync(() => transport.Connections[1].Channels.Sum(channel => channel.UnbindCount) == 1);

        await manager.SwitchBrokerAsync(new BrokerConnectionOptions(), default);
        FakeConnection returned = transport.Connections[2];
        FakeChannel cleanup = Assert.Single(returned.Channels, channel => channel.ConsumeCount == 0);
        Assert.Equal(1, cleanup.UnbindCount);
        Assert.Empty(cleanup.Exchanges);
        Assert.Equal("obsolete", Assert.Single(cleanup.UnboundBindings).Exchange.Name);
        FakeChannel consumer = Assert.Single(returned.Channels, channel => channel.ConsumeCount == 1);
        Assert.Equal("events", Assert.Single(consumer.Exchanges).Name);
        Assert.Equal(1, consumer.BindCount);

        await manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);
        Assert.Equal(0, transport.Connections[3].Channels.Sum(channel => channel.UnbindCount));
    }

    [Theory]
    [InlineData("same")]
    [InlineData("different")]
    public async Task ConflictingExchangeDeclarationDoesNotReuseBindingOrInterruptConsumer(string route)
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("conflict", new TextSerializer(), _ => calls++), default);
        using IQueueBinding live = await receiver.BindAsync(Binding("events", "same"));
        BindingConfig conflicting = Binding("events", route);
        conflicting.Exchange.Type = "direct";

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await receiver.BindAsync(conflicting));
        FakeChannel channel = transport.Connections.Single().Channels.Single();
        Assert.Single(channel.Exchanges);
        await channel.DeliverAsync(19, "valid"u8.ToArray());
        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 19 }, channel.Acks);
    }

    [Fact]
    public void BrokerTopologyIdentityNormalizesUriButSeparatesVirtualHosts()
    {
        BrokerIdentity fields = BrokerIdentity.Create(new BrokerConnectionOptions { HostName = "LOCALHOST" });
        Assert.Equal(fields, BrokerIdentity.Create(new BrokerConnectionOptions { Uri = new Uri("amqp://localhost/%2f") }));
        Assert.NotEqual(fields, BrokerIdentity.Create(new BrokerConnectionOptions { VirtualHost = "other" }));
    }

    [Fact]
    public async Task CancelledInitialBindClosesThatLogicalReceiver()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("cancel-bind", new TextSerializer(), _ => { }), default);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await receiver.BindAsync(Binding("events", "route"), cancellation.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await receiver.BindAsync(Binding("events", "route")));
    }

    [Fact]
    public async Task SuccessfulHandlerAcksAndFailuresRejectWithoutRequeue()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        int handled = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("work", new TextSerializer(), message =>
        {
            if (message == "bad-handler") throw new InvalidOperationException("handler");
            handled++;
        }), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "#"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();

        await channel.DeliverAsync(1, "ok"u8.ToArray());
        await channel.DeliverAsync(2, "bad-handler"u8.ToArray());
        await channel.DeliverAsync(3, new byte[] { 0xff });

        Assert.Equal(1, handled);
        Assert.Equal(new ulong[] { 1 }, channel.Acks);
        Assert.Equal(new ulong[] { 2, 3 }, channel.Rejects);
        Assert.Contains(errors, error => error.Stage == ErrorStage.Consume && error.DeliveryTag == 2);
        Assert.Contains(errors, error => error.Stage == ErrorStage.Decoding && error.DeliveryTag == 3);
    }

    [Fact]
    public async Task AutoAckNeverAcknowledgesOrRejectsButStillReportsAllFailures()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        int handled = 0;
        ConsumerSnapshot consumer = new(true, "", false, false, AmqpTable.Empty);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("auto-ack", new TextSerializer(), message =>
        {
            if (message == "bad-handler") throw new InvalidOperationException("handler");
            handled++;
        }, consumer), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "#"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();

        await channel.DeliverAsync(1, "ok"u8.ToArray());
        await channel.DeliverAsync(2, "bad-handler"u8.ToArray());
        await channel.DeliverAsync(3, new byte[] { 0xff });

        Assert.Equal(1, handled);
        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Rejects);
        Assert.Contains(errors, error => error.Stage == ErrorStage.Consume && error.DeliveryTag == 2);
        Assert.Contains(errors, error => error.Stage == ErrorStage.Decoding && error.DeliveryTag == 3);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompressionAndLimitFailuresUseExistingDecodeSettlementBoundary(bool autoAck)
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        var codec = new CompositeCodec<string>(JsonMessageSerializer<string>.Default, new GzipCompressor(), 16);
        var handled = new List<string>();
        ConsumerSnapshot consumer = new(autoAck, "", false, false, AmqpTable.Empty);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("compressed", codec, handled.Add, consumer), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "#"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();
        byte[] good = Encode(codec, "ok");
        byte[] oversized = Encode(codec, new string('x', 17));
        await channel.DeliverAsync(1, new byte[] { 0xff, 0xff, 0xff, 0xff });
        await channel.DeliverAsync(2, oversized);
        await channel.DeliverAsync(3, good);
        Assert.Equal(new[] { "ok" }, handled);
        Assert.Equal(2, errors.Count(e => e.Stage == ErrorStage.Decoding && e.Outcome == ErrorOutcome.Failed));
        if (autoAck)
        {
            Assert.Empty(channel.Acks);
            Assert.Empty(channel.Rejects);
        }
        else
        {
            Assert.Equal(new ulong[] { 3 }, channel.Acks);
            Assert.Equal(new ulong[] { 1, 2 }, channel.Rejects);
        }
    }

    [Fact]
    public async Task ConsumerParametersPrefetchAndReturnedTagSurviveSwitchAndRecovery()
    {
        var transport = new FakeTransportFactory { ReturnedConsumerTag = "broker-returned" };
        await using var manager = CreateManager(transport);
        ConsumerSnapshot consumer = new(true, "configured", true, false,
            AmqpTable.Capture(new Dictionary<string, object?> { ["x-priority"] = 7 }));
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("parameters", new TextSerializer(), _ => { }, consumer), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel initial = transport.Connections[0].Channels.Single();

        await manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);
        FakeChannel switched = transport.Connections[1].Channels.Single();
        await switched.FailAsync();
        await EventuallyAsync(() => transport.Connections.Count == 2 &&
            transport.Connections[1].Channels.Count(channel => channel.ConsumeCount == 1) == 2);
        FakeChannel recovered = transport.Connections[1].Channels.Last(channel => channel.ConsumeCount == 1);

        foreach (FakeChannel channel in new[] { initial, switched, recovered })
        {
            Assert.Equal(consumer, channel.ConsumerOptions);
            Assert.Equal((ushort)8, channel.PrefetchCount);
        }
        Assert.Equal(new[] { "broker-returned" }, initial.CancelledConsumerTags);
        Assert.Empty(switched.CancelledConsumerTags);

        binding.Dispose();
        await EventuallyAsync(() => recovered.CancelCount == 1);
        Assert.Equal(new[] { "broker-returned" }, recovered.CancelledConsumerTags);
    }

    [Fact]
    public async Task RegularDeliveriesAndIdleCloseDoNotAllocateDrainWaiter()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("lazy-drain", new TextSerializer(), _ => calls++), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();

        for (ulong tag = 1; tag <= 1000; tag++)
            await channel.DeliverAsync(tag, "message"u8.ToArray());

        Assert.Equal(1000, calls);
        Assert.Null(GetDrainWaiter(channel));
        binding.Dispose();
        await EventuallyAsync(() => channel.CancelCount == 1);
        Assert.Null(GetDrainWaiter(channel));
    }

    [Fact]
    public async Task BlockedCloseCreatesOneSharedDrainWaiterAndLateDeliveryIsIgnored()
    {
        var transport = new FakeTransportFactory();
        var manager = CreateManager(transport, drainTimeout: TimeSpan.FromSeconds(2));
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("waiter", new TextSerializer(), _ =>
        {
            Interlocked.Increment(ref calls);
            entered.Set();
            release.Wait();
        }), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();
        Task delivery = Task.Run(() => channel.DeliverAsync(1, "blocked"u8.ToArray()));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));

        binding.Dispose();
        await EventuallyAsync(() => GetDrainWaiter(channel) is not null);
        object waiter = GetDrainWaiter(channel)!;
        await channel.DeliverAsync(2, "late"u8.ToArray());

        Assert.Same(waiter, GetDrainWaiter(channel));
        Assert.Equal(1, Volatile.Read(ref calls));
        release.Set();
        await delivery.WaitAsync(TimeSpan.FromSeconds(2));
        await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Same(waiter, GetDrainWaiter(channel));
    }

    [Fact]
    public async Task HandlerCanReleaseItsOwnBindingWithoutDeadlock()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IQueueBinding? binding = null;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("self-release", new TextSerializer(), _ =>
        {
            binding!.Dispose();
            Thread.Sleep(20); // Give detached cleanup a chance to race the callback's original-channel ACK.
        }), default);
        binding = await receiver.BindAsync(Binding("events", "one"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();

        await channel.DeliverAsync(10, "message"u8.ToArray()).WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(new ulong[] { 10 }, channel.Acks);
        await EventuallyAsync(() => channel.CancelCount == 1);
    }

    [Fact]
    public async Task FinalReceiverReleaseUsesAdministrativeChannelAndAcksOriginalDelivery()
    {
        var transport = new FakeTransportFactory { CloseChannelOnUnbind = true };
        await using var manager = CreateManager(transport);
        IQueueBinding? binding = null;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("final-release", new TextSerializer(), _ =>
        {
            binding!.Dispose();
            Thread.Sleep(20);
        }), default);
        binding = await receiver.BindAsync(Binding("missing-exchange", "route"));
        FakeChannel consumer = transport.Connections.Single().Channels.Single();

        await consumer.DeliverAsync(41, "message"u8.ToArray()).WaitAsync(TimeSpan.FromSeconds(2));
        await EventuallyAsync(() => transport.Connections[0].Channels.Count == 2 &&
            transport.Connections[0].Channels.Sum(channel => channel.UnbindCount) == 1);

        FakeChannel cleanup = Assert.Single(transport.Connections[0].Channels, channel => channel.UnbindCount == 1);
        Assert.NotSame(consumer, cleanup);
        Assert.Equal(0, consumer.UnbindCount);
        Assert.False(cleanup.IsOpen);
        Assert.Equal(new ulong[] { 41 }, consumer.Acks);
    }

    [Fact]
    public async Task DrainTimeoutReportsAndDoesNotWaitForeverForSynchronousHandler()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        var manager = CreateManager(transport, errors.Enqueue, TimeSpan.FromMilliseconds(50));
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("drain", new TextSerializer(), _ =>
        {
            entered.Set();
            release.Wait();
        }), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();
        Task delivery = Task.Run(() => channel.DeliverAsync(99, "blocked"u8.ToArray()));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(1));
        stopwatch.Stop();

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
        Assert.Contains(errors, error => error.Stage == ErrorStage.Drain && error.Outcome == ErrorOutcome.Unknown);
        release.Set();
        await delivery.WaitAsync(TimeSpan.FromSeconds(2));
        binding.Dispose();
    }

    [Fact]
    public async Task BrokerSwitchDrainsOldGenerationAndAcksOnOriginalChannel()
    {
        var transport = new FakeTransportFactory();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("switch", new TextSerializer(), _ =>
        {
            entered.Set();
            release.Wait();
        }), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel oldChannel = transport.Connections[0].Channels.Single();

        Task delivery = Task.Run(() => oldChannel.DeliverAsync(42, "old"u8.ToArray()));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));
        Task switching = manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);
        await EventuallyAsync(() => transport.Connections.Count == 2);
        Assert.False(switching.IsCompleted);

        release.Set();
        await delivery.WaitAsync(TimeSpan.FromSeconds(2));
        await switching.WaitAsync(TimeSpan.FromSeconds(2));
        FakeChannel newChannel = transport.Connections[1].Channels.Single();

        Assert.Equal(new ulong[] { 42 }, oldChannel.Acks);
        Assert.Empty(newChannel.Acks);
        Assert.Equal(1, oldChannel.CancelCount);
    }

    [Fact]
    public async Task InitialBindFailureRollsBackAndAllowsFreshReceiver()
    {
        var transport = new FakeTransportFactory { BindFailure = new InvalidOperationException("bind") };
        await using var manager = CreateManager(transport);
        IReceiver<string> failed = await manager.OpenAsync(Snapshot("rollback", new TextSerializer(), _ => { }), default);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await failed.BindAsync(Binding("events", "route")));
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await failed.BindAsync(Binding("events", "route")));

        transport.BindFailure = null;
        IReceiver<string> replacement = await manager.OpenAsync(Snapshot("rollback", new TextSerializer(), _ => { }), default);
        using IQueueBinding binding = await replacement.BindAsync(Binding("events", "route"));
        Assert.NotSame(failed, replacement);
    }

    [Fact]
    public async Task ForegroundBindOnClosedConnectionRecoversEveryActiveReceiver()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int firstCalls = 0;
        int secondCalls = 0;
        IReceiver<string> first = await manager.OpenAsync(Snapshot("recover-first", new TextSerializer(), _ => firstCalls++), default);
        IReceiver<string> second = await manager.OpenAsync(Snapshot("recover-second", new TextSerializer(), _ => secondCalls++), default);
        using IQueueBinding firstOriginal = await first.BindAsync(Binding("events", "first"));
        using IQueueBinding secondBinding = await second.BindAsync(Binding("events", "second"));
        FakeConnection failed = transport.Connections[0];
        failed.MarkClosedWithoutSignal();

        using IQueueBinding addedAfterFailure = await first.BindAsync(Binding("events", "added"));

        Assert.Equal(2, transport.Connections.Count);
        FakeConnection recovered = transport.Connections[1];
        Assert.Equal(2, recovered.Channels.Count);
        Assert.Equal(new[] { 1, 2 }, recovered.Channels.Select(channel => channel.BindCount).Order().ToArray());
        foreach (FakeChannel channel in recovered.Channels)
            await channel.DeliverAsync((ulong)(100 + recovered.Channels.IndexOf(channel)), "message"u8.ToArray());
        Assert.Equal(1, firstCalls);
        Assert.Equal(1, secondCalls);

        await failed.SignalClosedAsync();
        await Task.Delay(30);
        Assert.Equal(2, transport.Connections.Count);
    }

    [Fact]
    public async Task UnexpectedDisconnectRebuildsTopologyAndConsumer()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("recover", new TextSerializer(), _ => calls++), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));

        await transport.Connections[0].BreakAsync();
        await EventuallyAsync(() => transport.Connections.Count == 2 &&
            transport.Connections[1].Channels.Count(channel => channel.IsRegistered) == 1);
        FakeChannel recovered = transport.Connections[1].Channels.Single(channel => channel.IsRegistered);
        await recovered.DeliverAsync(88, "recovered"u8.ToArray());

        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 88 }, recovered.Acks);
        Assert.Equal(1, recovered.BindCount);
        Assert.Equal(1, recovered.ConsumeCount);
    }

    [Fact]
    public async Task CandidateConnectionShutdownAfterConsumeDoesNotCommitDeadGeneration()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("candidate-race", new TextSerializer(), _ => calls++), default);
        IQueueBinding obsolete = await receiver.BindAsync(Binding("events", "obsolete"));
        using IQueueBinding live = await receiver.BindAsync(Binding("events", "live"));
        FakeChannel oldChannel = transport.Connections[0].Channels.Single();
        transport.ConsumeHook = _ =>
        {
            transport.ConsumeHook = null;
            obsolete.Dispose();
            transport.ChannelCreatedHook = async channel =>
            {
                transport.ChannelCreatedHook = null;
                await channel.Connection.BreakAsync();
            };
            return Task.CompletedTask;
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default));
        await oldChannel.DeliverAsync(71, "old-still-usable"u8.ToArray());

        Assert.Equal(2, transport.Connections.Count);
        Assert.Equal(2, transport.Connections[1].Channels.Count);
        Assert.False(transport.Connections[1].IsOpen);
        Assert.Equal(0, oldChannel.CancelCount);
        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 71 }, oldChannel.Acks);
    }

    [Fact]
    public async Task UnexpectedConsumerChannelShutdownRecoversWithoutNewBind()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("channel-shutdown", new TextSerializer(), _ => calls++), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel failed = transport.Connections[0].Channels.Single();

        await failed.FailAsync();
        await EventuallyAsync(() => transport.Connections.Count == 1 &&
            transport.Connections[0].Channels.Count(channel => channel.ConsumeCount == 1) == 2);
        FakeChannel recovered = transport.Connections[0].Channels.Last(channel => channel.ConsumeCount == 1);
        await recovered.DeliverAsync(81, "recovered"u8.ToArray());

        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 81 }, recovered.Acks);
    }

    [Fact]
    public async Task ServerConsumerCancelRecoversWithoutNewBind()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("server-cancel", new TextSerializer(), _ => calls++), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel cancelled = transport.Connections[0].Channels.Single();
        Assert.True(cancelled.Connection.IsOpen);

        await cancelled.ServerCancelAsync();
        await EventuallyAsync(() => transport.Connections.Count == 1 &&
            transport.Connections[0].Channels.Count(channel => channel.ConsumeCount == 1) == 2);
        FakeChannel recovered = transport.Connections[0].Channels.Last(channel => channel.ConsumeCount == 1);
        await recovered.DeliverAsync(82, "recovered"u8.ToArray());

        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 82 }, recovered.Acks);
    }

    [Fact]
    public async Task BlockedAndRepeatedLocalRepairLeavesHealthySessionUntouched()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        int firstCalls = 0;
        int secondCalls = 0;
        IReceiver<string> first = await manager.OpenAsync(Snapshot("isolated-failure", new TextSerializer(), _ => firstCalls++), default);
        IReceiver<string> second = await manager.OpenAsync(Snapshot("isolated-healthy", new TextSerializer(), _ => secondCalls++), default);
        using IQueueBinding firstBinding = await first.BindAsync(Binding("events", "first"));
        using IQueueBinding secondBinding = await second.BindAsync(Binding("events", "second"));
        FakeConnection connection = transport.Connections.Single();
        FakeChannel failed = Assert.Single(connection.Channels, channel => channel.QueueName == "isolated-failure");
        FakeChannel healthy = Assert.Single(connection.Channels, channel => channel.QueueName == "isolated-healthy");
        int healthyConsumeCount = healthy.ConsumeCount;
        int healthyCancelCount = healthy.CancelCount;
        int healthyDisposeCount = healthy.DisposeCount;
        var preparationEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePreparation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int attempts = 0;
        transport.QueueDeclareHook = async (_, queue) =>
        {
            if (queue.Name != "isolated-failure") return;
            int attempt = Interlocked.Increment(ref attempts);
            if (attempt == 1)
            {
                preparationEntered.TrySetResult();
                await releasePreparation.Task;
            }
            if (attempt <= 2) throw new InvalidOperationException("local topology preparation failed");
            transport.QueueDeclareHook = null;
        };

        await failed.FailAsync();
        await preparationEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await healthy.DeliverAsync(201, "while-blocked"u8.ToArray());
        releasePreparation.TrySetResult();
        await EventuallyAsync(() => Volatile.Read(ref attempts) >= 2);
        await healthy.DeliverAsync(202, "while-retrying"u8.ToArray());
        await EventuallyAsync(() => connection.Channels.Count(channel =>
            channel.QueueName == "isolated-failure" && channel.ConsumeCount == 1) == 2);
        FakeChannel recovered = connection.Channels.Last(channel =>
            channel.QueueName == "isolated-failure" && channel.ConsumeCount == 1);
        await recovered.DeliverAsync(203, "recovered"u8.ToArray());

        Assert.Same(connection, transport.Connections.Single());
        Assert.Equal(healthyConsumeCount, healthy.ConsumeCount);
        Assert.Equal(healthyCancelCount, healthy.CancelCount);
        Assert.Equal(healthyDisposeCount, healthy.DisposeCount);
        Assert.Equal(new ulong[] { 201, 202 }, healthy.Acks);
        Assert.Equal(new ulong[] { 203 }, recovered.Acks);
        Assert.Equal(1, firstCalls);
        Assert.Equal(2, secondCalls);
        Assert.DoesNotContain(errors, error => error.Stage == ErrorStage.Connection &&
            error.Resource == "isolated-failure");
    }

    [Fact]
    public async Task FailedUninstalledLocalCandidateIsCleanedAndItsStickyCallbackIsStale()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("sticky-local-candidate", new TextSerializer(), _ => calls++), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeConnection connection = transport.Connections.Single();
        FakeChannel failed = connection.Channels.Single();
        FakeChannel? failedCandidate = null;
        transport.ConsumeHook = async channel =>
        {
            failedCandidate = channel;
            await channel.FailAsync(new InvalidOperationException("candidate failed before installation"));
        };

        await failed.FailAsync();
        await EventuallyAsync(() => connection.Channels.Count(channel =>
            channel.QueueName == "sticky-local-candidate" && channel.ConsumeCount == 1) == 3);
        FakeChannel recovered = connection.Channels.Last(channel => channel.IsRegistered);
        int channelCount = connection.Channels.Count;
        await Task.Delay(50);
        await recovered.DeliverAsync(211, "recovered"u8.ToArray());

        Assert.Single(transport.Connections);
        Assert.Equal(channelCount, connection.Channels.Count);
        Assert.Equal(1, failedCandidate!.DisposeCount);
        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 211 }, recovered.Acks);
    }

    [Fact]
    public async Task ForegroundBindOnHealthyReceiverDoesNotReplaceConnectionBecauseAnotherSessionIsBroken()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> brokenReceiver = await manager.OpenAsync(Snapshot("foreground-broken", new TextSerializer(), _ => { }), default);
        IReceiver<string> healthyReceiver = await manager.OpenAsync(Snapshot("foreground-healthy", new TextSerializer(), _ => { }), default);
        using IQueueBinding brokenBinding = await brokenReceiver.BindAsync(Binding("events", "broken"));
        using IQueueBinding healthyBinding = await healthyReceiver.BindAsync(Binding("events", "healthy"));
        FakeConnection connection = transport.Connections.Single();
        FakeChannel broken = Assert.Single(connection.Channels, channel => channel.QueueName == "foreground-broken");
        FakeChannel healthy = Assert.Single(connection.Channels, channel => channel.QueueName == "foreground-healthy");
        var preparationEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePreparation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.QueueDeclareHook = async (_, queue) =>
        {
            if (queue.Name != "foreground-broken") return;
            preparationEntered.TrySetResult();
            await releasePreparation.Task;
            transport.QueueDeclareHook = null;
            throw new InvalidOperationException("broken receiver remains local");
        };

        await broken.FailAsync();
        await preparationEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Task<IQueueBinding> addedTask = healthyReceiver.BindAsync(Binding("events", "added")).AsTask();
        Assert.False(addedTask.IsCompleted);
        releasePreparation.TrySetResult();
        using IQueueBinding added = await addedTask.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Same(connection, transport.Connections.Single());
        Assert.Equal(2, healthy.BindCount);
        Assert.Equal(1, healthy.ConsumeCount);
        Assert.Equal(0, healthy.CancelCount);
        Assert.Equal(0, healthy.DisposeCount);
    }

    [Fact]
    public async Task ForegroundBindRepairsFailedTargetWithPendingBindingIncluded()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("pending-repair", new TextSerializer(), _ => { }), default);
        using IQueueBinding original = await receiver.BindAsync(Binding("events", "original"));
        FakeConnection connection = transport.Connections.Single();
        FakeChannel failed = connection.Channels.Single();
        var preparationEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePreparation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.QueueDeclareHook = async (_, queue) =>
        {
            if (queue.Name != "pending-repair") return;
            preparationEntered.TrySetResult();
            await releasePreparation.Task;
            transport.QueueDeclareHook = null;
            throw new InvalidOperationException("first repair attempt failed");
        };

        await failed.FailAsync();
        await preparationEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Task<IQueueBinding> pendingTask = receiver.BindAsync(Binding("events", "pending")).AsTask();
        releasePreparation.TrySetResult();
        using IQueueBinding pending = await pendingTask.WaitAsync(TimeSpan.FromSeconds(2));
        FakeChannel recovered = connection.Channels.Last(channel => channel.QueueName == "pending-repair" && channel.IsRegistered);

        Assert.Single(transport.Connections);
        Assert.Equal(2, recovered.BindCount);
        Assert.Equal(1, recovered.ConsumeCount);
    }

    [Fact]
    public async Task ConcurrentSessionFaultsAreDeduplicatedAndStaleCallbacksCannotReplaceRepairs()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int firstCalls = 0;
        int secondCalls = 0;
        IReceiver<string> first = await manager.OpenAsync(Snapshot("concurrent-fault-one", new TextSerializer(), _ => firstCalls++), default);
        IReceiver<string> second = await manager.OpenAsync(Snapshot("concurrent-fault-two", new TextSerializer(), _ => secondCalls++), default);
        using IQueueBinding firstBinding = await first.BindAsync(Binding("events", "one"));
        using IQueueBinding secondBinding = await second.BindAsync(Binding("events", "two"));
        FakeConnection connection = transport.Connections.Single();
        FakeChannel failedFirst = Assert.Single(connection.Channels, channel => channel.QueueName == "concurrent-fault-one");
        FakeChannel failedSecond = Assert.Single(connection.Channels, channel => channel.QueueName == "concurrent-fault-two");

        await Task.WhenAll(failedFirst.FailAsync(), failedSecond.FailAsync());
        await EventuallyAsync(() => connection.Channels.Count(channel => channel.QueueName == "concurrent-fault-one" && channel.ConsumeCount == 1) == 2 &&
            connection.Channels.Count(channel => channel.QueueName == "concurrent-fault-two" && channel.ConsumeCount == 1) == 2);
        FakeChannel recoveredFirst = connection.Channels.Last(channel => channel.QueueName == "concurrent-fault-one" && channel.IsRegistered);
        FakeChannel recoveredSecond = connection.Channels.Last(channel => channel.QueueName == "concurrent-fault-two" && channel.IsRegistered);
        int channelCount = connection.Channels.Count;

        await Task.WhenAll(failedFirst.FailAsync(), failedFirst.FailAsync(), failedSecond.ServerCancelAsync());
        await Task.Delay(50);
        await recoveredFirst.DeliverAsync(301, "one"u8.ToArray());
        await recoveredSecond.DeliverAsync(302, "two"u8.ToArray());

        Assert.Single(transport.Connections);
        Assert.Equal(channelCount, connection.Channels.Count);
        Assert.Equal(1, firstCalls);
        Assert.Equal(1, secondCalls);
        Assert.Equal(new ulong[] { 301 }, recoveredFirst.Acks);
        Assert.Equal(new ulong[] { 302 }, recoveredSecond.Acks);
    }

    [Fact]
    public async Task LastBindingReleaseDuringCandidatePreparationCannotResurrectOldReceiver()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int oldCalls = 0;
        int replacementCalls = 0;
        IReceiver<string> oldReceiver = await manager.OpenAsync(Snapshot("replacement-race", new TextSerializer(), _ => oldCalls++), default);
        IQueueBinding oldBinding = await oldReceiver.BindAsync(Binding("events", "old"));
        FakeChannel failed = transport.Connections.Single().Channels.Single();
        var candidateEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCandidate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        FakeChannel? abandonedCandidate = null;
        Task? abandonedDelivery = null;
        transport.ConsumeHook = async channel =>
        {
            abandonedCandidate = channel;
            abandonedDelivery = channel.DeliverAsync(401, "must-not-run"u8.ToArray());
            candidateEntered.TrySetResult();
            await releaseCandidate.Task;
        };

        await failed.FailAsync();
        await candidateEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        oldBinding.Dispose();
        IReceiver<string> replacement = await manager.OpenAsync(
            Snapshot("replacement-race", new TextSerializer(), _ => replacementCalls++), default);
        Task<IQueueBinding> replacementBindingTask = replacement.BindAsync(Binding("events", "new")).AsTask();
        releaseCandidate.TrySetResult();
        await abandonedDelivery!.WaitAsync(TimeSpan.FromSeconds(2));
        using IQueueBinding replacementBinding = await replacementBindingTask.WaitAsync(TimeSpan.FromSeconds(2));
        FakeChannel replacementChannel = transport.Connections.Single().Channels.Last(channel =>
            channel.QueueName == "replacement-race" && channel.IsRegistered);
        await replacementChannel.DeliverAsync(402, "replacement"u8.ToArray());

        Assert.Equal(0, oldCalls);
        Assert.Equal(1, replacementCalls);
        Assert.Equal(1, abandonedCandidate!.DisposeCount);
        Assert.Equal(new ulong[] { 402 }, replacementChannel.Acks);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await oldReceiver.BindAsync(Binding("events", "again")));
    }

    [Fact]
    public async Task ConnectionFailureDuringLocalRepairEscalatesToWholeGenerationRecovery()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> first = await manager.OpenAsync(Snapshot("escalate-one", new TextSerializer(), _ => { }), default);
        IReceiver<string> second = await manager.OpenAsync(Snapshot("escalate-two", new TextSerializer(), _ => { }), default);
        using IQueueBinding firstBinding = await first.BindAsync(Binding("events", "one"));
        using IQueueBinding secondBinding = await second.BindAsync(Binding("events", "two"));
        FakeConnection failedConnection = transport.Connections.Single();
        FakeChannel failedSession = Assert.Single(failedConnection.Channels, channel => channel.QueueName == "escalate-one");
        var candidateEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCandidate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.ConsumeHook = async _ =>
        {
            candidateEntered.TrySetResult();
            await releaseCandidate.Task;
        };

        await failedSession.FailAsync();
        await candidateEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await failedConnection.BreakAsync();
        releaseCandidate.TrySetResult();
        await EventuallyAsync(() => transport.Connections.Count == 2 &&
            transport.Connections[1].Channels.Count(channel => channel.IsRegistered) == 2);

        Assert.False(failedConnection.IsOpen);
        Assert.Equal(new[] { "escalate-one", "escalate-two" }, transport.Connections[1].Channels
            .Where(channel => channel.IsRegistered).Select(channel => channel.QueueName).Order().ToArray());
    }

    [Fact]
    public async Task BrokerSwitchMakesQueuedOldSessionRepairStale()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("switch-stale-repair", new TextSerializer(), _ => calls++), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeConnection oldConnection = transport.Connections.Single();
        FakeChannel oldSession = oldConnection.Channels.Single();
        var candidateEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCandidate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.ConsumeHook = async _ =>
        {
            candidateEntered.TrySetResult();
            await releaseCandidate.Task;
        };

        Task switching = manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);
        await candidateEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await oldSession.FailAsync();
        releaseCandidate.TrySetResult();
        await switching.WaitAsync(TimeSpan.FromSeconds(2));
        await Task.Delay(50);
        FakeChannel switched = transport.Connections[1].Channels.Single(channel => channel.IsRegistered);
        await switched.DeliverAsync(501, "switched"u8.ToArray());

        Assert.Equal(2, transport.Connections.Count);
        Assert.Single(oldConnection.Channels);
        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 501 }, switched.Acks);
    }

    [Fact]
    public async Task DrainTimeoutClosesSessionOnceAndIgnoresLateDelivery()
    {
        var transport = new FakeTransportFactory();
        var manager = CreateManager(transport, drainTimeout: TimeSpan.FromMilliseconds(40));
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("drain-timeout-close", new TextSerializer(), _ =>
        {
            Interlocked.Increment(ref calls);
            entered.Set();
            release.Wait();
        }), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();
        Task delivery = Task.Run(() => channel.DeliverAsync(601, "blocked"u8.ToArray()));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(2)));
        bool bindingReleased = false;
        bool managerDisposed = false;

        try
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            binding.Dispose();
            bindingReleased = true;
            await EventuallyAsync(() => channel.DisposeCount == 1);
            stopwatch.Stop();

            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1));
            Assert.Equal(1, channel.DisposeCount);
            await channel.DeliverAsync(602, "late"u8.ToArray());
            Assert.Equal(1, Volatile.Read(ref calls));

            release.Set();
            await delivery.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.Equal(1, Volatile.Read(ref calls));
            Assert.Equal(1, channel.DisposeCount);

            await manager.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            managerDisposed = true;
            Assert.Equal(1, channel.DisposeCount);
        }
        finally
        {
            release.Set();
            await delivery.WaitAsync(TimeSpan.FromSeconds(2));
            if (!bindingReleased) binding.Dispose();
            if (!managerDisposed) await manager.DisposeAsync();
        }
    }

    [Fact]
    public async Task ConcurrentDecodesReceiveEachDeliveryBodyAndSettleIndependently()
    {
        const int deliveries = 12;
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        var codec = new BlockingDecodeCodec(deliveries);
        var handled = new ConcurrentBag<string>();
        IReceiver<string> receiver = await manager.OpenAsync(
            Snapshot("decode-concurrency", codec, handled.Add), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();

        Task[] callbacks = Enumerable.Range(1, deliveries)
            .Select(tag => Task.Run(() => channel.DeliverAsync((ulong)tag,
                System.Text.Encoding.UTF8.GetBytes($"body-{tag}"))))
            .ToArray();
        Assert.True(codec.AllEntered.Wait(TimeSpan.FromSeconds(2)));
        codec.Release.Set();
        await Task.WhenAll(callbacks).WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(Enumerable.Range(1, deliveries).Select(tag => $"body-{tag}").Order(), handled.Order());
        Assert.Equal(Enumerable.Range(1, deliveries).Select(tag => (ulong)tag).Order(), channel.Acks.Order());
        Assert.Empty(channel.Rejects);
    }

    [Fact]
    public async Task DecodeBodyRemainsValidUntilCallbackReturnsAfterDrainDeadline()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        var manager = CreateManager(transport, errors.Enqueue, TimeSpan.FromMilliseconds(40));
        var codec = new BlockingDecodeCodec(1);
        string? handled = null;
        IReceiver<string> receiver = await manager.OpenAsync(
            Snapshot("decode-after-drain", codec, message => handled = message), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel channel = transport.Connections.Single().Channels.Single();
        Task callback = Task.Run(() => channel.DeliverAsync(1, "body-after-timeout"u8.ToArray()));
        Assert.True(codec.AllEntered.Wait(TimeSpan.FromSeconds(2)));

        try
        {
            binding.Dispose();
            await EventuallyAsync(() => channel.DisposeCount == 1);
            codec.Release.Set();
            await callback.WaitAsync(TimeSpan.FromSeconds(2));

            Assert.Equal("body-after-timeout", handled);
            Assert.Empty(channel.Acks);
            Assert.Empty(channel.Rejects);
            Assert.Contains(errors, error => error.Stage == ErrorStage.Consume &&
                error.Outcome == ErrorOutcome.Unknown && error.DeliveryTag == 1);
        }
        finally
        {
            codec.Release.Set();
            await callback.WaitAsync(TimeSpan.FromSeconds(2));
            await manager.DisposeAsync();
        }
    }

    [Fact]
    public async Task IntentionalConsumerChannelClosureDoesNotTriggerRecovery()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("intentional-close", new TextSerializer(), _ => { }), default);
        IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel consumer = transport.Connections[0].Channels.Single();

        binding.Dispose();
        await EventuallyAsync(() => consumer.CancelCount == 1);
        await Task.Delay(50);

        Assert.Single(transport.Connections);
        Assert.Null(await consumer.Completion);
    }

    [Fact]
    public async Task FailedSwitchLeavesOldConsumerActive()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        IReceiver<string> receiver = await manager.OpenAsync(Snapshot("switch-fail", new TextSerializer(), _ => calls++), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel oldChannel = transport.Connections[0].Channels.Single();
        transport.ConnectFailure = new InvalidOperationException("unavailable");

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "bad" }, default));
        await oldChannel.DeliverAsync(7, "still-active"u8.ToArray());

        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 7 }, oldChannel.Acks);
        Assert.Equal(0, oldChannel.CancelCount);
    }

    [Fact]
    public async Task SameBrokerExplicitExclusiveSwitchFailsBeforeCandidateAndPreservesOldConsumer()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport, errors.Enqueue);
        int calls = 0;
        ConsumerSnapshot exclusive = new(false, "", false, true, AmqpTable.Empty);
        IReceiver<string> receiver = await manager.OpenAsync(
            Snapshot("exclusive-same", new TextSerializer(), _ => calls++, exclusive), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel oldChannel = transport.Connections.Single().Channels.Single();

        InvalidOperationException failure = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "LOCALHOST" }, default));
        await oldChannel.DeliverAsync(70, "still-active"u8.ToArray());

        Assert.Contains("exclusive consumer", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Single(transport.Connections);
        Assert.Equal(0, oldChannel.CancelCount);
        Assert.Equal(1, calls);
        Assert.Equal(new ulong[] { 70 }, oldChannel.Acks);
        Assert.Contains(errors, error => error.Stage == ErrorStage.BrokerSwitch && error.Outcome == ErrorOutcome.Failed);
    }

    [Fact]
    public async Task SameBrokerExplicitExclusiveSwitchSucceedsAfterFailedRecoveryFullyClosedOldGeneration()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        ConsumerSnapshot exclusive = new(false, "", false, true, AmqpTable.Empty);
        IReceiver<string> receiver = await manager.OpenAsync(
            Snapshot("exclusive-closed-old", new TextSerializer(), _ => { }, exclusive), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel oldChannel = transport.Connections.Single().Channels.Single();
        var candidateEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCandidate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        transport.ConsumeHook = async _ =>
        {
            candidateEntered.TrySetResult();
            await releaseCandidate.Task;
            throw new InvalidOperationException("candidate preparation failed");
        };

        await oldChannel.Connection.BreakAsync();
        await candidateEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Task switching = manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "LOCALHOST" }, default);
        releaseCandidate.TrySetResult();
        await switching.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(3, transport.Connections.Count);
        Assert.False(transport.Connections[0].IsOpen);
        FakeChannel replacement = Assert.Single(transport.Connections[2].Channels,
            channel => channel.QueueName == "exclusive-closed-old");
        Assert.True(replacement.IsRegistered);
        Assert.Equal(0, transport.ExclusiveConflictCount);
    }

    [Fact]
    public async Task DifferentBrokerExplicitExclusiveSwitchSucceeds()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        ConsumerSnapshot exclusive = new(false, "exclusive-tag", false, true, AmqpTable.Empty);
        IReceiver<string> receiver = await manager.OpenAsync(
            Snapshot("exclusive-other", new TextSerializer(), _ => { }, exclusive), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        FakeChannel oldChannel = transport.Connections.Single().Channels.Single();

        await manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default);

        Assert.Equal(2, transport.Connections.Count);
        Assert.Equal(1, oldChannel.CancelCount);
        Assert.Equal(exclusive, transport.Connections[1].Channels.Single().ConsumerOptions);
        Assert.Equal(0, transport.ExclusiveConflictCount);
    }

    [Fact]
    public async Task ExclusiveLocalRecoveryLeavesOtherConsumerRegistered()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        ConsumerSnapshot exclusive = new(false, "", false, true, AmqpTable.Empty);
        IReceiver<string> first = await manager.OpenAsync(
            Snapshot("exclusive-one", new TextSerializer(), _ => { }, exclusive), default);
        IReceiver<string> second = await manager.OpenAsync(
            Snapshot("exclusive-two", new TextSerializer(), _ => { }, exclusive), default);
        using IQueueBinding firstBinding = await first.BindAsync(Binding("events", "one"));
        using IQueueBinding secondBinding = await second.BindAsync(Binding("events", "two"));
        FakeConnection old = transport.Connections.Single();
        FakeChannel failed = Assert.Single(old.Channels, channel => channel.QueueName == "exclusive-one");
        FakeChannel stillActive = Assert.Single(old.Channels, channel => channel.QueueName == "exclusive-two");

        await failed.FailAsync();
        await EventuallyAsync(() => old.Channels.Count(channel => channel.ConsumeCount == 1) == 3);
        FakeChannel recovered = old.Channels.Last(channel => channel.QueueName == "exclusive-one");

        Assert.Single(transport.Connections);
        Assert.Equal(0, stillActive.CancelCount);
        Assert.True(stillActive.IsRegistered);
        Assert.True(recovered.IsRegistered);
        Assert.Equal(0, transport.ExclusiveConflictCount);
    }

    [Fact]
    public async Task ExclusiveRecoveryRetriesAfterOldCloseTimeoutWithoutCachedDisposeDeadlock()
    {
        var errors = new ConcurrentQueue<ComponentError>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transport = new FakeTransportFactory
        {
            BlockConsumerCloseQueue = "exclusive-blocked",
            ConsumerCloseRelease = release
        };
        await using var manager = CreateManager(transport, errors.Enqueue, TimeSpan.FromMilliseconds(40));
        ConsumerSnapshot exclusive = new(false, "", false, true, AmqpTable.Empty);
        IReceiver<string> failedReceiver = await manager.OpenAsync(
            Snapshot("exclusive-failed", new TextSerializer(), _ => { }, exclusive), default);
        IReceiver<string> blockedReceiver = await manager.OpenAsync(
            Snapshot("exclusive-blocked", new TextSerializer(), _ => { }, exclusive), default);
        using IQueueBinding failedBinding = await failedReceiver.BindAsync(Binding("events", "failed"));
        using IQueueBinding blockedBinding = await blockedReceiver.BindAsync(Binding("events", "blocked"));
        FakeChannel failed = Assert.Single(transport.Connections[0].Channels,
            channel => channel.QueueName == "exclusive-failed");

        await failed.Connection.BreakAsync();
        await EventuallyAsync(() => transport.ExclusiveConflictCount > 0);
        release.TrySetResult();
        await EventuallyAsync(() => transport.Connections.Count >= 3 &&
            transport.Connections[^1].Channels.Count(channel => channel.IsRegistered) == 2);

        Assert.Contains(errors, error => error.Stage == ErrorStage.Cleanup && error.Outcome == ErrorOutcome.Unknown);
        Assert.True(transport.ExclusiveConflictCount > 0);
    }

    [Fact]
    public async Task FailedAutoAckCandidateDoesNotInvokeHandlerOrSendSettlement()
    {
        var transport = new FakeTransportFactory();
        await using var manager = CreateManager(transport);
        int calls = 0;
        ConsumerSnapshot autoAck = new(true, "", false, false, AmqpTable.Empty);
        IReceiver<string> receiver = await manager.OpenAsync(
            Snapshot("auto-candidate", new TextSerializer(), _ => calls++, autoAck), default);
        using IQueueBinding binding = await receiver.BindAsync(Binding("events", "route"));
        Task? candidateDelivery = null;
        FakeChannel? candidateChannel = null;
        transport.ConsumeHook = channel =>
        {
            candidateChannel = channel;
            candidateDelivery = channel.DeliverAsync(91, "candidate"u8.ToArray());
            throw new InvalidOperationException("candidate preparation failed");
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.SwitchBrokerAsync(new BrokerConnectionOptions { HostName = "second" }, default));
        await candidateDelivery!.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(0, calls);
        Assert.Empty(candidateChannel!.Acks);
        Assert.Empty(candidateChannel.Rejects);
    }

    private static SubscriptionManager CreateManager(FakeTransportFactory transport, Action<ComponentError>? observer = null,
        TimeSpan? drainTimeout = null)
    {
        var broker = new BrokerConnectionOptions();
        var options = new ServiceSnapshot(broker, broker, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(20),
            drainTimeout ?? TimeSpan.FromMilliseconds(500), observer);
        return new SubscriptionManager(options, new ErrorSink(observer), transport);
    }

    private static SubscriptionSnapshot<string> Snapshot(string queue, ISerializer<string> serializer, Action<string> handler,
        ConsumerSnapshot? consumer = null) => Snapshot(queue, new CompositeCodec<string>(serializer), handler, consumer);

    private static SubscriptionSnapshot<string> Snapshot(string queue, ICodec<string> serializer, Action<string> handler,
        ConsumerSnapshot? consumer = null) =>
        new(new QueueSnapshot(queue, false, false, false, AmqpTable.Empty),
            consumer ?? new(false, "", false, false, AmqpTable.Empty), serializer, 8, handler);

    private static byte[] Encode<TMessage>(ICodec<TMessage> codec, TMessage message) =>
        codec.Encode(message).ToArray();

    private static BindingConfig Binding(string exchange, string routingKey) => new()
    {
        Exchange = new ExchangeConfig { Name = exchange, Type = "topic", Durable = false }, RoutingKey = routingKey
    };

    private static async Task EventuallyAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition()) await Task.Delay(10, timeout.Token);
    }

    private static object? GetDrainWaiter(FakeChannel channel)
    {
        object target = channel.DeliveryTarget ?? throw new InvalidOperationException("Consumer delivery handler is not installed.");
        return target.GetType().GetField("_idle", System.Reflection.BindingFlags.Instance |
            System.Reflection.BindingFlags.NonPublic)!.GetValue(target);
    }

    private sealed class BlockingDecodeCodec(int expected) : ICodec<string>
    {
        public CountdownEvent AllEntered { get; } = new(expected);
        public ManualResetEventSlim Release { get; } = new();

        public void Encode(string message, IBufferWriter<byte> output) => throw new NotSupportedException();
        public Span<byte> Encode(string message) => throw new NotSupportedException();

        public string Decode(ReadOnlySpan<byte> input)
        {
            AllEntered.Signal();
            Release.Wait();
            return System.Text.Encoding.UTF8.GetString(input);
        }
    }

    private sealed class TextSerializer : ISerializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> body)
        {
            if (!body.IsEmpty && body[0] == 0xff) throw new FormatException("invalid");
            return System.Text.Encoding.UTF8.GetString(body);
        }

        public void Serialize(string message, IBufferWriter<byte> writer) => throw new NotSupportedException();
    }

    private sealed class LockedList<T> : IReadOnlyList<T>
    {
        private readonly object _gate = new();
        private readonly List<T> _items = [];

        public int Count { get { lock (_gate) return _items.Count; } }
        public T this[int index] { get { lock (_gate) return _items[index]; } }

        public void Add(T item)
        {
            lock (_gate) _items.Add(item);
        }

        public int IndexOf(T item)
        {
            lock (_gate) return _items.IndexOf(item);
        }

        public IEnumerator<T> GetEnumerator()
        {
            T[] snapshot;
            lock (_gate) snapshot = _items.ToArray();
            return ((IEnumerable<T>)snapshot).GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class FakeTransportFactory : ISubscriptionTransportFactory
    {
        private readonly object _consumerGate = new();
        private readonly Dictionary<(BrokerIdentity Broker, string Queue), List<FakeChannel>> _consumers = new();
        private Func<FakeChannel, Task>? _consumeHook;
        private Func<FakeChannel, Task>? _channelCreatedHook;
        private Func<FakeChannel, QueueSnapshot, Task>? _queueDeclareHook;
        private int _nextConsumerTag;
        private int _exclusiveConflictCount;

        public LockedList<FakeConnection> Connections { get; } = new();
        public Exception? ConnectFailure { get; set; }
        public Exception? BindFailure { get; set; }
        public bool CloseChannelOnUnbind { get; set; }
        public string? ReturnedConsumerTag { get; set; }
        public string? BlockConsumerCloseQueue { get; set; }
        public TaskCompletionSource? ConsumerCloseRelease { get; set; }
        public int ExclusiveConflictCount => Volatile.Read(ref _exclusiveConflictCount);
        public Func<FakeChannel, Task>? ConsumeHook
        {
            get => Volatile.Read(ref _consumeHook);
            set => Volatile.Write(ref _consumeHook, value);
        }
        public Func<FakeChannel, Task>? ChannelCreatedHook
        {
            get => Volatile.Read(ref _channelCreatedHook);
            set => Volatile.Write(ref _channelCreatedHook, value);
        }
        public Func<FakeChannel, QueueSnapshot, Task>? QueueDeclareHook
        {
            get => Volatile.Read(ref _queueDeclareHook);
            set => Volatile.Write(ref _queueDeclareHook, value);
        }

        public Func<FakeChannel, Task>? TakeConsumeHook() => Interlocked.Exchange(ref _consumeHook, null);
        public Func<FakeChannel, Task>? TakeChannelCreatedHook() => Interlocked.Exchange(ref _channelCreatedHook, null);

        public Task<ISubscriptionConnection> ConnectAsync(BrokerConnectionOptions options, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (ConnectFailure is { } failure) throw failure;
            var connection = new FakeConnection(this, BrokerIdentity.Create(options));
            Connections.Add(connection);
            return Task.FromResult<ISubscriptionConnection>(connection);
        }

        public string Register(FakeChannel channel, string queue, ConsumerSnapshot options)
        {
            lock (_consumerGate)
            {
                var key = (channel.Connection.Broker, queue);
                if (!_consumers.TryGetValue(key, out List<FakeChannel>? consumers))
                    _consumers.Add(key, consumers = []);
                if ((options.Exclusive && consumers.Count != 0) ||
                    (!options.Exclusive && consumers.Any(item => item.ConsumerOptions?.Exclusive == true)))
                {
                    Interlocked.Increment(ref _exclusiveConflictCount);
                    throw new InvalidOperationException($"Exclusive consumer conflict for queue '{queue}'.");
                }
                consumers.Add(channel);
            }
            return ReturnedConsumerTag ?? (options.ConsumerTag.Length == 0
                ? $"consumer-{Interlocked.Increment(ref _nextConsumerTag)}" : options.ConsumerTag);
        }

        public void Unregister(FakeChannel channel)
        {
            lock (_consumerGate)
            {
                if (channel.QueueName is not { } queue) return;
                var key = (channel.Connection.Broker, queue);
                if (!_consumers.TryGetValue(key, out List<FakeChannel>? consumers)) return;
                consumers.Remove(channel);
                if (consumers.Count == 0) _consumers.Remove(key);
            }
        }

        public Task WaitForConsumerCloseAsync(FakeChannel channel, CancellationToken cancellationToken)
        {
            TaskCompletionSource? release = ConsumerCloseRelease;
            return release is not null && channel.IsRegistered && channel.QueueName == BlockConsumerCloseQueue
                ? release.Task.WaitAsync(cancellationToken) : Task.CompletedTask;
        }
    }

    private sealed class FakeConnection(FakeTransportFactory factory, BrokerIdentity broker) : ISubscriptionConnection
    {
        private int _disposed;
        private int _open = 1;
        public BrokerIdentity Broker { get; } = broker;
        public LockedList<FakeChannel> Channels { get; } = new();
        public bool IsOpen => Volatile.Read(ref _open) != 0;
        public event Func<Task>? Closed;

        public async Task<ISubscriptionChannel> CreateChannelAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!IsOpen) throw new InvalidOperationException("connection closed");
            var channel = new FakeChannel(factory, this);
            Channels.Add(channel);
            if (factory.TakeChannelCreatedHook() is { } hook) await hook(channel);
            return channel;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Interlocked.Exchange(ref _open, 0);
            foreach (FakeChannel channel in Channels) await channel.DisposeAsync();
        }

        public void MarkClosedWithoutSignal() => Interlocked.Exchange(ref _open, 0);

        public async Task SignalClosedAsync()
        {
            if (Closed is null) return;
            foreach (Func<Task> handler in Closed.GetInvocationList()) await handler();
        }

        public async Task BreakAsync()
        {
            MarkClosedWithoutSignal();
            await SignalClosedAsync();
        }
    }

    private sealed class FakeChannel(FakeTransportFactory factory, FakeConnection connection) : ISubscriptionChannel
    {
        private readonly object _disposeGate = new();
        private readonly TaskCompletionSource<Exception?> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Func<SubscriptionDelivery, CancellationToken, Task>? _handler;
        private Task? _disposeTask;
        private int _open = 1;
        private int _bindCount;
        private int _unbindCount;
        private int _consumeCount;
        private int _cancelCount;
        private int _disposeCount;
        private int _registered;
        public FakeConnection Connection { get; } = connection;
        public string? QueueName { get; private set; }
        public ConsumerSnapshot? ConsumerOptions { get; private set; }
        public ushort? PrefetchCount { get; private set; }
        public bool IsOpen => Volatile.Read(ref _open) != 0;
        public Task<Exception?> Completion => _completion.Task;
        public int BindCount => Volatile.Read(ref _bindCount);
        public int UnbindCount => Volatile.Read(ref _unbindCount);
        public int ConsumeCount => Volatile.Read(ref _consumeCount);
        public int CancelCount => Volatile.Read(ref _cancelCount);
        public int DisposeCount => Volatile.Read(ref _disposeCount);
        public bool IsRegistered => Volatile.Read(ref _registered) != 0;
        public object? DeliveryTarget => Volatile.Read(ref _handler)?.Target;
        public LockedList<ulong> Acks { get; } = new();
        public LockedList<ulong> Rejects { get; } = new();
        public LockedList<string> CancelledConsumerTags { get; } = new();
        public LockedList<ExchangeSnapshot> Exchanges { get; } = new();
        public LockedList<BindingSnapshot> UnboundBindings { get; } = new();

        public Task ExchangeDeclareAsync(ExchangeSnapshot exchange, CancellationToken cancellationToken)
        {
            Exchanges.Add(exchange);
            return Task.CompletedTask;
        }
        public async Task QueueDeclareAsync(QueueSnapshot queue, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            QueueName = queue.Name;
            if (factory.QueueDeclareHook is { } hook) await hook(this, queue);
        }
        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken)
        {
            PrefetchCount = prefetchCount;
            return Task.CompletedTask;
        }

        public Task QueueBindAsync(string queue, BindingSnapshot binding, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (factory.BindFailure is { } failure) throw failure;
            Interlocked.Increment(ref _bindCount);
            return Task.CompletedTask;
        }

        public Task QueueUnbindAsync(string queue, BindingSnapshot binding, CancellationToken cancellationToken)
        {
            UnboundBindings.Add(binding);
            Interlocked.Increment(ref _unbindCount);
            if (factory.CloseChannelOnUnbind)
            {
                Interlocked.Exchange(ref _open, 0);
                _completion.TrySetResult(new InvalidOperationException("stale binding closed administrative channel"));
            }
            return Task.CompletedTask;
        }

        public async Task<string> BasicConsumeAsync(string queue, ConsumerSnapshot options,
            Func<SubscriptionDelivery, CancellationToken, Task> handler, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _consumeCount);
            QueueName = queue;
            ConsumerOptions = options;
            string tag = factory.Register(this, queue, options);
            Interlocked.Exchange(ref _registered, 1);
            Volatile.Write(ref _handler, handler);
            if (factory.TakeConsumeHook() is { } hook) await hook(this);
            return tag;
        }

        public async Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _cancelCount);
            CancelledConsumerTags.Add(consumerTag);
            await factory.WaitForConsumerCloseAsync(this, cancellationToken);
            Unregister();
            _completion.TrySetResult(null);
        }

        public Task BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken)
        {
            if (!IsOpen) throw new InvalidOperationException("closed");
            Acks.Add(deliveryTag);
            return Task.CompletedTask;
        }

        public Task BasicRejectAsync(ulong deliveryTag, CancellationToken cancellationToken)
        {
            if (!IsOpen) throw new InvalidOperationException("closed");
            Rejects.Add(deliveryTag);
            return Task.CompletedTask;
        }

        public Task DeliverAsync(ulong tag, byte[] body) =>
            Volatile.Read(ref _handler)!(new SubscriptionDelivery(tag, body), CancellationToken.None);

        public Task FailAsync(Exception? exception = null)
        {
            Interlocked.Exchange(ref _open, 0);
            Unregister();
            _completion.TrySetResult(exception ?? new InvalidOperationException("channel failed"));
            return Task.CompletedTask;
        }

        public Task ServerCancelAsync()
        {
            Unregister();
            _completion.TrySetResult(new InvalidOperationException("consumer cancelled by server"));
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            lock (_disposeGate) return new(_disposeTask ??= DisposeCoreAsync());
        }

        private async Task DisposeCoreAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            await factory.WaitForConsumerCloseAsync(this, CancellationToken.None);
            Interlocked.Exchange(ref _open, 0);
            Unregister();
            _completion.TrySetResult(null);
        }

        private void Unregister()
        {
            if (Interlocked.Exchange(ref _registered, 0) != 0) factory.Unregister(this);
        }
    }
}
