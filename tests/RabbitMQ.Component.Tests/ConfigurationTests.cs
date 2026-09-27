using System.Collections;
using RabbitMQ.Client;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Component.Internal;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Tests;

public sealed class ConfigurationTests
{
    [Fact]
    public void ArgumentsAreDeepCopiedAndExportsCannotMutateSnapshot()
    {
        byte[] bytes = [1, 2];
        var list = new List<object?> { bytes, new Dictionary<string, object?> { ["n"] = 3 } };
        var source = new Dictionary<string, object?> { ["nested"] = list };
        var snapshot = AmqpTable.Capture(source);
        var original = AmqpTable.Capture(source);
        bytes[0] = 99;
        list.Add(false);
        source.Clear();
        var export = snapshot.ToDictionary();
        ((byte[])((List<object?>)export["nested"]!)[0]!)[0] = 88;
        Assert.Equal(original, snapshot);
        Assert.Equal(original.GetHashCode(), snapshot.GetHashCode());
    }

    [Fact]
    public void StructuralEqualityIgnoresTableOrderButNotArrayOrderOrNumericType()
    {
        var a = AmqpTable.Capture(new Dictionary<string, object?> { ["a"] = 1, ["b"] = new object?[] { 2, 3 } });
        var b = AmqpTable.Capture(new Dictionary<string, object?> { ["b"] = new ArrayList { 2, 3 }, ["a"] = 1 });
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, AmqpTable.Capture(new Dictionary<string, object?> { ["a"] = 1L, ["b"] = new[] { 2, 3 } }));
        Assert.NotEqual(a, AmqpTable.Capture(new Dictionary<string, object?> { ["a"] = 1, ["b"] = new[] { 3, 2 } }));
        Assert.Equal(AmqpTable.Capture(null), AmqpTable.Capture(new Dictionary<string, object?>()));
    }

    [Fact]
    public void SupportsClientWireValueTypesAndSharedNonCyclicNodes()
    {
        var shared = new Hashtable { ["x"] = new BinaryTableValue([1, 2]) };
        object?[] values = [null, "text", true, (byte)1, (sbyte)-1, (short)-2, (ushort)2, 3, 3U,
            4L, 1.5f, 1.5d, 123.45m, new AmqpTimestamp(5), new byte[] { 6 }, shared, shared];
        var table = AmqpTable.Capture(new Dictionary<string, object?> { ["values"] = values });
        Assert.Equal(table, AmqpTable.Capture(table.ToDictionary()));
    }

    [Fact]
    public void RejectsUnsupportedValuesCyclesAndExcessiveNesting()
    {
        Assert.Throws<ArgumentException>(() => AmqpTable.Capture(new Dictionary<string, object?> { ["x"] = DateTime.UtcNow }));
        Assert.Throws<ArgumentException>(() => AmqpTable.Capture(new Dictionary<string, object?> { ["x"] = ulong.MaxValue }));
        Assert.Throws<ArgumentException>(() => AmqpTable.Capture(new Dictionary<string, object?> { ["x"] = decimal.MaxValue }));
        var cycle = new Dictionary<string, object?>();
        cycle["self"] = cycle;
        Assert.Throws<ArgumentException>(() => AmqpTable.Capture(cycle));
        var list = new ArrayList(); list.Add(list);
        Assert.Throws<ArgumentException>(() => AmqpTable.Capture(new Dictionary<string, object?> { ["list"] = list }));
        object node = 1;
        for (int i = 0; i < 70; i++) node = new object[] { node };
        Assert.Throws<ArgumentException>(() => AmqpTable.Capture(new Dictionary<string, object?> { ["deep"] = node }));
    }

    [Theory]
    [InlineData("")]
    [InlineData("nul\0name")]
    public void InvalidQueueNamesAreRejected(string name) =>
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Queue(new QueueConfig { Name = name }));

    [Fact]
    public void ShortStringsUseUtf8ByteLimitAndPluginExchangesAreAllowed()
    {
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Queue(new QueueConfig { Name = new string('界', 86) }));
        var exchange = ConfigSnapshots.Exchange(new ExchangeConfig { Name = "plugin", Type = "x-custom-plugin" });
        Assert.Equal("x-custom-plugin", exchange.Type);
        Assert.Equal("", ConfigSnapshots.Publish(new PublishConfig<int> { Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default) }).Exchange.Name);
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Binding(new BindingConfig()));
    }

    [Fact]
    public void PublishKeyIsTypeAndRoutingKeyButCompatibilityIncludesExchangeSerializerAndCapacity()
    {
        var serializer = new CompositeCodec<int>(JsonMessageSerializer<int>.Default);
        var config = new PublishConfig<int> { Codec = serializer, RoutingKey = "r", Exchange = new() { Name = "a" } };
        var original = ConfigSnapshots.Publish(config);
        Assert.True(original.IsCompatibleWith(ConfigSnapshots.Publish(config)));
        config.Exchange.Name = "b";
        var changed = ConfigSnapshots.Publish(config);
        Assert.Equal(original.Key, changed.Key);
        Assert.False(original.IsCompatibleWith(changed));
        config.Exchange.Name = "a";
        config.Codec = new CompositeCodec<int>(new JsonMessageSerializer<int>());
        Assert.False(original.IsCompatibleWith(ConfigSnapshots.Publish(config)));
        config.Codec = serializer; config.BufferCapacity++;
        Assert.False(original.IsCompatibleWith(ConfigSnapshots.Publish(config)));
        config.BufferCapacity = 0;
        Assert.Throws<ArgumentOutOfRangeException>(() => ConfigSnapshots.Publish(config));
        config.BufferCapacity = 1; config.Codec = null!;
        Assert.Throws<ArgumentNullException>(() => ConfigSnapshots.Publish(config));
    }

    [Fact]
    public void SubscriptionCompatibilityIncludesHandlerPrefetchAndConsumerSettings()
    {
        var config = new SubscriptionConfig<int> { Queue = new() { Name = "q" }, Codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default) };
        Action<int> action = _ => { };
        var snapshot = ConfigSnapshots.Subscription(config, action);
        Assert.False(snapshot.Consumer.AutoAck);
        Assert.Equal("", snapshot.Consumer.ConsumerTag);
        Assert.False(snapshot.Consumer.NoLocal);
        Assert.False(snapshot.Consumer.Exclusive);
        Assert.Equal(32, snapshot.PrefetchCount);
        Assert.True(snapshot.IsCompatibleWith(ConfigSnapshots.Subscription(config, action)));
        Assert.False(snapshot.IsCompatibleWith(ConfigSnapshots.Subscription(config, _ => { })));
        config.PrefetchCount = 0;
        Assert.False(snapshot.IsCompatibleWith(ConfigSnapshots.Subscription(config, action)));
        config.PrefetchCount = 32;
        config.Consumer.Exclusive = true;
        Assert.False(snapshot.IsCompatibleWith(ConfigSnapshots.Subscription(config, action)));
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Subscription(config, async _ => await Task.Yield()));
        config.Consumer = null!;
        Assert.Throws<ArgumentNullException>(() => ConfigSnapshots.Subscription(config, action));
    }

    [Fact]
    public void ConsumerSnapshotValidatesTagAndDeepCopiesStructuralArguments()
    {
        byte[] bytes = [1, 2];
        var config = new ConsumerConfig
        {
            AutoAck = true,
            ConsumerTag = "worker",
            NoLocal = true,
            Exclusive = true,
            Arguments = new Dictionary<string, object?> { ["x-priority"] = 4, ["nested"] = new object?[] { bytes } }
        };
        ConsumerSnapshot snapshot = ConfigSnapshots.Consumer(config);
        ConsumerSnapshot equivalent = ConfigSnapshots.Consumer(config);
        bytes[0] = 9;
        config.Arguments["x-priority"] = 5;

        Assert.Equal(snapshot, equivalent);
        Assert.Equal(4, snapshot.Arguments.ToDictionary()["x-priority"]);
        Assert.NotEqual(snapshot, ConfigSnapshots.Consumer(config));
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Consumer(new() { ConsumerTag = null! }));
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Consumer(new() { ConsumerTag = "bad\0tag" }));
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Consumer(new() { ConsumerTag = new string('界', 86) }));
    }

    [Fact]
    public void CodecCompatibilityUsesReferenceIdentityEvenWhenDependenciesMatch()
    {
        var serializer = JsonMessageSerializer<int>.Default;
        var codec = new CompositeCodec<int>(serializer);
        var publish = new PublishConfig<int> { Codec = codec };
        var subscription = new SubscriptionConfig<int> { Codec = codec, Queue = new() { Name = "q" } };
        Action<int> handler = _ => { };
        var originalPublish = ConfigSnapshots.Publish(publish);
        var originalSubscription = ConfigSnapshots.Subscription(subscription, handler);
        publish.Codec = new CompositeCodec<int>(serializer);
        subscription.Codec = publish.Codec;
        Assert.False(originalPublish.IsCompatibleWith(ConfigSnapshots.Publish(publish)));
        Assert.False(originalSubscription.IsCompatibleWith(ConfigSnapshots.Subscription(subscription, handler)));
        publish.Codec = codec;
        subscription.Codec = codec;
        Assert.True(originalPublish.IsCompatibleWith(ConfigSnapshots.Publish(publish)));
        Assert.True(originalSubscription.IsCompatibleWith(ConfigSnapshots.Subscription(subscription, handler)));
        subscription.Codec = null!;
        Assert.Throws<ArgumentNullException>(() => ConfigSnapshots.Subscription(subscription, handler));
        Assert.Same(codec, originalSubscription.Codec);
    }

    [Fact]
    public void BindingKeyIncludesQueueExchangeRoutingAndStructuralArguments()
    {
        var config = new BindingConfig { Exchange = new() { Name = "x" }, RoutingKey = "r", Arguments = new Dictionary<string, object?> { ["n"] = 1 } };
        var first = ConfigSnapshots.Binding(config);
        Assert.Equal(first.Key("q"), ConfigSnapshots.Binding(config).Key("q"));
        Assert.NotEqual(first.Key("q"), first.Key("other"));
        config.Arguments["n"] = 2;
        Assert.NotEqual(first.Key("q"), ConfigSnapshots.Binding(config).Key("q"));
        Assert.Equal(1, first.Arguments.ToDictionary()["n"]);
    }

    [Fact]
    public void FactoryDisablesClientRecoveryAndSnapshotsBothRolesIndependently()
    {
        var source = new BrokerConnectionOptions { HostName = "example.invalid", UserName = "a", Password = "secret" };
        var options = new RabbitMQServiceOptions { PublishBroker = source, SubscriptionBroker = source };
        var snapshot = ConfigSnapshots.Service(options);
        source.HostName = "changed";
        Assert.NotSame(snapshot.PublishBroker, snapshot.SubscriptionBroker);
        var factory = snapshot.PublishBroker.CreateFactory();
        Assert.Equal("example.invalid", factory.HostName);
        Assert.False(factory.AutomaticRecoveryEnabled);
        Assert.False(factory.TopologyRecoveryEnabled);
        Assert.Equal(1, factory.ConsumerDispatchConcurrency);
        Assert.DoesNotContain("secret", source.ToString());
        Assert.Equal(TimeSpan.FromMilliseconds(250), snapshot.ReconnectDelay(0));
        Assert.Equal(TimeSpan.FromSeconds(30), snapshot.ReconnectDelay(int.MaxValue));
    }

    [Fact]
    public void UriTakesPrecedenceAndInvalidOptionsAreRejectedWithoutCredentialLeak()
    {
        var options = new BrokerConnectionOptions { Uri = new Uri("amqp://user:secret@localhost:1234/vhost"), Port = -1 };
        var factory = ConfigSnapshots.Broker(options).CreateFactory();
        Assert.Equal(1234, factory.Port);
        Assert.Equal("vhost", factory.VirtualHost);
        Assert.Equal("user", factory.UserName);
        options.Uri = new Uri("https://user:secret@localhost");
        var exception = Assert.Throws<ArgumentException>(() => ConfigSnapshots.Broker(options));
        Assert.DoesNotContain("secret", exception.ToString());
        Assert.Throws<ArgumentOutOfRangeException>(() => ConfigSnapshots.Broker(new() { ConnectionTimeout = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => ConfigSnapshots.Broker(new() { Port = 0 }));
        Assert.Throws<ArgumentException>(() => ConfigSnapshots.Service(new() { ReconnectMinDelay = TimeSpan.FromSeconds(60) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => ConfigSnapshots.Service(new() { DrainTimeout = Timeout.InfiniteTimeSpan }));
    }
}
