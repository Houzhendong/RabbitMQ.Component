using RabbitMQ.Client;
using RabbitMQ.Component.Configuration;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Component.Internal.Subscriptions;

internal readonly record struct SubscriptionDelivery(ulong DeliveryTag, ReadOnlyMemory<byte> Body);

internal interface ISubscriptionTransportFactory
{
    Task<ISubscriptionConnection> ConnectAsync(BrokerConnectionOptions options, CancellationToken cancellationToken);
}

internal interface ISubscriptionConnection : IAsyncDisposable
{
    bool IsOpen { get; }
    event Func<Task>? Closed;
    Task<ISubscriptionChannel> CreateChannelAsync(CancellationToken cancellationToken);
}

internal interface ISubscriptionChannel : IAsyncDisposable
{
    bool IsOpen { get; }
    // Completes once: null for requested shutdown, otherwise the channel/consumer failure.
    Task<Exception?> Completion { get; }
    Task ExchangeDeclareAsync(ExchangeSnapshot exchange, CancellationToken cancellationToken);
    Task QueueDeclareAsync(QueueSnapshot queue, CancellationToken cancellationToken);
    Task QueueBindAsync(string queue, BindingSnapshot binding, CancellationToken cancellationToken);
    Task QueueUnbindAsync(string queue, BindingSnapshot binding, CancellationToken cancellationToken);
    Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken);
    Task<string> BasicConsumeAsync(string queue, ConsumerSnapshot options,
        Func<SubscriptionDelivery, CancellationToken, Task> handler, CancellationToken cancellationToken);
    Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken);
    Task BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken);
    Task BasicRejectAsync(ulong deliveryTag, CancellationToken cancellationToken);
}

internal sealed class RabbitSubscriptionTransportFactory : ISubscriptionTransportFactory
{
    public async Task<ISubscriptionConnection> ConnectAsync(BrokerConnectionOptions options, CancellationToken cancellationToken)
    {
        var connection = await options.CreateFactory().CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new RabbitSubscriptionConnection(connection);
    }
}

internal sealed class RabbitSubscriptionConnection : ISubscriptionConnection
{
    private readonly IConnection _connection;

    public RabbitSubscriptionConnection(IConnection connection)
    {
        _connection = connection;
        _connection.ConnectionShutdownAsync += OnShutdownAsync;
    }

    public bool IsOpen => _connection.IsOpen;
    public event Func<Task>? Closed;

    public async Task<ISubscriptionChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        var channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        return new RabbitSubscriptionChannel(channel);
    }

    private async Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        Func<Task>? handlers = Closed;
        if (handlers is null) return;
        foreach (Func<Task> handler in handlers.GetInvocationList())
        {
            try { await handler().ConfigureAwait(false); }
            catch { }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _connection.ConnectionShutdownAsync -= OnShutdownAsync;
        try
        {
            if (_connection.IsOpen) await _connection.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch { }
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}

internal sealed class RabbitSubscriptionChannel : ISubscriptionChannel
{
    private readonly IChannel channel;
    private readonly TaskCompletionSource<Exception?> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private AsyncEventingBasicConsumer? _consumer;
    private int _stopping;

    public RabbitSubscriptionChannel(IChannel channel)
    {
        this.channel = channel;
        channel.ChannelShutdownAsync += OnShutdownAsync;
        // A shutdown may have happened before the adapter was constructed.
        if (!channel.IsOpen)
            _completion.TrySetResult(new InvalidOperationException("Subscription channel was already closed."));
    }

    public bool IsOpen => channel.IsOpen;
    public Task<Exception?> Completion => _completion.Task;

    private Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        _completion.TrySetResult(Volatile.Read(ref _stopping) != 0 ? null :
            new RabbitMQ.Client.Exceptions.OperationInterruptedException(args));
        return Task.CompletedTask;
    }

    private Task OnConsumerCancelledAsync(object sender, ConsumerEventArgs args)
    {
        _completion.TrySetResult(Volatile.Read(ref _stopping) != 0 ? null :
            new InvalidOperationException("The broker cancelled the subscription consumer."));
        return Task.CompletedTask;
    }

    public Task ExchangeDeclareAsync(ExchangeSnapshot exchange, CancellationToken cancellationToken) =>
        channel.ExchangeDeclareAsync(exchange.Name, exchange.Type, exchange.Durable, exchange.AutoDelete,
            exchange.Arguments.ToDictionary(), passive: false, noWait: false, cancellationToken);

    public async Task QueueDeclareAsync(QueueSnapshot queue, CancellationToken cancellationToken) =>
        _ = await channel.QueueDeclareAsync(queue.Name, queue.Durable, queue.Exclusive, queue.AutoDelete,
            queue.Arguments.ToDictionary(), passive: false, noWait: false, cancellationToken).ConfigureAwait(false);

    public Task QueueBindAsync(string queue, BindingSnapshot binding, CancellationToken cancellationToken) =>
        channel.QueueBindAsync(queue, binding.Exchange.Name, binding.RoutingKey, binding.Arguments.ToDictionary(),
            noWait: false, cancellationToken);

    public Task QueueUnbindAsync(string queue, BindingSnapshot binding, CancellationToken cancellationToken) =>
        channel.QueueUnbindAsync(queue, binding.Exchange.Name, binding.RoutingKey, binding.Arguments.ToDictionary(),
            cancellationToken);

    public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken) =>
        channel.BasicQosAsync(0, prefetchCount, global: false, cancellationToken);

    public async Task<string> BasicConsumeAsync(string queue, ConsumerSnapshot options,
        Func<SubscriptionDelivery, CancellationToken, Task> handler, CancellationToken cancellationToken)
    {
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, args) => handler(new(args.DeliveryTag, args.Body), args.CancellationToken);
        consumer.UnregisteredAsync += OnConsumerCancelledAsync;
        _consumer = consumer;
        string tag = await channel.BasicConsumeAsync(queue, options.AutoAck, options.ConsumerTag, options.NoLocal,
            options.Exclusive, options.Arguments.ToDictionary(), consumer, cancellationToken).ConfigureAwait(false);
        return tag;
    }

    public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref _stopping, 1);
        return channel.BasicCancelAsync(consumerTag, noWait: false, cancellationToken);
    }

    public Task BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken) =>
        channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken).AsTask();

    public Task BasicRejectAsync(ulong deliveryTag, CancellationToken cancellationToken) =>
        channel.BasicRejectAsync(deliveryTag, requeue: false, cancellationToken).AsTask();

    public async ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _stopping, 1);
        _completion.TrySetResult(null);
        channel.ChannelShutdownAsync -= OnShutdownAsync;
        if (_consumer is not null) _consumer.UnregisteredAsync -= OnConsumerCancelledAsync;
        _consumer = null;
        try
        {
            if (channel.IsOpen) await channel.CloseAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch { }
        await channel.DisposeAsync().ConfigureAwait(false);
    }
}
