using System.Runtime.CompilerServices;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Component.Configuration;

namespace RabbitMQ.Component.Internal.Publishing;

internal sealed class RabbitMqPublishTransportFactory : IPublishTransportFactory
{
    public async ValueTask<IPublishTransportConnection> ConnectAsync(
        BrokerConnectionOptions options, CancellationToken cancellationToken)
    {
        IConnection connection = await options.CreateFactory().CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new RabbitMqPublishTransportConnection(connection);
    }
}

internal sealed class RabbitMqPublishTransportConnection(IConnection connection) : IPublishTransportConnection
{
    private readonly IConnection _connection = connection;

    public bool IsOpen => _connection.IsOpen;

    public async ValueTask<IPublishTransportChannel> CreateChannelAsync(CancellationToken cancellationToken)
    {
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        IChannel channel = await _connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        return new RabbitMqPublishTransportChannel(channel);
    }

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}

internal sealed class RabbitMqPublishTransportChannel : IPublishTransportChannel
{
    private readonly IChannel _channel;
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public RabbitMqPublishTransportChannel(IChannel channel)
    {
        _channel = channel;
        _channel.ChannelShutdownAsync += OnShutdownAsync;
        if (!channel.IsOpen) _completion.TrySetResult();
    }

    public bool IsOpen => _channel.IsOpen;
    public Task Completion => _completion.Task;

    public async ValueTask DeclareExchangeAsync(ExchangeSnapshot exchange, CancellationToken cancellationToken)
    {
        if (exchange.Name.Length == 0) return;
        await _channel.ExchangeDeclareAsync(exchange.Name, exchange.Type, exchange.Durable, exchange.AutoDelete,
            exchange.Arguments.ToDictionary(), passive: false, noWait: false, cancellationToken).ConfigureAwait(false);
    }

    // Awaiting publisher confirms suspends once per message; pool the state machine box.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        try
        {
            await _channel.BasicPublishAsync(exchange, routingKey, mandatory: true, new BasicProperties(), body,
                cancellationToken).ConfigureAwait(false);
        }
        catch (PublishException exception)
        {
            throw new DefinitePublishException(exception);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _channel.ChannelShutdownAsync -= OnShutdownAsync;
        _completion.TrySetResult();
        await _channel.DisposeAsync().ConfigureAwait(false);
    }

    private Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        _completion.TrySetResult();
        return Task.CompletedTask;
    }
}
