using RabbitMQ.Component.Configuration;

namespace RabbitMQ.Component.Internal.Publishing;

internal interface IPublishTransportFactory
{
    ValueTask<IPublishTransportConnection> ConnectAsync(BrokerConnectionOptions options, CancellationToken cancellationToken);
}

internal interface IPublishTransportConnection : IAsyncDisposable
{
    bool IsOpen { get; }
    ValueTask<IPublishTransportChannel> CreateChannelAsync(CancellationToken cancellationToken);
}

internal interface IPublishTransportChannel : IAsyncDisposable
{
    bool IsOpen { get; }
    Task Completion { get; }
    ValueTask DeclareExchangeAsync(ExchangeSnapshot exchange, CancellationToken cancellationToken);
    ValueTask PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body, CancellationToken cancellationToken);
}

/// <summary>A broker nack or mandatory return: the publish definitely did not succeed.</summary>
internal sealed class DefinitePublishException(Exception innerException)
    : Exception("The broker rejected or returned the publication.", innerException);
