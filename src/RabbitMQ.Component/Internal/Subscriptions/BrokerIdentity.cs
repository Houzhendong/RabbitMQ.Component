using RabbitMQ.Component.Configuration;

namespace RabbitMQ.Component.Internal.Subscriptions;

// Topology belongs to the broker/vhost, not to the credentials or a particular connection generation.
internal readonly record struct BrokerIdentity(string HostName, int Port, string VirtualHost, bool Tls)
{
    public static BrokerIdentity Create(BrokerConnectionOptions options)
    {
        var factory = options.CreateFactory();
        return new(factory.HostName.ToLowerInvariant(), factory.Port, factory.VirtualHost, factory.Ssl.Enabled);
    }
}

internal readonly record struct ScopedBindingKey(BrokerIdentity Broker, BindingKey Binding);
