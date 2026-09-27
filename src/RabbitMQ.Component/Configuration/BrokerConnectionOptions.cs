using RabbitMQ.Client;

namespace RabbitMQ.Component.Configuration;

/// <summary>Connection values are snapshotted by the service. Never log this object's credentials or URI.</summary>
public sealed class BrokerConnectionOptions
{
    /// <summary>Optional amqp/amqps URI. When set it takes precedence over host/port/vhost/credentials.</summary>
    public Uri? Uri { get; set; }
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string VirtualHost { get; set; } = "/";
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string? ClientProvidedName { get; set; }
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(10);

    internal ConnectionFactory CreateFactory()
    {
        var factory = new ConnectionFactory
        {
            // 组件自行重建连接，并恢复拓扑与消费者；禁用客户端自动恢复，避免两套恢复机制竞态。
            // 拓扑恢复依赖客户端自动连接恢复；组件会按当前有效绑定自行恢复，故保持禁用。
            // 客户端自动恢复也不会修复通道级协议错误，此类故障由组件按接收器粒度修复。
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false,
            ConsumerDispatchConcurrency = 1,
            RequestedHeartbeat = RequestedHeartbeat,
            RequestedConnectionTimeout = ConnectionTimeout,
            ClientProvidedName = ClientProvidedName
        };
        if (Uri is not null) factory.Uri = Uri;
        else
        {
            factory.HostName = HostName;
            factory.Port = Port;
            factory.VirtualHost = VirtualHost;
            factory.UserName = UserName;
            factory.Password = Password;
        }
        return factory;
    }

    public override string ToString() => nameof(BrokerConnectionOptions);
}
