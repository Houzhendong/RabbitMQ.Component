using System.Runtime.ExceptionServices;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Component.Configuration;

namespace RabbitMQ.Component.IntegrationTests;

public sealed class BrokerConnectivityTests
{
    [BrokerFact]
    public async Task BothBrokersCanDeclareRoutePublishAndConsume()
    {
        await VerifyBrokerAsync(BrokerTestEnvironment.Primary(), "primary");
        await VerifyBrokerAsync(BrokerTestEnvironment.Secondary(), "secondary");
    }

    private static async Task VerifyBrokerAsync(BrokerConnectionOptions options, string role)
    {
        BrokerTestTopology topology = BrokerTestEnvironment.CreateTopology($"connectivity-{role}");
        using var timeout = BrokerTestEnvironment.CreateTimeout();
        IConnection? connection = null;
        Exception? operationFailure = null;
        var cleanupFailures = new List<Exception>();
        bool declarationAttempted = false;

        try
        {
            connection = await BrokerTestEnvironment.CreateFactory(options)
                .CreateConnectionAsync(timeout.Token);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
            declarationAttempted = true;
            await topology.DeclareAsync(channel, timeout.Token);

            byte[] body = Encoding.UTF8.GetBytes($"probe-{Guid.NewGuid():N}");
            await channel.BasicPublishAsync(
                topology.ExchangeName,
                topology.RoutingKey,
                body,
                timeout.Token);

            BasicGetResult? delivery = await channel.BasicGetAsync(
                topology.QueueName,
                autoAck: false,
                timeout.Token);
            Assert.NotNull(delivery);
            Assert.Equal(body, delivery.Body.ToArray());
            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, timeout.Token);

            QueueDeclareOk queue = await channel.QueueDeclarePassiveAsync(topology.QueueName, timeout.Token);
            Assert.Equal((uint)0, queue.MessageCount);
        }
        catch (Exception exception)
        {
            operationFailure = exception is Xunit.Sdk.XunitException
                ? exception
                : new InvalidOperationException(
                    $"The configured {role} broker failed the AMQP connectivity check for " +
                    $"'{topology.ExchangeName}'/'{topology.QueueName}' ({exception.GetType().Name}).");
        }

        if (connection is not null)
        {
            try { await connection.DisposeAsync(); }
            catch (Exception exception)
            {
                cleanupFailures.Add(new InvalidOperationException(
                    $"Could not dispose the {role} connectivity-check connection ({exception.GetType().Name})."));
            }
        }

        if (declarationAttempted)
        {
            using var cleanupTimeout = BrokerTestEnvironment.CreateTimeout(BrokerTestEnvironment.CleanupTimeout);
            IConnection? cleanupConnection = null;
            try
            {
                cleanupConnection = await BrokerTestEnvironment.CreateFactory(options)
                    .CreateConnectionAsync(cleanupTimeout.Token);
                await topology.CleanupAsync(cleanupConnection);
            }
            catch (Exception exception)
            {
                cleanupFailures.Add(new InvalidOperationException(
                    $"Could not clean connectivity resources '{topology.ExchangeName}'/'{topology.QueueName}' " +
                    $"on the {role} broker ({exception.GetType().Name})."));
            }
            finally
            {
                if (cleanupConnection is not null)
                {
                    try { await cleanupConnection.DisposeAsync(); }
                    catch (Exception exception)
                    {
                        cleanupFailures.Add(new InvalidOperationException(
                            $"Could not dispose the {role} cleanup connection ({exception.GetType().Name})."));
                    }
                }
            }
        }

        Exception? cleanupFailure = cleanupFailures.Count switch
        {
            0 => null,
            1 => cleanupFailures[0],
            _ => new AggregateException("Multiple connectivity-test cleanup operations failed.", cleanupFailures)
        };
        if (operationFailure is not null && cleanupFailure is not null)
            throw new AggregateException("The connectivity check and its cleanup both failed.", operationFailure, cleanupFailure);
        if (operationFailure is not null)
            ExceptionDispatchInfo.Capture(operationFailure).Throw();
        if (cleanupFailure is not null)
            ExceptionDispatchInfo.Capture(cleanupFailure).Throw();
    }
}
