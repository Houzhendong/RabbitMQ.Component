using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using RabbitMQ.Component;
using RabbitMQ.Component.Configuration;

namespace Microsoft.Extensions.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddRabbitMQComponent(
        this IServiceCollection services,
        RabbitMQServiceOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.TryAddSingleton<IRabbitMQService>(provider =>
            new RabbitMQService(options, provider.GetService<ILogger<RabbitMQService>>()));
        return services;
    }

    public static IServiceCollection AddRabbitMQComponent(
        this IServiceCollection services,
        Action<RabbitMQServiceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new RabbitMQServiceOptions();
        configure(options);
        return services.AddRabbitMQComponent(options);
    }
}
