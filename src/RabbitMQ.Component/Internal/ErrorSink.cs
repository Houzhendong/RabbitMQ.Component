using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Component.Diagnostics;

namespace RabbitMQ.Component.Internal;

internal sealed class ErrorSink(Action<ComponentError>? observer, ILogger? logger = null)
{
    private readonly ILogger _logger = logger ?? NullLogger.Instance;

    public void Report(ComponentError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        // Neither exception text (possibly URI-bearing) nor message bodies are safe default log fields.
        try
        {
            _logger.LogError("RabbitMQ component error: role={Role}, stage={Stage}, outcome={Outcome}, resource={Resource}, exceptionType={ExceptionType}",
                error.Role, error.Stage, error.Outcome, error.Resource, error.Exception.GetType().FullName);
        }
        catch { /* A faulty logging provider must not change ACK/publish semantics. */ }
        try { observer?.Invoke(error); }
        catch
        {
            try { _logger.LogWarning("RabbitMQ component error observer threw an exception."); }
            catch { }
        }
    }
}
