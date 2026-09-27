using RabbitMQ.Component.Diagnostics;

namespace RabbitMQ.Component.Internal.Publishing;

internal sealed class PublishConnection : IAsyncDisposable
{
    private readonly ServiceSnapshot _options;
    private readonly ErrorSink _errors;
    private readonly IPublishTransportFactory _factory;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IPublishTransportConnection? _connection;
    private int _disposed;

    public PublishConnection(ServiceSnapshot options, ErrorSink errors, IPublishTransportFactory factory)
    {
        _options = options;
        _errors = errors;
        _factory = factory;
    }

    public TimeSpan ReconnectDelay(int attempt) => _options.ReconnectDelay(attempt);

    public async ValueTask<IPublishTransportConnection> GetAsync(CancellationToken cancellationToken)
    {
        int attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (_connection is { IsOpen: true }) return _connection;

                if (_connection is not null)
                {
                    await DisposeConnectionAsync(_connection).ConfigureAwait(false);
                    _connection = null;
                }

                try
                {
                    _connection = await _factory.ConnectAsync(_options.PublishBroker, cancellationToken).ConfigureAwait(false);
                    if (!_connection.IsOpen) throw new InvalidOperationException("The publishing connection was not open after creation.");
                    return _connection;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    _errors.Report(new ComponentError
                    {
                        Stage = ErrorStage.Connection,
                        Outcome = ErrorOutcome.Failed,
                        Role = BrokerRole.Publish,
                        Resource = "publisher-connection",
                        Exception = exception
                    });
                }
            }
            finally
            {
                _gate.Release();
            }

            await Task.Delay(_options.ReconnectDelay(attempt++), cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_connection is not null)
            {
                await DisposeConnectionAsync(_connection).ConfigureAwait(false);
                _connection = null;
            }
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    private async ValueTask DisposeConnectionAsync(IPublishTransportConnection connection)
    {
        try { await connection.DisposeAsync().ConfigureAwait(false); }
        catch (Exception exception)
        {
            _errors.Report(new ComponentError
            {
                Stage = ErrorStage.Cleanup,
                Outcome = ErrorOutcome.Failed,
                Role = BrokerRole.Publish,
                Resource = "publisher-connection",
                Exception = exception
            });
        }
    }
}
