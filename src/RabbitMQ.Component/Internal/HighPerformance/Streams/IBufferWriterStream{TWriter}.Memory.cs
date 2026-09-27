// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

namespace RabbitMQ.Component.Internal.HighPerformance.Streams;

partial class IBufferWriterStream<TWriter>
{
    public override void CopyTo(Stream destination, int bufferSize) => throw new NotSupportedException();

    public override ValueTask<int> ReadAsync(Memory<byte> buffer,
        CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled(cancellationToken);

        try
        {
            Write(buffer.Span);
            return ValueTask.CompletedTask;
        }
        catch (OperationCanceledException e)
        {
            return ValueTask.FromCanceled(e.CancellationToken);
        }
        catch (Exception e)
        {
            return ValueTask.FromException(e);
        }
    }

    public override int Read(Span<byte> buffer) => throw new NotSupportedException();

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfDisposed();
        WriteChunks(buffer);
    }
}
