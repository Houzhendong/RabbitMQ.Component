// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Vendored from CommunityToolkit/dotnet commit b135626dd54d33b8f05f2ff31591592c004aa848.

using System.Buffers;
using System.Runtime.CompilerServices;

namespace RabbitMQ.Component.Internal.HighPerformance.Streams;

/// <summary>A write-only <see cref="Stream"/> wrapping an <see cref="IBufferWriter{T}"/>.</summary>
/// <remarks>
/// Local patches replace upstream MemoryStream-internal validation helpers with .NET 8-compatible
/// validation and copy writes in GetSpan(1) chunks so bounded writers reject the next byte rather
/// than a decompressor's oversized write hint. Disposing this stream never disposes the writer.
/// </remarks>
internal sealed partial class IBufferWriterStream<TWriter> : Stream
    where TWriter : struct, IBufferWriter<byte>
{
    private readonly TWriter bufferWriter;
    private bool disposed;

    public IBufferWriterStream(TWriter bufferWriter) => this.bufferWriter = bufferWriter;

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !this.disposed;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken) =>
        throw new NotSupportedException();

    public override void Flush()
    {
        ThrowIfDisposed();
    }

    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
        try
        {
            Flush();
            return Task.CompletedTask;
        }
        catch (Exception e)
        {
            return Task.FromException(e);
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
        CancellationToken cancellationToken) => throw new NotSupportedException();

    public override Task WriteAsync(byte[] buffer, int offset, int count,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return Task.FromCanceled(cancellationToken);
        try
        {
            Write(buffer, offset, count);
            return Task.CompletedTask;
        }
        catch (OperationCanceledException e)
        {
            return Task.FromCanceled(e.CancellationToken);
        }
        catch (Exception e)
        {
            return Task.FromException(e);
        }
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override int ReadByte() => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
    {
        ThrowIfDisposed();
        ValidateBufferArguments(buffer, offset, count);
        WriteChunks(buffer.AsSpan(offset, count));
    }

    public override void WriteByte(byte value)
    {
        ThrowIfDisposed();
        Span<byte> destination = this.bufferWriter.GetSpan(1);
        if (destination.IsEmpty) ThrowEndOfBuffer();
        destination[0] = value;
        this.bufferWriter.Advance(1);
    }

    protected override void Dispose(bool disposing)
    {
        this.disposed = true;
        base.Dispose(disposing);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteChunks(ReadOnlySpan<byte> source)
    {
        while (!source.IsEmpty)
        {
            Span<byte> destination = this.bufferWriter.GetSpan(1);
            if (destination.IsEmpty) ThrowEndOfBuffer();
            int count = Math.Min(source.Length, destination.Length);
            source[..count].CopyTo(destination);
            this.bufferWriter.Advance(count);
            source = source[count..];
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(this.disposed, this);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowEndOfBuffer() =>
        throw new ArgumentException("The current buffer writer can't contain the requested input data.");
}
