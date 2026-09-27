using System.Buffers;
using System.IO.Compression;
using RabbitMQ.Component.Internal.HighPerformance;

namespace RabbitMQ.Component.Serialization;

/// <summary>Synchronous .NET gzip compression. Instances may be shared across threads.</summary>
public sealed class GzipCompressor(CompressionLevel compressionLevel = CompressionLevel.Optimal) : ICompressor
{
    public void Compress(ReadOnlySpan<byte> input, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        using var destination = output.AsStream();
        using var gzip = new GZipStream(destination, compressionLevel, leaveOpen: true);
        gzip.Write(input);
        // Dispose finalizes the trailer before returning, without disposing caller output.
    }

    public unsafe void Decompress(ReadOnlySpan<byte> input, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (input.IsEmpty) return; // fixed yields null for an empty span; empty input decodes to nothing, as before.
        // Read the borrowed input in place instead of copying it into a MemoryStream; the stream
        // never outlives this call, so the pin covers every read.
        fixed (byte* source = input)
        {
            using var stream = new UnmanagedMemoryStream(source, input.Length);
            using var gzip = new GZipStream(stream, CompressionMode.Decompress, leaveOpen: true);
            Span<byte> buffer = stackalloc byte[CompressionBuffer.ChunkSize];
            int count;
            while ((count = gzip.Read(buffer)) != 0) CompressionBuffer.Write(output, buffer[..count]);
        }
    }
}
