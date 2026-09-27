using System.Buffers;
using System.IO.Compression;

namespace RabbitMQ.Component.Serialization;

/// <summary>Synchronous .NET Brotli compression. Instances may be shared across threads.</summary>
public sealed class BrotliCompressor(CompressionLevel compressionLevel = CompressionLevel.Optimal) : ICompressor
{
    // Same quality/window as BrotliStream, so the wire bytes are unchanged; the struct codec avoids
    // the per-message stream, input copy and writer-stream wrapper allocations.
    private const int Window = 22;
    private readonly int _quality = compressionLevel switch
    {
        CompressionLevel.NoCompression => 0,
        CompressionLevel.Fastest => 1,
        CompressionLevel.Optimal => 4,
        CompressionLevel.SmallestSize => 11,
        _ => throw new ArgumentOutOfRangeException(nameof(compressionLevel))
    };

    public void Compress(ReadOnlySpan<byte> input, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        using var encoder = new BrotliEncoder(_quality, Window);
        Span<byte> buffer = stackalloc byte[CompressionBuffer.ChunkSize];
        while (true)
        {
            OperationStatus status = encoder.Compress(input, buffer, out int consumed, out int written, isFinalBlock: true);
            CompressionBuffer.Write(output, buffer[..written]);
            input = input[consumed..];
            if (status == OperationStatus.Done) return;
            if (status != OperationStatus.DestinationTooSmall)
                throw new InvalidOperationException("Brotli compression failed.");
        }
    }

    public void Decompress(ReadOnlySpan<byte> input, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        using var decoder = new BrotliDecoder();
        Span<byte> buffer = stackalloc byte[CompressionBuffer.ChunkSize];
        while (true)
        {
            OperationStatus status = decoder.Decompress(input, buffer, out int consumed, out int written);
            CompressionBuffer.Write(output, buffer[..written]);
            input = input[consumed..];
            // Mirrors BrotliStream: trailing bytes after the final block and truncated input end the
            // read without an error; only malformed data throws, with BrotliStream's exception type.
            if (status is OperationStatus.Done or OperationStatus.NeedMoreData) return;
            if (status == OperationStatus.InvalidData)
                throw new InvalidOperationException("Brotli decompression failed.");
        }
    }
}
