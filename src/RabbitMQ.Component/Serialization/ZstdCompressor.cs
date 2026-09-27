using System.Buffers;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace RabbitMQ.Component.Serialization;

/// <summary>
/// Zstandard compression via the managed ZstdSharp.Port library. Instances may be shared across threads:
/// each call uses and releases its own context. Decompression is strict: empty, truncated or malformed
/// payloads and trailing bytes are rejected; concatenated frames are decoded in order.
/// </summary>
public sealed class ZstdCompressor : ICompressor
{
    private readonly int _compressionLevel;
    private readonly int _maxWindowLog;

    /// <param name="compressionLevel">Zstandard level; defaults to 3.</param>
    /// <param name="maxWindowLog">
    /// Largest accepted frame window (log2 bytes). Output is bounded separately by the codec limit, but the
    /// decoder allocates a window from the frame header before producing output; the default 23 (8 MiB)
    /// covers every frame this type produces at levels up to 19.
    /// </param>
    public ZstdCompressor(int compressionLevel = Compressor.DefaultCompressionLevel, int maxWindowLog = 23)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(compressionLevel, Compressor.MinCompressionLevel);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(compressionLevel, Compressor.MaxCompressionLevel);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxWindowLog, 10);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxWindowLog, 31);
        _compressionLevel = compressionLevel;
        _maxWindowLog = maxWindowLog;
    }

    public void Compress(ReadOnlySpan<byte> input, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        using var compressor = new Compressor(_compressionLevel);
        // Records the content size in the frame header and shrinks the window to the message size.
        compressor.SetPledgedSrcSize((ulong)input.Length);
        Span<byte> buffer = stackalloc byte[CompressionBuffer.ChunkSize];
        while (true)
        {
            OperationStatus status = compressor.WrapStream(input, buffer, out int consumed, out int written, isFinalBlock: true);
            CompressionBuffer.Write(output, buffer[..written]);
            input = input[consumed..];
            if (status == OperationStatus.Done) return;
            if (status != OperationStatus.DestinationTooSmall || (consumed == 0 && written == 0))
                throw new InvalidOperationException("Zstandard compression failed.");
        }
    }

    public void Decompress(ReadOnlySpan<byte> input, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (input.IsEmpty) throw new InvalidDataException("The Zstandard payload is empty.");
        using var decompressor = new Decompressor();
        decompressor.SetParameter(ZSTD_dParameter.ZSTD_d_windowLogMax, _maxWindowLog);
        Span<byte> buffer = stackalloc byte[CompressionBuffer.ChunkSize];
        while (true)
        {
            // Done is reported only after every input byte was consumed, so concatenated frames continue
            // here and trailing garbage surfaces as InvalidData.
            OperationStatus status = decompressor.UnwrapStream(input, buffer, out int consumed, out int written);
            CompressionBuffer.Write(output, buffer[..written]);
            input = input[consumed..];
            switch (status)
            {
                case OperationStatus.Done:
                    return;
                case OperationStatus.NeedMoreData:
                    throw new InvalidDataException("The Zstandard payload is truncated.");
                case OperationStatus.DestinationTooSmall when consumed != 0 || written != 0:
                    continue;
                default:
                    throw new InvalidDataException("The Zstandard payload is invalid.");
            }
        }
    }
}
