using System.Buffers;
using System.Runtime.InteropServices;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;

namespace RabbitMQ.Component.Serialization;

/// <summary>
/// Serializes messages, optionally compressing the wire body. Both peers must agree on the codec;
/// no format header or compression detection is added. Dependencies remain caller-owned and must
/// be thread-safe when shared. Deserialized values must not borrow input or decompression memory.
/// </summary>
public sealed class CompositeCodec<T> : IOwnedBufferCodec<T>
{
    private readonly ISerializer<T> _serializer;
    private readonly ICompressor? _compressor;
    private readonly int _maxDecompressedBytes;

    public CompositeCodec(ISerializer<T> serializer, ICompressor? compressor = null,
        int maxDecompressedBytes = 64 * 1024 * 1024)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxDecompressedBytes);
        _serializer = serializer;
        _compressor = compressor;
        _maxDecompressedBytes = maxDecompressedBytes;
    }

    public void Encode(T message, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (_compressor is null)
        {
            _serializer.Serialize(message, output);
            return;
        }

        if (_serializer is IOwnedBufferSerializer<T> ownedSerializer)
        {
            IOwnedBuffer owner = ownedSerializer.SerializeOwned(message)
                ?? throw new InvalidOperationException("Serializer returned no buffer owner.");
            try
            {
                _compressor.Compress(ValidMemory(owner).Span, output);
            }
            finally
            {
                owner.Dispose();
            }
            return;
        }

        using var serialized = new OwnedArrayPoolBufferWriter();
        _serializer.Serialize(message, serialized);
        _compressor.Compress(serialized.WrittenSpan, output);
    }

    public Span<byte> Encode(T message)
    {
        var output = new ArrayBufferWriter<byte>();
        Encode(message, output);

        if (MemoryMarshal.TryGetArray(output.WrittenMemory, out ArraySegment<byte> segment))
            return segment.AsSpan();

        return output.WrittenSpan.ToArray().AsSpan();
    }

    public bool TryEncodeOwned(T message, out IOwnedBuffer? owner)
    {
        if (_compressor is null && _serializer is IOwnedBufferSerializer<T> ownedSerializer)
        {
            owner = ownedSerializer.SerializeOwned(message)
                ?? throw new InvalidOperationException("Serializer returned no buffer owner.");
            return true;
        }

        owner = null;
        return false;
    }

    public T Decode(ReadOnlySpan<byte> input)
    {
        if (_compressor is null) return _serializer.Deserialize(input);

        // Start near a typical compression ratio so common payloads skip most doubling steps, each of
        // which rents, copies and clears. Capped so a large, poorly compressible body cannot pre-rent
        // several times its size; beyond that the writer grows on demand up to the fixed limit.
        int initialCapacity = (int)Math.Min(_maxDecompressedBytes,
            Math.Min(64 * 1024L, Math.Max(256L, input.Length * 4L)));
        using var decompressed = new OwnedArrayPoolBufferWriter(initialCapacity, maxCapacity: _maxDecompressedBytes);
        _compressor.Decompress(input, decompressed);
        return _serializer.Deserialize(decompressed.WrittenSpan);
    }

    private static ReadOnlyMemory<byte> ValidMemory(IOwnedBuffer owner)
    {
        Memory<byte> memory = owner.Memory;
        int length = owner.Length;
        if (length < 0 || length > memory.Length)
            throw new InvalidOperationException("Serializer returned an invalid payload length.");
        return memory[..length];
    }
}
