using System.Buffers;

namespace RabbitMQ.Component.Serialization;

/// <summary>
/// Shared serializers must be thread-safe. The component does not dispose serializers.
/// Deserialize must not return objects borrowing the supplied delivery or decompression memory.
/// </summary>
public interface ISerializer<T>
{
    void Serialize(T message, IBufferWriter<byte> writer);
    T Deserialize(ReadOnlySpan<byte> body);
}

/// <summary>Optional fast path. Ownership transfers to the caller until the send has completed.</summary>
public interface IOwnedBufferSerializer<T> : ISerializer<T>
{
    IOwnedBuffer SerializeOwned(T message);
}

/// <summary>Length is the valid payload length, which may be smaller than Memory.Length.</summary>
public interface IOwnedBuffer : IMemoryOwner<byte>
{
    int Length { get; }
}
