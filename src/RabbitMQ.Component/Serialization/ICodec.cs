using System.Buffers;

namespace RabbitMQ.Component.Serialization;

/// <summary>
/// Encodes and decodes messages. Shared codecs and their dependencies must be thread-safe.
/// The component does not dispose codecs. Decode must not return objects borrowing supplied memory.
/// </summary>
public interface ICodec<T>
{
    /// <summary>Encodes a message into caller-owned output. The output is borrowed only for this call.</summary>
    void Encode(T message, IBufferWriter<byte> output);

    /// <summary>
    /// Encodes a message into an independent, non-pooled managed array and returns its exact valid
    /// region. This convenience overload may allocate. The returned bytes remain valid after this
    /// call and are not overwritten by later codec operations. Do not retain a span across an
    /// asynchronous boundary; copy it to memory owned by the asynchronous operation instead.
    /// </summary>
    Span<byte> Encode(T message);

    /// <summary>Decodes a message while borrowing the input only for this synchronous call.</summary>
    T Decode(ReadOnlySpan<byte> input);
}

/// <summary>
/// Synchronously transforms borrowed input into caller-owned output. Shared implementations must be
/// thread-safe. Neither input nor output may be retained after the call, and the caller's output
/// must not be disposed. Dependencies remain caller-owned.
/// </summary>
public interface ICompressor
{
    void Compress(ReadOnlySpan<byte> input, IBufferWriter<byte> output);
    void Decompress(ReadOnlySpan<byte> input, IBufferWriter<byte> output);
}

/// <summary>
/// Optional encode fast path. True transfers an independent owner to the caller. False requires
/// the caller to use <see cref="ICodec{T}.Encode(T, IBufferWriter{byte})"/>.
/// </summary>
public interface IOwnedBufferCodec<T> : ICodec<T>
{
    bool TryEncodeOwned(T message, out IOwnedBuffer? owner);
}
