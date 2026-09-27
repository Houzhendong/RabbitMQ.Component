using System.Buffers;

namespace RabbitMQ.Component.Serialization;

internal static class CompressionBuffer
{
    public const int ChunkSize = 8192;

    /// <summary>
    /// Copies in GetSpan(1) chunks, matching IBufferWriterStream: a bounded writer rejects only a byte
    /// that actually exceeds its limit, never an oversized hint for output that may not exist.
    /// </summary>
    public static void Write(IBufferWriter<byte> output, ReadOnlySpan<byte> source)
    {
        while (!source.IsEmpty)
        {
            Span<byte> destination = output.GetSpan(1);
            if (destination.IsEmpty) throw new ArgumentException("The current buffer writer can't contain the requested input data.");
            int count = Math.Min(source.Length, destination.Length);
            source[..count].CopyTo(destination);
            output.Advance(count);
            source = source[count..];
        }
    }
}
