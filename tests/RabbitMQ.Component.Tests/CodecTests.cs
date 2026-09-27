using System.Buffers;
using System.Text;
using System.Text.Json;
using RabbitMQ.Component.Internal.HighPerformance;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Tests;

public sealed class CodecTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void JsonRoundTripsSpanSliceWithoutBorrowingMemory(int kind)
    {
        var codec = new CompositeCodec<SerializationTests.Message>(
            JsonMessageSerializer<SerializationTests.Message>.Default, Compressor(kind));
        var expected = new SerializationTests.Message("你好 " + new string('a', 20000), 42);
        byte[] encoded = codec.Encode(expected).ToArray();
        byte[] envelope = Enumerable.Repeat((byte)0xff, encoded.Length + 10).ToArray();
        encoded.CopyTo(envelope, 4);

        var actual = codec.Decode(envelope.AsSpan(4, encoded.Length));
        Array.Clear(envelope);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 0)]
    [InlineData(2, 0)]
    [InlineData(3, 0)]
    [InlineData(0, 25000)]
    [InlineData(1, 25000)]
    [InlineData(2, 25000)]
    [InlineData(3, 25000)]
    public void EmptyAndVariableBytePayloadsRoundTripAndStreamsFinalize(int kind, int size)
    {
        byte[] expected = Enumerable.Range(0, size).Select(i => (byte)i).ToArray();
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), Compressor(kind));
        var output = new ArrayBufferWriter<byte>(1);

        codec.Encode(expected, output);

        Assert.Equal(expected, codec.Decode(output.WrittenSpan));
        // The compressor must leave caller output usable.
        output.GetSpan(1)[0] = 123;
        output.Advance(1);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void RepeatedEncodeReusesTheSameCallerWriterAfterClear(int kind)
    {
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), Compressor(kind));
        var output = new ArrayBufferWriter<byte>(1);
        foreach (byte[] expected in new[] { new byte[] { 1, 2, 3 }, new byte[] { 9, 8 } })
        {
            codec.Encode(expected, output);
            Assert.Equal(expected, codec.Decode(output.WrittenSpan));
            output.Clear();
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void ReturnSpanIsIndependentAndWireEquivalentToWriterOverload(int kind)
    {
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), Compressor(kind));

        Span<byte> first = codec.Encode(new byte[] { 1, 2, 3 });
        byte[] expectedFirst = first.ToArray();
        Span<byte> second = codec.Encode(new byte[] { 9, 8 });

        Assert.Equal(expectedFirst, first.ToArray());
        Assert.Equal(new byte[] { 9, 8 }, codec.Decode(second));

        var output = new ArrayBufferWriter<byte>();
        codec.Encode(new byte[] { 1, 2, 3 }, output);
        Assert.Equal(expectedFirst, output.WrittenSpan.ToArray());

        if (!first.IsEmpty) first[0] ^= 0xff;
        Assert.Equal(new byte[] { 9, 8 }, codec.Decode(second));
    }

    [Fact]
    public void ReturnSpanHasExactEmptyPayloadLength()
    {
        var codec = new CompositeCodec<byte[]>(new BytesSerializer());
        Assert.True(codec.Encode(Array.Empty<byte>()).IsEmpty);
    }

    [Fact]
    public void DecodeProvidesCompressorWithScopedFixedLimitWriter()
    {
        var compressor = new CallbackCompressor(output =>
        {
            var bounded = Assert.IsType<OwnedArrayPoolBufferWriter>(output);
            Assert.Equal(4, bounded.MaxCapacity);
            Assert.False(bounded.GetType().GetProperty(nameof(bounded.MaxCapacity))!.CanWrite);
            output.GetMemory(5);
        });
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), compressor, maxDecompressedBytes: 4);

        Assert.Throws<InvalidDataException>(() => codec.Decode(ReadOnlySpan<byte>.Empty));
        Assert.NotNull(compressor.Output);
        Assert.Throws<ObjectDisposedException>(() => compressor.Output!.GetMemory());
    }

    [Theory]
    [InlineData(System.IO.Compression.CompressionLevel.NoCompression)]
    [InlineData(System.IO.Compression.CompressionLevel.Fastest)]
    [InlineData(System.IO.Compression.CompressionLevel.Optimal)]
    [InlineData(System.IO.Compression.CompressionLevel.SmallestSize)]
    public void BrotliStructEncoderKeepsStreamWireFormat(System.IO.Compression.CompressionLevel level)
    {
        var input = new byte[8193];
        new Random(12).NextBytes(input);
        var expected = new ArrayBufferWriter<byte>();
        using (var stream = expected.AsStream())
        using (var compressor = new System.IO.Compression.BrotliStream(stream, level, leaveOpen: true))
            compressor.Write(input);

        var actual = new ArrayBufferWriter<byte>();
        new BrotliCompressor(level).Compress(input, actual);
        Assert.True(expected.WrittenSpan.SequenceEqual(actual.WrittenSpan));
    }

    [Theory]
    [InlineData("truncated")]
    [InlineData("half")]
    [InlineData("trailing")]
    [InlineData("empty")]
    [InlineData("invalid")]
    public void BrotliStructDecoderMatchesStreamForIncompleteOrExtraInput(string shape)
    {
        var compressed = new ArrayBufferWriter<byte>();
        new BrotliCompressor().Compress(Encoding.UTF8.GetBytes(new string('x', 1000) + "tail"), compressed);
        byte[] full = compressed.WrittenSpan.ToArray();
        byte[] input = shape switch
        {
            "truncated" => full[..^2],
            "half" => full[..(full.Length / 2)],
            "trailing" => [.. full, 1, 2, 3],
            "empty" => [],
            "invalid" => Enumerable.Repeat((byte)0xff, 64).ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        Assert.Equal(StreamOutcome(input), ComponentOutcome(input));

        static string StreamOutcome(byte[] input)
        {
            try
            {
                using var source = new MemoryStream(input);
                using var brotli = new System.IO.Compression.BrotliStream(source, System.IO.Compression.CompressionMode.Decompress);
                using var decoded = new MemoryStream();
                brotli.CopyTo(decoded);
                return Convert.ToHexString(decoded.ToArray());
            }
            catch (Exception exception) { return exception.GetType().Name; }
        }

        static string ComponentOutcome(byte[] input)
        {
            try
            {
                var decoded = new ArrayBufferWriter<byte>();
                new BrotliCompressor().Decompress(input, decoded);
                return Convert.ToHexString(decoded.WrittenSpan);
            }
            catch (Exception exception) { return exception.GetType().Name; }
        }
    }

    [Fact]
    public void ZstdInteroperatesWithStandardFramesAndConcatenation()
    {
        byte[] first = Encoding.UTF8.GetBytes(new string('a', 5000) + "first");
        byte[] second = Encoding.UTF8.GetBytes("second" + new string('b', 300));
        var compressor = new ZstdCompressor();

        var ours = new ArrayBufferWriter<byte>();
        compressor.Compress(first, ours);
        using (var reference = new ZstdSharp.Decompressor())
            Assert.Equal(first, reference.Unwrap(ours.WrittenSpan).ToArray());

        byte[] foreign;
        using (var reference = new ZstdSharp.Compressor(19))
            foreign = reference.Wrap(second).ToArray();
        Assert.Equal(second, ZstdDecompress(compressor, foreign));

        Assert.Equal([.. first, .. second], ZstdDecompress(compressor, [.. ours.WrittenSpan.ToArray(), .. foreign]));
    }

    [Theory]
    [InlineData("empty")]
    [InlineData("truncated")]
    [InlineData("half")]
    [InlineData("trailing")]
    [InlineData("invalid")]
    public void ZstdRejectsIncompleteExtraOrInvalidInputAndStaysUsable(string shape)
    {
        var compressor = new ZstdCompressor();
        byte[] payload = Encoding.UTF8.GetBytes(new string('z', 1000) + "tail");
        var compressed = new ArrayBufferWriter<byte>();
        compressor.Compress(payload, compressed);
        byte[] full = compressed.WrittenSpan.ToArray();
        byte[] input = shape switch
        {
            "empty" => [],
            "truncated" => full[..^1],
            "half" => full[..(full.Length / 2)],
            "trailing" => [.. full, 1, 2, 3],
            "invalid" => Enumerable.Repeat((byte)0xff, 64).ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };

        Assert.Throws<InvalidDataException>(() => ZstdDecompress(compressor, input));
        Assert.Equal(payload, ZstdDecompress(compressor, full));
    }

    [Fact]
    public void ZstdRejectsFramesWhoseWindowExceedsTheConfiguredLimit()
    {
        // A streamed frame of unknown size declares the full 128 MiB window despite a tiny body. (Ending
        // on the first call would let zstd treat it as one-shot and shrink the window to the input.)
        byte[] payload = Encoding.UTF8.GetBytes("large window");
        var buffer = new byte[1024];
        int length;
        using (var reference = new ZstdSharp.Compressor())
        {
            reference.SetParameter(ZstdSharp.Unsafe.ZSTD_cParameter.ZSTD_c_windowLog, 27);
            reference.WrapStream(payload, buffer, out int consumed, out length, isFinalBlock: false);
            Assert.Equal(payload.Length, consumed);
            Assert.Equal(OperationStatus.Done,
                reference.WrapStream([], buffer.AsSpan(length), out _, out int tail, isFinalBlock: true));
            length += tail;
        }
        byte[] frame = buffer[..length];

        Assert.Throws<InvalidDataException>(() => ZstdDecompress(new ZstdCompressor(), frame));
        Assert.Equal(payload, ZstdDecompress(new ZstdCompressor(maxWindowLog: 27), frame));
    }

    [Theory]
    [InlineData(int.MinValue, 23)]
    [InlineData(23, 23)]
    [InlineData(3, 9)]
    [InlineData(3, 32)]
    public void ZstdConstructorRejectsOutOfRangeSettings(int level, int maxWindowLog) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZstdCompressor(level, maxWindowLog));

    private static byte[] ZstdDecompress(ZstdCompressor compressor, byte[] input)
    {
        var output = new ArrayBufferWriter<byte>();
        compressor.Decompress(input, output);
        return output.WrittenSpan.ToArray();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void CompressorsAcceptSpanSlicesAndEmptySource(int kind)
    {
        ICompressor compressor = Compressor(kind)!;
        foreach (byte[] expected in new[] { Array.Empty<byte>(), Encoding.UTF8.GetBytes(new string('x', 20000)) })
        {
            byte[] envelope = Enumerable.Repeat((byte)0xcc, expected.Length + 6).ToArray();
            expected.CopyTo(envelope, 3);
            using var encoded = new OwnedArrayPoolBufferWriter();
            compressor.Compress(envelope.AsSpan(3, expected.Length), encoded);
            byte[] encodedEnvelope = Enumerable.Repeat((byte)0xdd, encoded.WrittenCount + 4).ToArray();
            encoded.WrittenSpan.CopyTo(encodedEnvelope.AsSpan(2));
            using var decoded = new OwnedArrayPoolBufferWriter();
            compressor.Decompress(encodedEnvelope.AsSpan(2, encoded.WrittenCount), decoded);
            Assert.Equal(expected, decoded.WrittenMemory.ToArray());
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void MalformedInputFails(int kind)
    {
        var codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default, Compressor(kind));
        byte[] malformed = Enumerable.Repeat((byte)0xff, 64).ToArray();
        if (kind == 0) Assert.ThrowsAny<JsonException>(() => codec.Decode(malformed));
        else if (kind is 1 or 3) Assert.Throws<InvalidDataException>(() => codec.Decode(malformed));
        else Assert.Throws<InvalidOperationException>(() => codec.Decode(malformed));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 1)]
    [InlineData(3, 1)]
    [InlineData(1, 257)]
    [InlineData(2, 257)]
    [InlineData(3, 257)]
    [InlineData(1, 8193)]
    [InlineData(2, 8193)]
    [InlineData(3, 8193)]
    public void ExactDecompressionLimitSucceedsAndOneMoreByteFails(int kind, int limit)
    {
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), Compressor(kind), limit);
        byte[] exact = Encode(codec, new byte[limit]);
        Assert.Equal(limit, codec.Decode(exact).Length);
        byte[] oversized = Encode(codec, new byte[limit + 1]);
        Assert.Throws<InvalidDataException>(() => codec.Decode(oversized));
    }

    [Theory]
    [InlineData("array")]
    [InlineData("span")]
    [InlineData("byte")]
    public void ToolkitWriterStreamWritesInChunksAndPreservesExactLogicalBound(string writeKind)
    {
        byte[] expected = Enumerable.Range(0, 257).Select(i => (byte)i).ToArray();
        using var writer = new OwnedArrayPoolBufferWriter(1, maxCapacity: expected.Length);
        using (Stream stream = writer.AsStream())
        {
            if (writeKind == "array") stream.Write(expected, 0, expected.Length);
            else if (writeKind == "span") stream.Write(expected.AsSpan());
            else foreach (byte value in expected) stream.WriteByte(value);

            Assert.Equal(expected, writer.WrittenMemory.ToArray());
            Assert.Throws<InvalidDataException>(() => stream.WriteByte(1));
        }

        // Disposing the adapter must not dispose its caller-owned writer.
        writer.Clear();
        writer.GetSpan(1)[0] = 42;
        writer.Advance(1);
        Assert.Equal(new byte[] { 42 }, writer.WrittenMemory.ToArray());
    }

    [Fact]
    public async Task ToolkitWriterStreamMemoryWriteUsesChunkedBoundedPath()
    {
        byte[] expected = Enumerable.Range(0, 257).Select(i => (byte)i).ToArray();
        using var writer = new OwnedArrayPoolBufferWriter(1, maxCapacity: expected.Length);
        using Stream stream = writer.AsStream();
        await stream.WriteAsync(expected.AsMemory());
        Assert.Equal(expected, writer.WrittenMemory.ToArray());
        await Assert.ThrowsAsync<InvalidDataException>(async () => await stream.WriteAsync(new byte[] { 1 }.AsMemory()));
    }

    [Theory]
    [InlineData("hint")]
    [InlineData("advance")]
    [InlineData("one-more")]
    [InlineData("overflow")]
    public void ArbitraryCompressorCannotBypassOutputLimit(string attack)
    {
        var serializer = new BytesSerializer();
        var compressor = new CallbackCompressor(output =>
        {
            if (attack == "hint") { output.GetMemory(258); return; }
            if (attack == "overflow") { output.GetSpan(int.MaxValue); return; }
            Memory<byte> memory = output.GetMemory(257);
            Assert.Equal(257, memory.Length);
            if (attack == "advance") { output.Advance(int.MaxValue); return; }
            output.Advance(257);
            output.GetSpan();
        });
        var codec = new CompositeCodec<byte[]>(serializer, compressor, 257);
        Assert.Throws<InvalidDataException>(() => codec.Decode(ReadOnlySpan<byte>.Empty));
        Assert.Equal(0, serializer.DecodeCalls);
        Assert.Throws<ObjectDisposedException>(() => compressor.Output!.GetMemory());
    }

    [Fact]
    public void CustomCompressorCanFillExactLimitButCannotAdvanceUnprovidedMemory()
    {
        var exact = new CallbackCompressor(output =>
        {
            output.GetSpan(257).Fill(7);
            output.Advance(257);
        });
        Assert.Equal(Enumerable.Repeat((byte)7, 257),
            new CompositeCodec<byte[]>(new BytesSerializer(), exact, 257).Decode(ReadOnlySpan<byte>.Empty));
        var invalid = new CallbackCompressor(output => output.Advance(1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CompositeCodec<byte[]>(new BytesSerializer(), invalid, 257).Decode(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void BoundedWriterUsesOversizedRentalWithoutExposingPastLogicalLimit()
    {
        var pool = new OversizedReusingPool(extraCapacity: 1024);
        var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 257);
        byte[] backing = Assert.Single(pool.Rented);
        Assert.True(backing.Length > 257);
        Assert.Equal(257, writer.Capacity);
        Assert.Empty(((IOwnedBuffer)writer).Memory.ToArray());
        Assert.Equal(257, writer.GetMemory().Length);
        Assert.Equal(257, writer.GetSpan().Length);

        int rents = pool.Rented.Count;
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.GetMemory(-1));
        Assert.Throws<InvalidDataException>(() => writer.GetMemory(258));
        Assert.Throws<InvalidDataException>(() => writer.GetSpan(int.MaxValue));
        Assert.Throws<InvalidDataException>(() => writer.Advance(258));
        Assert.Equal(rents, pool.Rented.Count);

        writer.GetSpan(257).Fill(42);
        writer.Advance(257);
        Assert.Equal(257, writer.WrittenCount);
        Assert.Equal(257, writer.WrittenMemory.Length);
        Assert.Equal(257, ((IOwnedBuffer)writer).Memory.Length);
        Assert.All(writer.WrittenMemory.ToArray(), b => Assert.Equal(42, b));
        Assert.Throws<InvalidDataException>(() => writer.GetMemory());
        Assert.Throws<InvalidDataException>(() => writer.GetSpan(1));
        Assert.Throws<InvalidDataException>(() => writer.Advance(1));
        Assert.Equal(rents, pool.Rented.Count);

        writer.Dispose();
        writer.Dispose();
        Assert.Single(pool.Returned);
        Assert.Equal(0, pool.ActiveCount);
        Assert.All(backing, b => Assert.Equal(0, b));
        Assert.Throws<ObjectDisposedException>(() => _ = ((IOwnedBuffer)writer).Memory);
    }

    [Fact]
    public void BoundedWriterGrowthReturnsEachRentalOnceAndClampsLogicalCapacity()
    {
        var pool = new OversizedReusingPool(extraCapacity: 1);
        var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 9);
        Assert.Equal(2, writer.Capacity);
        writer.GetSpan(2)[0] = 11;
        writer.GetSpan(2)[1] = 12;
        writer.Advance(2);

        Assert.Equal(4, writer.GetMemory(3).Length);
        Assert.Equal(new byte[] { 11, 12 }, writer.WrittenMemory.ToArray());
        Assert.Equal(2, pool.Rented.Count);
        Assert.Single(pool.Returned);
        writer.GetSpan(3).Fill(13);
        writer.Advance(3);

        Assert.Equal(4, writer.GetSpan(4).Length);
        Assert.Equal(9, writer.Capacity);
        Assert.Equal(new byte[] { 11, 12, 13, 13, 13 }, writer.WrittenMemory.ToArray());
        Assert.Equal(3, pool.Rented.Count);
        Assert.Equal(2, pool.Returned.Count);
        writer.GetSpan(4).Fill(14);
        writer.Advance(4);
        Assert.Equal(9, writer.WrittenCount);

        int rents = pool.Rented.Count;
        Assert.Throws<InvalidDataException>(() => writer.GetMemory(1));
        Assert.Throws<InvalidDataException>(() => writer.Advance(1));
        Assert.Equal(rents, pool.Rented.Count);
        writer.Dispose();
        writer.Dispose();

        Assert.Equal(pool.Rented.Count, pool.Returned.Count);
        Assert.Equal(0, pool.ActiveCount);
        Assert.All(pool.Rented, rented =>
            Assert.Equal(1, pool.Returned.Count(returned => ReferenceEquals(rented, returned))));
        Assert.All(pool.Returned, returned => Assert.All(returned, b => Assert.Equal(0, b)));
    }

    [Fact]
    public void RepeatedWriterUseReusesPoolAfterSuccessExceptionAndCodecFailure()
    {
        var pool = new OversizedReusingPool(extraCapacity: 7);
        byte[] first;
        using (var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 8))
        {
            first = Assert.Single(pool.Rented);
            writer.GetSpan(8).Fill(1);
            writer.Advance(8);
        }

        Assert.Throws<FormatException>((Action)(() =>
        {
            using var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 8);
            Assert.Same(first, pool.Rented[^1]);
            writer.GetSpan(2).Fill(2);
            writer.Advance(2);
            throw new FormatException();
        }));

        var serializer = new BytesSerializer { FailEncode = true };
        Assert.Throws<FormatException>(() =>
        {
            using var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 8);
            Assert.Same(first, pool.Rented[^1]);
            new CompositeCodec<byte[]>(serializer).Encode(new byte[] { 3, 4, 5 }, writer);
        });

        Assert.Equal(1, pool.CreatedCount);
        Assert.Equal(3, pool.Rented.Count);
        Assert.Equal(pool.Rented.Count, pool.Returned.Count);
        Assert.Equal(0, pool.ActiveCount);
        Assert.All(pool.Rented, rented => Assert.Same(first, rented));
        Assert.All(first, b => Assert.Equal(0, b));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ConstructorRejectsNonpositiveLimit(int limit) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new CompositeCodec<int>(JsonMessageSerializer<int>.Default, maxDecompressedBytes: limit));

    [Fact]
    public void ConstructorRejectsNullSerializerAndPlainCodecDoesNotApplyDecompressionLimit()
    {
        Assert.Throws<ArgumentNullException>(() => new CompositeCodec<int>(null!));
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), maxDecompressedBytes: 1);
        Assert.Equal(2, codec.Decode(new byte[2]).Length);
        Assert.Throws<ArgumentNullException>(() => codec.Encode(Array.Empty<byte>(), null!));
    }

    [Fact]
    public void DefaultLimitIs64MiBAndCheckedBeforeLargeAllocation()
    {
        var compressor = new CallbackCompressor(output => output.GetMemory(64 * 1024 * 1024 + 1));
        var codec = new CompositeCodec<byte[]>(new BytesSerializer(), compressor);
        Assert.Throws<InvalidDataException>(() => codec.Decode(ReadOnlySpan<byte>.Empty));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void OwnedSerializerFastPathSurvivesComposition(bool compressed)
    {
        var serializer = new OwnedSerializer();
        var codec = new CompositeCodec<byte[]>(serializer, compressed ? new GzipCompressor() : null);
        IOwnedBuffer? owner = null;
        byte[] encoded;
        if (codec.TryEncodeOwned(new byte[] { 1, 2, 3 }, out owner))
        {
            Assert.NotNull(owner);
            encoded = owner.Memory[..owner.Length].ToArray();
        }
        else
        {
            encoded = Encode(codec, new byte[] { 1, 2, 3 });
        }

        Assert.Equal(0, serializer.SerializeCalls);
        Assert.Equal(1, serializer.OwnedCalls);
        Assert.Equal(compressed ? 1 : 0, serializer.Owner!.DisposeCount);
        if (!compressed) Assert.Same(serializer.Owner, owner);
        Assert.Equal(new byte[] { 1, 2, 3 }, codec.Decode(encoded));
        owner?.Dispose();
        Assert.Equal(1, serializer.Owner.DisposeCount);
        Assert.False(serializer.Disposed);
    }

    [Fact]
    public void PlainOwnedFastPathDoesNotCallThrowingOrdinarySerialize()
    {
        var serializer = new OwnedSerializer { FailEncode = true };
        var codec = new CompositeCodec<byte[]>(serializer);
        Assert.True(codec.TryEncodeOwned(new byte[] { 4, 5 }, out IOwnedBuffer? owner));
        using (owner)
        {
            Assert.NotNull(owner);
            Assert.Equal(new byte[] { 4, 5 }, owner.Memory[..owner.Length].ToArray());
        }
        Assert.Equal(0, serializer.SerializeCalls);
        Assert.Equal(1, serializer.OwnedCalls);
    }

    [Fact]
    public void CompressionFailureAndOwnerDisposalFailureReleaseOwnersButNotCallerOutput()
    {
        var serializer = new OwnedSerializer();
        var compressor = new CallbackCompressor(output => throw new FormatException());
        var codec = new CompositeCodec<byte[]>(serializer, compressor);
        var output = new ArrayBufferWriter<byte>();
        Assert.Throws<FormatException>(() => codec.Encode(new byte[] { 1 }, output));
        Assert.Equal(1, serializer.Owner!.DisposeCount);
        Assert.Same(output, compressor.Output);
        Assert.False(serializer.Disposed);
        Assert.False(compressor.Disposed);

        output.Clear();
        serializer.ThrowOnDispose = true;
        compressor.Callback = writer => { writer.GetSpan(1)[0] = 1; writer.Advance(1); };
        Assert.Throws<IOException>(() => codec.Encode(new byte[] { 1 }, output));
        Assert.Equal(1, serializer.Owner!.DisposeCount);
        output.Clear();
        output.GetSpan(1)[0] = 42;
        output.Advance(1);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("memory")]
    [InlineData("length")]
    [InlineData("negative")]
    [InlineData("oversized")]
    public void CompressedCodecReleasesInvalidSerializerOwnersExactlyOnce(string failure)
    {
        var serializer = new InvalidOwnedSerializer(failure);
        var codec = new CompositeCodec<byte[]>(serializer, new GzipCompressor());
        var output = new ArrayBufferWriter<byte>();
        Assert.ThrowsAny<Exception>(() => codec.Encode(new byte[] { 1 }, output));
        Assert.Equal(failure == "null" ? 0 : 1, serializer.DisposeCalls);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void CompressorFinalizationFailureDoesNotDisposeCallerOutput(int kind)
    {
        var serializer = new OwnedSerializer();
        var codec = new CompositeCodec<byte[]>(serializer, Compressor(kind));
        var output = new FailingWriter();
        Assert.Throws<IOException>(() => codec.Encode(Array.Empty<byte>(), output));
        Assert.Equal(1, serializer.Owner!.DisposeCount);
        Assert.False(output.Disposed);
    }

    [Fact]
    public void DecodeFailureDisposesInternalBoundedBufferButNotDependencies()
    {
        int observedMaxCapacity = 0;
        var compressor = new CallbackCompressor(output =>
        {
            observedMaxCapacity = Assert.IsType<OwnedArrayPoolBufferWriter>(output).MaxCapacity;
            output.GetSpan(1)[0] = 1;
            output.Advance(1);
        });
        var serializer = new BytesSerializer { FailDecode = true };
        var codec = new CompositeCodec<byte[]>(serializer, compressor, maxDecompressedBytes: 8);

        Assert.Throws<FormatException>(() => codec.Decode(ReadOnlySpan<byte>.Empty));
        var bounded = Assert.IsType<OwnedArrayPoolBufferWriter>(compressor.Output);
        Assert.Equal(8, observedMaxCapacity);
        Assert.Throws<ObjectDisposedException>(() => bounded.GetMemory());
        Assert.False(serializer.Disposed);
        Assert.False(compressor.Disposed);

        compressor.Callback = _ => throw new IOException();
        Assert.Throws<IOException>(() => codec.Decode(ReadOnlySpan<byte>.Empty));
        Assert.Equal(1, serializer.DecodeCalls);
        Assert.Throws<ObjectDisposedException>(() => compressor.Output!.GetMemory());
    }

    [Fact]
    public void CompressedSerializationUsesAndDisposesScopedIntermediate()
    {
        var serializer = new BytesSerializer();
        var codec = new CompositeCodec<byte[]>(serializer, new BrotliCompressor());
        var output = new ArrayBufferWriter<byte>();

        codec.Encode(new byte[] { 1, 2 }, output);

        var intermediate = Assert.IsType<OwnedArrayPoolBufferWriter>(serializer.Output);
        Assert.Throws<ObjectDisposedException>(() => intermediate.GetMemory());
        Assert.Equal(new byte[] { 1, 2 }, codec.Decode(output.WrittenSpan));
    }

    [Fact]
    public void SerializationFailureDisposesIntermediateAndLeavesCallerOutputReusable()
    {
        var serializer = new BytesSerializer { FailEncode = true };
        var codec = new CompositeCodec<byte[]>(serializer, new BrotliCompressor());
        var output = new ArrayBufferWriter<byte>();
        Assert.Throws<FormatException>(() => codec.Encode(new byte[] { 1, 2 }, output));
        Assert.Throws<ObjectDisposedException>(() => serializer.Output!.GetMemory());
        output.Clear();
        output.GetSpan(1)[0] = 42;
        output.Advance(1);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task SharedCodecAndCompressorSupportConcurrentOperations(int kind)
    {
        var codec = new CompositeCodec<int>(JsonMessageSerializer<int>.Default, Compressor(kind));
        await Task.WhenAll(Enumerable.Range(0, 50).Select(i => Task.Run(() =>
        {
            byte[] body = Encode(codec, i);
            Assert.Equal(i, codec.Decode(body));
        })));
    }

    private static byte[] Encode<TMessage>(ICodec<TMessage> codec, TMessage message) =>
        codec.Encode(message).ToArray();

    private static ICompressor? Compressor(int kind) => kind switch
    {
        0 => null, 1 => new GzipCompressor(), 2 => new BrotliCompressor(), 3 => new ZstdCompressor(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private class BytesSerializer : ISerializer<byte[]>, IDisposable
    {
        public int DecodeCalls { get; private set; }
        public int SerializeCalls { get; private set; }
        public bool Disposed { get; private set; }
        public bool FailDecode { get; init; }
        public bool FailEncode { get; init; }
        public IBufferWriter<byte>? Output { get; private set; }
        public void Serialize(byte[] message, IBufferWriter<byte> writer)
        {
            SerializeCalls++;
            Output = writer;
            writer.Write(message);
            if (FailEncode) throw new FormatException();
        }
        public byte[] Deserialize(ReadOnlySpan<byte> body)
        {
            DecodeCalls++;
            if (FailDecode) throw new FormatException();
            return body.ToArray();
        }
        public void Dispose() => Disposed = true;
    }

    private sealed class OwnedSerializer : BytesSerializer, IOwnedBufferSerializer<byte[]>
    {
        public Owner? Owner { get; private set; }
        public int OwnedCalls { get; private set; }
        public bool ThrowOnDispose { get; set; }
        public IOwnedBuffer SerializeOwned(byte[] message)
        {
            OwnedCalls++;
            Owner = new Owner(message, ThrowOnDispose);
            return Owner;
        }
    }

    private sealed class Owner(byte[] bytes, bool throwOnDispose) : IOwnedBuffer
    {
        // Deliberately expose more memory than the valid payload.
        public Memory<byte> Memory { get; } = bytes.Concat(new byte[] { 99, 99 }).ToArray();
        public int Length => bytes.Length;
        public int DisposeCount { get; private set; }
        public void Dispose()
        {
            if (DisposeCount != 0) return;
            DisposeCount++;
            Memory.Span.Clear();
            if (throwOnDispose) throw new IOException();
        }
    }

    private sealed class CallbackCompressor(Action<IBufferWriter<byte>> callback) : ICompressor, IDisposable
    {
        public Action<IBufferWriter<byte>> Callback { get; set; } = callback;
        public IBufferWriter<byte>? Output { get; private set; }
        public bool Disposed { get; private set; }
        public void Compress(ReadOnlySpan<byte> input, IBufferWriter<byte> output) { Output = output; Callback(output); }
        public void Decompress(ReadOnlySpan<byte> input, IBufferWriter<byte> output) { Output = output; Callback(output); }
        public void Dispose() => Disposed = true;
    }

    private sealed class InvalidOwnedSerializer(string failure) : IOwnedBufferSerializer<byte[]>, IOwnedBuffer
    {
        public int DisposeCalls { get; private set; }
        public Memory<byte> Memory => failure == "memory" ? throw new FormatException() : new byte[1];
        public int Length => failure switch
        {
            "length" => throw new FormatException(), "negative" => -1, "oversized" => 2, _ => 1
        };
        public IOwnedBuffer SerializeOwned(byte[] message) => failure == "null" ? null! : this;
        public void Serialize(byte[] message, IBufferWriter<byte> writer) => throw new NotSupportedException();
        public byte[] Deserialize(ReadOnlySpan<byte> body) => throw new NotSupportedException();
        public void Dispose() => DisposeCalls++;
    }

    private sealed class FailingWriter : IBufferWriter<byte>, IDisposable
    {
        public bool Disposed { get; private set; }
        public void Advance(int count) => throw new IOException();
        public Memory<byte> GetMemory(int sizeHint = 0) => throw new IOException();
        public Span<byte> GetSpan(int sizeHint = 0) => throw new IOException();
        public void Dispose() => Disposed = true;
    }

    private sealed class OversizedReusingPool(int extraCapacity) : ArrayPool<byte>
    {
        private readonly Stack<byte[]> _available = [];
        private readonly HashSet<byte[]> _active = new(ReferenceEqualityComparer.Instance);

        public List<byte[]> Rented { get; } = [];
        public List<byte[]> Returned { get; } = [];
        public int ActiveCount => _active.Count;
        public int CreatedCount { get; private set; }

        public override byte[] Rent(int minimumLength)
        {
            byte[] rented;
            if (_available.TryPeek(out byte[]? available) && available.Length >= minimumLength)
            {
                rented = _available.Pop();
            }
            else
            {
                rented = new byte[checked(minimumLength + extraCapacity)];
                CreatedCount++;
            }

            Assert.True(_active.Add(rented));
            Rented.Add(rented);
            return rented;
        }

        public override void Return(byte[] array, bool clearArray = false)
        {
            Assert.True(clearArray);
            Assert.True(_active.Remove(array));
            Array.Clear(array);
            Returned.Add(array);
            _available.Push(array);
        }
    }
}
