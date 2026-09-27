using System.Buffers;
using System.Text;
using System.Text.Json;
using RabbitMQ.Component.Internal.HighPerformance.Buffers;
using RabbitMQ.Component.Serialization;

namespace RabbitMQ.Component.Tests;

public sealed class SerializationTests
{
    public sealed record Message(string Text, int Number);

    [Fact]
    public void JsonRoundTripsSpanSliceAndDoesNotBorrowMemory()
    {
        var serializer = new JsonMessageSerializer<Message>();
        using var writer = new OwnedArrayPoolBufferWriter(1);
        serializer.Serialize(new("你好", 42), writer);
        byte[] bytes = Enumerable.Repeat((byte)0xff, writer.WrittenCount + 8).ToArray();
        writer.WrittenSpan.CopyTo(bytes.AsSpan(3));

        var message = serializer.Deserialize(bytes.AsSpan(3, writer.WrittenCount));
        Array.Fill(bytes, (byte)0);

        Assert.Equal(new("你好", 42), message);
    }

    [Fact]
    public void JsonCopiesOptionsAndRejectsMalformedOrTrailingValues()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        var serializer = new JsonMessageSerializer<Message>(options);
        options.PropertyNamingPolicy = null;
        using var writer = new OwnedArrayPoolBufferWriter();
        serializer.Serialize(new("x", 1), writer);
        Assert.Contains("\"text\"", Encoding.UTF8.GetString(writer.WrittenSpan));
        Assert.ThrowsAny<JsonException>(() => serializer.Deserialize("{"u8));
        Assert.ThrowsAny<JsonException>(() => new JsonMessageSerializer<int>().Deserialize("1 2"u8));
        Assert.Null(new JsonMessageSerializer<string?>().Deserialize("null"u8));
    }

    [Fact]
    public async Task SharedJsonSerializerCanRunConcurrently()
    {
        var serializer = JsonMessageSerializer<Message>.Default;
        await Task.WhenAll(Enumerable.Range(0, 100).Select(i => Task.Run(() =>
        {
            using var writer = new OwnedArrayPoolBufferWriter();
            serializer.Serialize(new("parallel", i), writer);
            Assert.Equal(i, serializer.Deserialize(writer.WrittenSpan).Number);
        })));
    }

    public sealed record Envelope(Message Inner);

    [Fact]
    public void ReusedJsonWriterSurvivesRecursionAndFailedSerialization()
    {
        // A converter that serializes on the same thread must not share the outer operation's writer.
        var options = new JsonSerializerOptions();
        options.Converters.Add(new NestedSerializingConverter());
        var serializer = new JsonMessageSerializer<Envelope>(options);
        using var writer = new OwnedArrayPoolBufferWriter();
        serializer.Serialize(new(new("inner", 7)), writer);
        Assert.Equal("""{"Inner":{"Text":"inner","Number":7}}""", Encoding.UTF8.GetString(writer.WrittenSpan));

        // A failure mid-object must not leave writer state for the next message on this thread.
        var failing = new JsonMessageSerializer<Envelope>(new JsonSerializerOptions { Converters = { new ThrowingConverter() } });
        using (var discarded = new OwnedArrayPoolBufferWriter())
            Assert.Throws<FormatException>(() => failing.Serialize(new(new("x", 1)), discarded));
        using var next = new OwnedArrayPoolBufferWriter();
        JsonMessageSerializer<Message>.Default.Serialize(new("after", 2), next);
        Assert.Equal("""{"Text":"after","Number":2}""", Encoding.UTF8.GetString(next.WrittenSpan));
    }

    [Fact]
    public void JsonSerializeReusesTheThreadWriterWithoutAllocating()
    {
        var serializer = JsonMessageSerializer<Message>.Default;
        var message = new Message("steady-state", 3);
        using var writer = new OwnedArrayPoolBufferWriter();
        for (int i = 0; i < 10; i++) { serializer.Serialize(message, writer); writer.Clear(); }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++) { serializer.Serialize(message, writer); writer.Clear(); }
        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    private sealed class NestedSerializingConverter : System.Text.Json.Serialization.JsonConverter<Message>
    {
        public override Message Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            throw new NotSupportedException();

        public override void Write(Utf8JsonWriter writer, Message value, JsonSerializerOptions options)
        {
            using var nested = new OwnedArrayPoolBufferWriter();
            JsonMessageSerializer<Message>.Default.Serialize(value, nested);
            writer.WriteRawValue(nested.WrittenSpan);
        }
    }

    private sealed class ThrowingConverter : System.Text.Json.Serialization.JsonConverter<Message>
    {
        public override Message Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            throw new NotSupportedException();

        public override void Write(Utf8JsonWriter writer, Message value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("partial", "value");
            throw new FormatException();
        }
    }

    [Fact]
    public void WriterGrowsPreservesPayloadAndReturnsEveryArrayOnce()
    {
        var pool = new TrackingPool();
        var writer = new OwnedArrayPoolBufferWriter(2, pool);
        writer.GetSpan(2)[0] = 7;
        writer.Advance(1);
        Assert.True(writer.GetMemory(5).Length >= 5);
        Assert.Equal(new byte[] { 7 }, writer.WrittenMemory.ToArray());
        Assert.Single(pool.Returned);
        writer.Clear();
        Assert.Equal(0, writer.WrittenCount);
        Assert.Equal(0, writer.GetSpan()[0]);
        writer.Dispose(); writer.Dispose();
        Assert.Equal(pool.Rented.Count, pool.Returned.Count);
        Assert.All(pool.Returned, bytes => Assert.All(bytes, b => Assert.Equal(0, b)));
        Assert.Throws<ObjectDisposedException>(() => writer.GetMemory());
        Assert.Throws<ObjectDisposedException>(() => writer.Advance(0));
        Assert.Throws<ObjectDisposedException>(() => writer.Clear());
    }

    [Fact]
    public void WriterRejectsInvalidHintsAndAdvance()
    {
        using var writer = new OwnedArrayPoolBufferWriter(1, new TrackingPool());
        Assert.False(writer.GetMemory().IsEmpty);
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.GetMemory(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(2));
        Assert.Throws<ArgumentOutOfRangeException>(() => new OwnedArrayPoolBufferWriter(0));
    }

    [Fact]
    public void FailedGrowthClearsReplacementAndKeepsOriginalOwnedUntilDispose()
    {
        var pool = new UndersizedGrowthPool();
        var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 8);
        writer.GetSpan(1)[0] = 9;
        writer.Advance(1);

        Assert.Throws<InvalidOperationException>(() => writer.GetSpan(1));
        Assert.Equal(new byte[] { 9 }, writer.WrittenMemory.ToArray());
        Assert.Single(pool.Returned);

        writer.Dispose();
        writer.Dispose();
        Assert.Equal(2, pool.Returned.Count);
        Assert.All(pool.Returned, bytes => Assert.All(bytes, value => Assert.Equal(0, value)));
    }

    [Fact]
    public void ThrowingReturnDuringGrowthPreclearsOldArrayAndKeepsReplacementOwned()
    {
        var pool = new ThrowingReturnPool { ThrowOnNextReturn = true };
        var writer = new OwnedArrayPoolBufferWriter(1, pool, maxCapacity: 8);
        writer.GetSpan(1)[0] = 9;
        writer.Advance(1);
        byte[] original = Assert.Single(pool.Rented);

        Assert.Throws<IOException>(() => writer.GetSpan(1));
        Assert.All(original, value => Assert.Equal(0, value));
        Assert.Equal(new byte[] { 9 }, writer.WrittenMemory.ToArray());
        Assert.Equal(2, pool.Rented.Count);
        Assert.Single(pool.ReturnAttempts);

        byte[] replacement = pool.Rented[1];
        writer.Dispose();
        writer.Dispose();
        Assert.Equal(2, pool.ReturnAttempts.Count);
        Assert.Same(original, pool.ReturnAttempts[0]);
        Assert.Same(replacement, pool.ReturnAttempts[1]);
        Assert.All(replacement, value => Assert.Equal(0, value));
    }

    [Fact]
    public void ThrowingReturnDuringDisposePreclearsFinalArrayWithoutRetrying()
    {
        var pool = new ThrowingReturnPool { ThrowOnNextReturn = true };
        var writer = new OwnedArrayPoolBufferWriter(2, pool);
        writer.GetSpan(2).Fill(7);
        writer.Advance(2);
        byte[] rented = Assert.Single(pool.Rented);

        Assert.Throws<IOException>(() => writer.Dispose());
        Assert.All(rented, value => Assert.Equal(0, value));
        Assert.Single(pool.ReturnAttempts);
        Assert.Throws<ObjectDisposedException>(() => writer.GetMemory());

        writer.Dispose();
        Assert.Single(pool.ReturnAttempts);
    }

    private sealed class TrackingPool : ArrayPool<byte>
    {
        public List<byte[]> Rented { get; } = [];
        public List<byte[]> Returned { get; } = [];
        public override byte[] Rent(int minimumLength) { var bytes = new byte[minimumLength]; Rented.Add(bytes); return bytes; }
        public override void Return(byte[] array, bool clearArray = false)
        {
            Assert.DoesNotContain(array, Returned);
            Assert.True(clearArray);
            Array.Clear(array);
            Returned.Add(array);
        }
    }

    private sealed class UndersizedGrowthPool : ArrayPool<byte>
    {
        private int _rentCount;
        public List<byte[]> Returned { get; } = [];

        public override byte[] Rent(int minimumLength) =>
            new byte[_rentCount++ == 0 ? minimumLength : minimumLength - 1];

        public override void Return(byte[] array, bool clearArray = false)
        {
            Assert.True(clearArray);
            Assert.DoesNotContain(Returned, returned => ReferenceEquals(array, returned));
            Array.Clear(array);
            Returned.Add(array);
        }
    }

    private sealed class ThrowingReturnPool : ArrayPool<byte>
    {
        public bool ThrowOnNextReturn { get; set; }
        public List<byte[]> Rented { get; } = [];
        public List<byte[]> ReturnAttempts { get; } = [];

        public override byte[] Rent(int minimumLength)
        {
            var array = new byte[minimumLength];
            Rented.Add(array);
            return array;
        }

        public override void Return(byte[] array, bool clearArray = false)
        {
            Assert.True(clearArray);
            Assert.DoesNotContain(array, ReturnAttempts);
            ReturnAttempts.Add(array);
            if (!ThrowOnNextReturn) return;
            ThrowOnNextReturn = false;
            throw new IOException("The pool rejected the return after observing the array.");
        }
    }
}
