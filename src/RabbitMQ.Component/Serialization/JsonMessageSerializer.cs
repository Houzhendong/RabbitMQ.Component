using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace RabbitMQ.Component.Serialization;

public sealed class JsonMessageSerializer<T> : ISerializer<T>
{
    private readonly JsonSerializerOptions _options;
    private JsonTypeInfo<T>? _typeInfo;
    public static JsonMessageSerializer<T> Default { get; } = new();

    /// <summary>Options are copied and frozen. Custom converters must themselves be thread-safe.</summary>
    public JsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options is null ? new JsonSerializerOptions() : new JsonSerializerOptions(options);
        _options.MakeReadOnly(populateMissingResolver: true);
    }

    // Resolved on first use so unsupported types still fail at (de)serialization, not construction.
    private JsonTypeInfo<T> TypeInfo => _typeInfo ??= (JsonTypeInfo<T>)_options.GetTypeInfo(typeof(T));

    public void Serialize(T message, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        Utf8JsonWriter jsonWriter = ReusableJsonWriter.Rent(writer);
        try
        {
            JsonSerializer.Serialize(jsonWriter, message, TypeInfo);
            jsonWriter.Flush();
        }
        finally
        {
            ReusableJsonWriter.Return(jsonWriter);
        }
    }

    public T Deserialize(ReadOnlySpan<byte> body)
    {
        var reader = new Utf8JsonReader(body, new JsonReaderOptions
        {
            AllowTrailingCommas = _options.AllowTrailingCommas,
            CommentHandling = _options.ReadCommentHandling,
            MaxDepth = _options.MaxDepth
        });
        T result = JsonSerializer.Deserialize(ref reader, TypeInfo)!;
        if (reader.Read())
            throw new JsonException("The payload must contain exactly one JSON value.");
        return result;
    }
}

/// <summary>One default-options writer per thread, shared by every message type.</summary>
internal static class ReusableJsonWriter
{
    [ThreadStatic] private static Utf8JsonWriter? t_writer;
    // Returned writers point here so the cache never keeps a caller's (possibly large) output alive.
    private static readonly IBufferWriter<byte> Detached = new ArrayBufferWriter<byte>(1);

    public static Utf8JsonWriter Rent(IBufferWriter<byte> output)
    {
        Utf8JsonWriter? writer = t_writer;
        // A converter that serializes recursively on this thread finds the slot empty and gets its own writer.
        if (writer is null) return new Utf8JsonWriter(output);
        t_writer = null;
        writer.Reset(output);
        return writer;
    }

    public static void Return(Utf8JsonWriter writer)
    {
        // Reset also discards unflushed bytes left by a failed serialization.
        writer.Reset(Detached);
        t_writer = writer;
    }
}
