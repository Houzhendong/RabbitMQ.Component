using System.Collections;
using System.Text;
using RabbitMQ.Client;

namespace RabbitMQ.Component.Internal;

/// <summary>Immutable, structurally comparable field table. Every export is a deep, client-compatible copy.</summary>
internal sealed class AmqpTable : IEquatable<AmqpTable>
{
    private readonly Dictionary<string, object?> _values;
    private AmqpTable(Dictionary<string, object?> values) => _values = values;
    public static AmqpTable Empty { get; } = new(new(StringComparer.Ordinal));
    public int Count => _values.Count;

    public static AmqpTable Capture(IDictionary<string, object?>? values) => values is null
        ? Empty : new(CloneTable(values, new(ReferenceEqualityComparer.Instance), 0));

    public Dictionary<string, object?> ToDictionary() => CloneTable(_values, new(ReferenceEqualityComparer.Instance), 0);
    public bool Equals(AmqpTable? other) => other is not null && TableEquals(_values, other._values);
    public override bool Equals(object? obj) => obj is AmqpTable other && Equals(other);
    public override int GetHashCode() => ValueHash(_values);

    private static Dictionary<string, object?> CloneTable(IDictionary<string, object?> source, HashSet<object> path, int depth)
    {
        Enter(source, path, depth);
        try
        {
            var result = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var (key, value) in source)
            {
                ValidateShortString(key, "AMQP field name", allowEmpty: false);
                result.Add(key, CloneValue(value, path, depth + 1));
            }
            return result;
        }
        finally { path.Remove(source); }
    }

    private static object? CloneValue(object? value, HashSet<object> path, int depth)
    {
        switch (value)
        {
            case null or string or bool or byte or sbyte or short or ushort or int or uint or long or float or double or AmqpTimestamp:
                return value;
            case decimal number:
                int[] bits = decimal.GetBits(number);
                if (bits[1] != 0 || bits[2] != 0 || bits[0] < 0)
                    throw new ArgumentException("AMQP decimal mantissa must fit in 31 bits.");
                return number;
            case byte[] bytes: return bytes.ToArray();
            case BinaryTableValue binary: return new BinaryTableValue(binary.Bytes.ToArray());
            case IDictionary<string, object?> table: return CloneTable(table, path, depth);
            case IDictionary table:
                Enter(table, path, depth);
                try
                {
                    var copy = new Dictionary<string, object?>(StringComparer.Ordinal);
                    foreach (DictionaryEntry item in table)
                    {
                        if (item.Key is not string key) throw new ArgumentException("AMQP field names must be strings.");
                        ValidateShortString(key, "AMQP field name", allowEmpty: false);
                        copy.Add(key, CloneValue(item.Value, path, depth + 1));
                    }
                    return copy;
                }
                finally { path.Remove(table); }
            case IList list:
                Enter(list, path, depth);
                try
                {
                    var copy = new List<object?>(list.Count);
                    foreach (object? item in list) copy.Add(CloneValue(item, path, depth + 1));
                    return copy;
                }
                finally { path.Remove(list); }
            default: throw new ArgumentException($"Unsupported AMQP field value type: {value.GetType().FullName}.");
        }
    }

    private static void Enter(object value, HashSet<object> path, int depth)
    {
        if (depth > 64) throw new ArgumentException("AMQP field nesting exceeds 64 levels.");
        if (!path.Add(value)) throw new ArgumentException("AMQP field tables cannot contain cycles.");
    }

    internal static void ValidateShortString(string? value, string name, bool allowEmpty = true)
    {
        if (value is null || (!allowEmpty && value.Length == 0) || value.Contains('\0') || Encoding.UTF8.GetByteCount(value) > 255)
            throw new ArgumentException($"{name} must be {(allowEmpty ? "0" : "1")}-255 UTF-8 bytes without NUL.", name);
    }

    private static bool TableEquals(Dictionary<string, object?> left, Dictionary<string, object?> right) =>
        left.Count == right.Count && left.All(entry => right.TryGetValue(entry.Key, out var value) && ValueEquals(entry.Value, value));

    private static bool ValueEquals(object? left, object? right)
    {
        if (left is null || right is null) return left is null && right is null;
        if (left.GetType() != right.GetType()) return false;
        return left switch
        {
            byte[] bytes => bytes.AsSpan().SequenceEqual((byte[])right),
            BinaryTableValue bytes => bytes.Bytes.AsSpan().SequenceEqual(((BinaryTableValue)right).Bytes),
            Dictionary<string, object?> table => TableEquals(table, (Dictionary<string, object?>)right),
            List<object?> list => list.Count == ((List<object?>)right).Count && list.Zip((List<object?>)right).All(pair => ValueEquals(pair.First, pair.Second)),
            _ => left.Equals(right)
        };
    }

    private static int ValueHash(object? value)
    {
        if (value is null) return 0;
        var hash = new HashCode();
        hash.Add(value.GetType());
        switch (value)
        {
            case byte[] bytes:
                foreach (byte item in bytes) hash.Add(item);
                break;
            case BinaryTableValue binary:
                foreach (byte item in binary.Bytes) hash.Add(item);
                break;
            case Dictionary<string, object?> table:
                foreach (var pair in table.OrderBy(pair => pair.Key, StringComparer.Ordinal))
                { hash.Add(pair.Key, StringComparer.Ordinal); hash.Add(ValueHash(pair.Value)); }
                break;
            case List<object?> list:
                foreach (object? item in list) hash.Add(ValueHash(item));
                break;
            default: hash.Add(value); break;
        }
        return hash.ToHashCode();
    }
}
