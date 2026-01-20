namespace ClickHouseSharp.Types;

public abstract class ClickHouseValue : IComparable<ClickHouseValue>, IEquatable<ClickHouseValue>
{
    public abstract ClickHouseType Type { get; }
    public abstract object? RawValue { get; }
    public virtual bool IsNull => false;

    public abstract int CompareTo(ClickHouseValue? other);
    public abstract bool Equals(ClickHouseValue? other);
    public abstract override int GetHashCode();

    public override bool Equals(object? obj) => obj is ClickHouseValue other && Equals(other);
    public override string ToString() => RawValue?.ToString() ?? "NULL";

    public static bool operator ==(ClickHouseValue? left, ClickHouseValue? right) =>
        left is null ? right is null : left.Equals(right);
    public static bool operator !=(ClickHouseValue? left, ClickHouseValue? right) => !(left == right);
    public static bool operator <(ClickHouseValue left, ClickHouseValue right) => left.CompareTo(right) < 0;
    public static bool operator <=(ClickHouseValue left, ClickHouseValue right) => left.CompareTo(right) <= 0;
    public static bool operator >(ClickHouseValue left, ClickHouseValue right) => left.CompareTo(right) > 0;
    public static bool operator >=(ClickHouseValue left, ClickHouseValue right) => left.CompareTo(right) >= 0;

    public T As<T>() => (T)RawValue!;
    public T? AsNullable<T>() where T : struct => IsNull ? null : (T)RawValue!;

    public long AsInt64() => Convert.ToInt64(RawValue);
    public double AsFloat64() => Convert.ToDouble(RawValue);
    public string AsString() => RawValue?.ToString() ?? "";
    public bool AsBool() => Convert.ToBoolean(RawValue);
}

public sealed class NullValue : ClickHouseValue
{
    public static readonly NullValue Instance = new();
    private NullValue() { }

    public override ClickHouseType Type => NullableType.Instance;
    public override object? RawValue => null;
    public override bool IsNull => true;

    public override int CompareTo(ClickHouseValue? other) =>
        other?.IsNull == true ? 0 : -1; // NULLs sort first
    public override bool Equals(ClickHouseValue? other) => other?.IsNull == true;
    public override int GetHashCode() => 0;
    public override string ToString() => "NULL";

    // Helper type for null values
    private sealed class NullableTypeInstance : ClickHouseType
    {
        public static readonly NullableTypeInstance Instance = new();
        public override string Name => "Nullable(Nothing)";
        public override Type ClrType => typeof(object);
        public override bool IsNullable => true;
        public override ClickHouseValue CreateValue(object? value) => NullValue.Instance;
        public override ClickHouseValue DefaultValue => NullValue.Instance;
    }

    private static class NullableType
    {
        public static readonly NullableTypeInstance Instance = NullableTypeInstance.Instance;
    }
}

public sealed class Int8Value(sbyte value) : ClickHouseValue
{
    public sbyte Value { get; } = value;
    public override ClickHouseType Type => Int8Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        Int8Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToSByte(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is Int8Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class Int16Value(short value) : ClickHouseValue
{
    public short Value { get; } = value;
    public override ClickHouseType Type => Int16Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        Int16Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToInt16(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is Int16Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class Int32Value(int value) : ClickHouseValue
{
    public int Value { get; } = value;
    public override ClickHouseType Type => Int32Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        Int32Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToInt32(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is Int32Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class Int64Value(long value) : ClickHouseValue
{
    public long Value { get; } = value;
    public override ClickHouseType Type => Int64Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        Int64Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToInt64(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is Int64Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class UInt8Value(byte value) : ClickHouseValue
{
    public byte Value { get; } = value;
    public override ClickHouseType Type => UInt8Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        UInt8Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToByte(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is UInt8Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class UInt16Value(ushort value) : ClickHouseValue
{
    public ushort Value { get; } = value;
    public override ClickHouseType Type => UInt16Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        UInt16Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToUInt16(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is UInt16Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class UInt32Value(uint value) : ClickHouseValue
{
    public uint Value { get; } = value;
    public override ClickHouseType Type => UInt32Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        UInt32Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToUInt32(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is UInt32Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class UInt64Value(ulong value) : ClickHouseValue
{
    public ulong Value { get; } = value;
    public override ClickHouseType Type => UInt64Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        UInt64Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToUInt64(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is UInt64Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class Float32Value(float value) : ClickHouseValue
{
    public float Value { get; } = value;
    public override ClickHouseType Type => Float32Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        Float32Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToSingle(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is Float32Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class Float64Value(double value) : ClickHouseValue
{
    public double Value { get; } = value;
    public override ClickHouseType Type => Float64Type.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        Float64Value v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToDouble(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is Float64Value v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class DecimalValue(decimal value, DecimalType type) : ClickHouseValue
{
    public decimal Value { get; } = value;
    private readonly DecimalType _type = type;
    public override ClickHouseType Type => _type;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        DecimalValue v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToDecimal(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is DecimalValue v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
}

public sealed class StringValue(string value) : ClickHouseValue
{
    public string Value { get; } = value;
    public override ClickHouseType Type => StringType.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        StringValue v => string.Compare(Value, v.Value, StringComparison.Ordinal),
        _ => string.Compare(Value, other.RawValue?.ToString(), StringComparison.Ordinal)
    };
    public override bool Equals(ClickHouseValue? other) => other is StringValue v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => $"'{Value}'";
}

public sealed class BoolValue(bool value) : ClickHouseValue
{
    public bool Value { get; } = value;
    public override ClickHouseType Type => BoolType.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        BoolValue v => Value.CompareTo(v.Value),
        _ => Value.CompareTo(Convert.ToBoolean(other.RawValue))
    };
    public override bool Equals(ClickHouseValue? other) => other is BoolValue v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => Value ? "true" : "false";
}

public sealed class DateValue(DateOnly value) : ClickHouseValue
{
    public DateOnly Value { get; } = value;
    public override ClickHouseType Type => DateType.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        DateValue v => Value.CompareTo(v.Value),
        DateTimeValue v => Value.ToDateTime(TimeOnly.MinValue).CompareTo(v.Value),
        _ => throw new InvalidOperationException($"Cannot compare Date with {other.Type}")
    };
    public override bool Equals(ClickHouseValue? other) => other is DateValue v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => $"'{Value:yyyy-MM-dd}'";
}

public sealed class DateTimeValue(DateTime value) : ClickHouseValue
{
    public DateTime Value { get; } = value;
    public override ClickHouseType Type => DateTimeType.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        DateTimeValue v => Value.CompareTo(v.Value),
        DateValue v => Value.CompareTo(v.Value.ToDateTime(TimeOnly.MinValue)),
        _ => throw new InvalidOperationException($"Cannot compare DateTime with {other.Type}")
    };
    public override bool Equals(ClickHouseValue? other) => other is DateTimeValue v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => $"'{Value:yyyy-MM-dd HH:mm:ss}'";
}

public sealed class UuidValue(Guid value) : ClickHouseValue
{
    public Guid Value { get; } = value;
    public override ClickHouseType Type => UuidType.Instance;
    public override object RawValue => Value;
    public override int CompareTo(ClickHouseValue? other) => other switch
    {
        null or { IsNull: true } => 1,
        UuidValue v => Value.CompareTo(v.Value),
        _ => throw new InvalidOperationException($"Cannot compare UUID with {other.Type}")
    };
    public override bool Equals(ClickHouseValue? other) => other is UuidValue v && Value == v.Value;
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => $"'{Value}'";
}

public sealed class ArrayValue(IReadOnlyList<ClickHouseValue> elements, ArrayType type) : ClickHouseValue
{
    public IReadOnlyList<ClickHouseValue> Elements { get; } = elements;
    private readonly ArrayType _type = type;
    public override ClickHouseType Type => _type;
    public override object RawValue => Elements;

    public override int CompareTo(ClickHouseValue? other)
    {
        if (other is null or { IsNull: true }) return 1;
        if (other is not ArrayValue arr) throw new InvalidOperationException($"Cannot compare Array with {other.Type}");

        for (int i = 0; i < Math.Min(Elements.Count, arr.Elements.Count); i++)
        {
            var cmp = Elements[i].CompareTo(arr.Elements[i]);
            if (cmp != 0) return cmp;
        }
        return Elements.Count.CompareTo(arr.Elements.Count);
    }

    public override bool Equals(ClickHouseValue? other) =>
        other is ArrayValue arr && Elements.SequenceEqual(arr.Elements);
    public override int GetHashCode() => Elements.Aggregate(0, (h, e) => HashCode.Combine(h, e.GetHashCode()));
    public override string ToString() => $"[{string.Join(", ", Elements)}]";
}

public sealed class TupleValue(IReadOnlyList<ClickHouseValue> elements, TupleType type) : ClickHouseValue
{
    public IReadOnlyList<ClickHouseValue> Elements { get; } = elements;
    private readonly TupleType _type = type;
    public override ClickHouseType Type => _type;
    public override object RawValue => Elements;

    public override int CompareTo(ClickHouseValue? other)
    {
        if (other is null or { IsNull: true }) return 1;
        if (other is not TupleValue tup) throw new InvalidOperationException($"Cannot compare Tuple with {other.Type}");

        for (int i = 0; i < Math.Min(Elements.Count, tup.Elements.Count); i++)
        {
            var cmp = Elements[i].CompareTo(tup.Elements[i]);
            if (cmp != 0) return cmp;
        }
        return Elements.Count.CompareTo(tup.Elements.Count);
    }

    public override bool Equals(ClickHouseValue? other) =>
        other is TupleValue tup && Elements.SequenceEqual(tup.Elements);
    public override int GetHashCode() => Elements.Aggregate(0, (h, e) => HashCode.Combine(h, e.GetHashCode()));
    public override string ToString() => $"({string.Join(", ", Elements)})";
}

public sealed class MapValue(Dictionary<ClickHouseValue, ClickHouseValue> entries, MapType type) : ClickHouseValue
{
    public IReadOnlyDictionary<ClickHouseValue, ClickHouseValue> Entries { get; } = entries;
    private readonly MapType _type = type;
    public override ClickHouseType Type => _type;
    public override object RawValue => Entries;

    public override int CompareTo(ClickHouseValue? other) =>
        throw new InvalidOperationException("Maps cannot be compared");
    public override bool Equals(ClickHouseValue? other) =>
        other is MapValue map && Entries.Count == map.Entries.Count &&
        Entries.All(kv => map.Entries.TryGetValue(kv.Key, out var v) && kv.Value.Equals(v));
    public override int GetHashCode() => Entries.Aggregate(0, (h, kv) => HashCode.Combine(h, kv.Key.GetHashCode(), kv.Value.GetHashCode()));
    public override string ToString() => $"{{{string.Join(", ", Entries.Select(kv => $"{kv.Key}: {kv.Value}"))}}}";
}
