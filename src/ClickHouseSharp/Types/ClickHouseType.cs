namespace ClickHouseSharp.Types;

public abstract class ClickHouseType
{
    public abstract string Name { get; }
    public abstract Type ClrType { get; }
    public virtual bool IsNullable => false;

    public abstract ClickHouseValue CreateValue(object? value);
    public abstract ClickHouseValue DefaultValue { get; }

    public override string ToString() => Name;

    public static ClickHouseType Parse(string typeName) => TypeParser.Parse(typeName);
    public static ClickHouseType Parse(Parsing.Ast.DataTypeNode node) => TypeParser.Parse(node);
}

// Integer types
public sealed class Int8Type : ClickHouseType
{
    public static readonly Int8Type Instance = new();
    public override string Name => "Int8";
    public override Type ClrType => typeof(sbyte);
    public override ClickHouseValue CreateValue(object? value) => new Int8Value(Convert.ToSByte(value));
    public override ClickHouseValue DefaultValue => new Int8Value(0);
}

public sealed class Int16Type : ClickHouseType
{
    public static readonly Int16Type Instance = new();
    public override string Name => "Int16";
    public override Type ClrType => typeof(short);
    public override ClickHouseValue CreateValue(object? value) => new Int16Value(Convert.ToInt16(value));
    public override ClickHouseValue DefaultValue => new Int16Value(0);
}

public sealed class Int32Type : ClickHouseType
{
    public static readonly Int32Type Instance = new();
    public override string Name => "Int32";
    public override Type ClrType => typeof(int);
    public override ClickHouseValue CreateValue(object? value) => new Int32Value(Convert.ToInt32(value));
    public override ClickHouseValue DefaultValue => new Int32Value(0);
}

public sealed class Int64Type : ClickHouseType
{
    public static readonly Int64Type Instance = new();
    public override string Name => "Int64";
    public override Type ClrType => typeof(long);
    public override ClickHouseValue CreateValue(object? value) => new Int64Value(Convert.ToInt64(value));
    public override ClickHouseValue DefaultValue => new Int64Value(0);
}

public sealed class UInt8Type : ClickHouseType
{
    public static readonly UInt8Type Instance = new();
    public override string Name => "UInt8";
    public override Type ClrType => typeof(byte);
    public override ClickHouseValue CreateValue(object? value) => new UInt8Value(Convert.ToByte(value));
    public override ClickHouseValue DefaultValue => new UInt8Value(0);
}

public sealed class UInt16Type : ClickHouseType
{
    public static readonly UInt16Type Instance = new();
    public override string Name => "UInt16";
    public override Type ClrType => typeof(ushort);
    public override ClickHouseValue CreateValue(object? value) => new UInt16Value(Convert.ToUInt16(value));
    public override ClickHouseValue DefaultValue => new UInt16Value(0);
}

public sealed class UInt32Type : ClickHouseType
{
    public static readonly UInt32Type Instance = new();
    public override string Name => "UInt32";
    public override Type ClrType => typeof(uint);
    public override ClickHouseValue CreateValue(object? value) => new UInt32Value(Convert.ToUInt32(value));
    public override ClickHouseValue DefaultValue => new UInt32Value(0);
}

public sealed class UInt64Type : ClickHouseType
{
    public static readonly UInt64Type Instance = new();
    public override string Name => "UInt64";
    public override Type ClrType => typeof(ulong);
    public override ClickHouseValue CreateValue(object? value) => new UInt64Value(Convert.ToUInt64(value));
    public override ClickHouseValue DefaultValue => new UInt64Value(0);
}

// Float types
public sealed class Float32Type : ClickHouseType
{
    public static readonly Float32Type Instance = new();
    public override string Name => "Float32";
    public override Type ClrType => typeof(float);
    public override ClickHouseValue CreateValue(object? value) => new Float32Value(Convert.ToSingle(value));
    public override ClickHouseValue DefaultValue => new Float32Value(0);
}

public sealed class Float64Type : ClickHouseType
{
    public static readonly Float64Type Instance = new();
    public override string Name => "Float64";
    public override Type ClrType => typeof(double);
    public override ClickHouseValue CreateValue(object? value) => new Float64Value(Convert.ToDouble(value));
    public override ClickHouseValue DefaultValue => new Float64Value(0);
}

// Decimal type
public sealed class DecimalType : ClickHouseType
{
    public int Precision { get; }
    public int Scale { get; }

    public DecimalType(int precision, int scale)
    {
        Precision = precision;
        Scale = scale;
    }

    public override string Name => $"Decimal({Precision}, {Scale})";
    public override Type ClrType => typeof(decimal);
    public override ClickHouseValue CreateValue(object? value) => new DecimalValue(Convert.ToDecimal(value), this);
    public override ClickHouseValue DefaultValue => new DecimalValue(0, this);
}

// String types
public sealed class StringType : ClickHouseType
{
    public static readonly StringType Instance = new();
    public override string Name => "String";
    public override Type ClrType => typeof(string);
    public override ClickHouseValue CreateValue(object? value) => new StringValue(value?.ToString() ?? "");
    public override ClickHouseValue DefaultValue => new StringValue("");
}

public sealed class FixedStringType : ClickHouseType
{
    public int Length { get; }

    public FixedStringType(int length) => Length = length;

    public override string Name => $"FixedString({Length})";
    public override Type ClrType => typeof(string);
    public override ClickHouseValue CreateValue(object? value)
    {
        var s = value?.ToString() ?? "";
        if (s.Length > Length) s = s[..Length];
        else if (s.Length < Length) s = s.PadRight(Length, '\0');
        return new StringValue(s);
    }
    public override ClickHouseValue DefaultValue => new StringValue(new string('\0', Length));
}

// Boolean type
public sealed class BoolType : ClickHouseType
{
    public static readonly BoolType Instance = new();
    public override string Name => "Bool";
    public override Type ClrType => typeof(bool);
    public override ClickHouseValue CreateValue(object? value) => new BoolValue(Convert.ToBoolean(value));
    public override ClickHouseValue DefaultValue => new BoolValue(false);
}

// Date/Time types
public sealed class DateType : ClickHouseType
{
    public static readonly DateType Instance = new();
    public override string Name => "Date";
    public override Type ClrType => typeof(DateOnly);
    public override ClickHouseValue CreateValue(object? value)
    {
        return value switch
        {
            DateOnly d => new DateValue(d),
            DateTime dt => new DateValue(DateOnly.FromDateTime(dt)),
            string s => new DateValue(DateOnly.Parse(s)),
            _ => throw new InvalidCastException($"Cannot convert {value?.GetType()} to Date")
        };
    }
    public override ClickHouseValue DefaultValue => new DateValue(new DateOnly(1970, 1, 1));
}

public sealed class DateTimeType : ClickHouseType
{
    public string? Timezone { get; }

    public DateTimeType(string? timezone = null) => Timezone = timezone;

    public static readonly DateTimeType Instance = new();
    public override string Name => Timezone != null ? $"DateTime('{Timezone}')" : "DateTime";
    public override Type ClrType => typeof(DateTime);
    public override ClickHouseValue CreateValue(object? value)
    {
        return value switch
        {
            DateTime dt => new DateTimeValue(dt),
            DateOnly d => new DateTimeValue(d.ToDateTime(TimeOnly.MinValue)),
            string s => new DateTimeValue(DateTime.Parse(s)),
            long l => new DateTimeValue(DateTimeOffset.FromUnixTimeSeconds(l).DateTime),
            _ => throw new InvalidCastException($"Cannot convert {value?.GetType()} to DateTime")
        };
    }
    public override ClickHouseValue DefaultValue => new DateTimeValue(new DateTime(1970, 1, 1));
}

public sealed class DateTime64Type : ClickHouseType
{
    public int Precision { get; }
    public string? Timezone { get; }

    public DateTime64Type(int precision = 3, string? timezone = null)
    {
        Precision = precision;
        Timezone = timezone;
    }

    public override string Name => Timezone != null ? $"DateTime64({Precision}, '{Timezone}')" : $"DateTime64({Precision})";
    public override Type ClrType => typeof(DateTime);
    public override ClickHouseValue CreateValue(object? value) => DateTimeType.Instance.CreateValue(value);
    public override ClickHouseValue DefaultValue => new DateTimeValue(new DateTime(1970, 1, 1));
}

// UUID type
public sealed class UuidType : ClickHouseType
{
    public static readonly UuidType Instance = new();
    public override string Name => "UUID";
    public override Type ClrType => typeof(Guid);
    public override ClickHouseValue CreateValue(object? value)
    {
        return value switch
        {
            Guid g => new UuidValue(g),
            string s => new UuidValue(Guid.Parse(s)),
            _ => throw new InvalidCastException($"Cannot convert {value?.GetType()} to UUID")
        };
    }
    public override ClickHouseValue DefaultValue => new UuidValue(Guid.Empty);
}

// Nullable type
public sealed class NullableType : ClickHouseType
{
    public ClickHouseType InnerType { get; }

    public NullableType(ClickHouseType innerType) => InnerType = innerType;

    public override string Name => $"Nullable({InnerType.Name})";
    public override Type ClrType => typeof(Nullable<>).MakeGenericType(InnerType.ClrType);
    public override bool IsNullable => true;
    public override ClickHouseValue CreateValue(object? value) =>
        value == null ? NullValue.Instance : InnerType.CreateValue(value);
    public override ClickHouseValue DefaultValue => NullValue.Instance;
}

// Array type
public sealed class ArrayType : ClickHouseType
{
    public ClickHouseType ElementType { get; }

    public ArrayType(ClickHouseType elementType) => ElementType = elementType;

    public override string Name => $"Array({ElementType.Name})";
    public override Type ClrType => typeof(List<>).MakeGenericType(ElementType.ClrType);
    public override ClickHouseValue CreateValue(object? value)
    {
        if (value is IEnumerable<object> enumerable)
        {
            var elements = enumerable.Select(e => ElementType.CreateValue(e)).ToList();
            return new ArrayValue(elements, this);
        }
        throw new InvalidCastException($"Cannot convert {value?.GetType()} to Array");
    }
    public override ClickHouseValue DefaultValue => new ArrayValue([], this);
}

// Tuple type
public sealed class TupleType : ClickHouseType
{
    public IReadOnlyList<ClickHouseType> ElementTypes { get; }
    public IReadOnlyList<string>? ElementNames { get; }

    public TupleType(IReadOnlyList<ClickHouseType> elementTypes, IReadOnlyList<string>? elementNames = null)
    {
        ElementTypes = elementTypes;
        ElementNames = elementNames;
    }

    public override string Name
    {
        get
        {
            var elements = ElementTypes.Select((t, i) =>
                ElementNames != null && i < ElementNames.Count ? $"{ElementNames[i]} {t.Name}" : t.Name);
            return $"Tuple({string.Join(", ", elements)})";
        }
    }

    public override Type ClrType => typeof(object[]);
    public override ClickHouseValue CreateValue(object? value)
    {
        if (value is IEnumerable<object> enumerable)
        {
            var elements = enumerable.Zip(ElementTypes, (v, t) => t.CreateValue(v)).ToList();
            return new TupleValue(elements, this);
        }
        throw new InvalidCastException($"Cannot convert {value?.GetType()} to Tuple");
    }
    public override ClickHouseValue DefaultValue =>
        new TupleValue(ElementTypes.Select(t => t.DefaultValue).ToList(), this);
}

// Map type
public sealed class MapType : ClickHouseType
{
    public ClickHouseType KeyType { get; }
    public ClickHouseType ValueType { get; }

    public MapType(ClickHouseType keyType, ClickHouseType valueType)
    {
        KeyType = keyType;
        ValueType = valueType;
    }

    public override string Name => $"Map({KeyType.Name}, {ValueType.Name})";
    public override Type ClrType => typeof(Dictionary<,>).MakeGenericType(KeyType.ClrType, ValueType.ClrType);
    public override ClickHouseValue CreateValue(object? value)
    {
        if (value is IDictionary<object, object> dict)
        {
            var map = dict.ToDictionary(
                kv => KeyType.CreateValue(kv.Key),
                kv => ValueType.CreateValue(kv.Value));
            return new MapValue(map, this);
        }
        throw new InvalidCastException($"Cannot convert {value?.GetType()} to Map");
    }
    public override ClickHouseValue DefaultValue => new MapValue(new Dictionary<ClickHouseValue, ClickHouseValue>(), this);
}

// LowCardinality type (wraps another type but stores efficiently)
public sealed class LowCardinalityType : ClickHouseType
{
    public ClickHouseType InnerType { get; }

    public LowCardinalityType(ClickHouseType innerType) => InnerType = innerType;

    public override string Name => $"LowCardinality({InnerType.Name})";
    public override Type ClrType => InnerType.ClrType;
    public override ClickHouseValue CreateValue(object? value) => InnerType.CreateValue(value);
    public override ClickHouseValue DefaultValue => InnerType.DefaultValue;
}
