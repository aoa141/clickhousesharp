using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class TypeFunctions
{
    public class ToInt8Function : ScalarFunction
    {
        public override string Name => "toInt8";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Int8Value(Convert.ToSByte(args[0].RawValue));
        }
    }

    public class ToInt16Function : ScalarFunction
    {
        public override string Name => "toInt16";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Int16Value(Convert.ToInt16(args[0].RawValue));
        }
    }

    public class ToInt32Function : ScalarFunction
    {
        public override string Name => "toInt32";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Int32Value(Convert.ToInt32(args[0].RawValue));
        }
    }

    public class ToInt64Function : ScalarFunction
    {
        public override string Name => "toInt64";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Int64Value(Convert.ToInt64(args[0].RawValue));
        }
    }

    public class ToUInt8Function : ScalarFunction
    {
        public override string Name => "toUInt8";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new UInt8Value(Convert.ToByte(args[0].RawValue));
        }
    }

    public class ToUInt16Function : ScalarFunction
    {
        public override string Name => "toUInt16";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new UInt16Value(Convert.ToUInt16(args[0].RawValue));
        }
    }

    public class ToUInt32Function : ScalarFunction
    {
        public override string Name => "toUInt32";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new UInt32Value(Convert.ToUInt32(args[0].RawValue));
        }
    }

    public class ToUInt64Function : ScalarFunction
    {
        public override string Name => "toUInt64";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new UInt64Value(Convert.ToUInt64(args[0].RawValue));
        }
    }

    public class ToFloat32Function : ScalarFunction
    {
        public override string Name => "toFloat32";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float32Value(Convert.ToSingle(args[0].RawValue));
        }
    }

    public class ToFloat64Function : ScalarFunction
    {
        public override string Name => "toFloat64";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Convert.ToDouble(args[0].RawValue));
        }
    }

    public class ToDecimalFunction : ScalarFunction
    {
        public override string Name => "toDecimal";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var precision = args.Count > 1 ? (int)args[1].AsInt64() : 10;
            var scale = args.Count > 2 ? (int)args[2].AsInt64() : 0;
            return new DecimalValue(Convert.ToDecimal(args[0].RawValue), new DecimalType(precision, scale));
        }
    }
}
