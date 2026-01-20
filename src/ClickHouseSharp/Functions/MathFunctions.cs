using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class MathFunctions
{
    public class AbsFunction : ScalarFunction
    {
        public override string Name => "abs";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var val = args[0].AsFloat64();
            return new Float64Value(Math.Abs(val));
        }
    }

    public class CeilFunction : ScalarFunction
    {
        public override string Name => "ceil";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Ceiling(args[0].AsFloat64()));
        }
    }

    public class FloorFunction : ScalarFunction
    {
        public override string Name => "floor";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Floor(args[0].AsFloat64()));
        }
    }

    public class RoundFunction : ScalarFunction
    {
        public override string Name => "round";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var precision = args.Count > 1 && !args[1].IsNull ? (int)args[1].AsInt64() : 0;
            return new Float64Value(Math.Round(args[0].AsFloat64(), precision, MidpointRounding.AwayFromZero));
        }
    }

    public class SqrtFunction : ScalarFunction
    {
        public override string Name => "sqrt";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Sqrt(args[0].AsFloat64()));
        }
    }

    public class PowerFunction : ScalarFunction
    {
        public override string Name => "pow";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Pow(args[0].AsFloat64(), args[1].AsFloat64()));
        }
    }

    public class ExpFunction : ScalarFunction
    {
        public override string Name => "exp";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Exp(args[0].AsFloat64()));
        }
    }

    public class LogFunction : ScalarFunction
    {
        public override string Name => "log";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Log(args[0].AsFloat64()));
        }
    }

    public class Log2Function : ScalarFunction
    {
        public override string Name => "log2";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Log2(args[0].AsFloat64()));
        }
    }

    public class Log10Function : ScalarFunction
    {
        public override string Name => "log10";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Log10(args[0].AsFloat64()));
        }
    }

    public class SinFunction : ScalarFunction
    {
        public override string Name => "sin";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Sin(args[0].AsFloat64()));
        }
    }

    public class CosFunction : ScalarFunction
    {
        public override string Name => "cos";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Cos(args[0].AsFloat64()));
        }
    }

    public class TanFunction : ScalarFunction
    {
        public override string Name => "tan";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Tan(args[0].AsFloat64()));
        }
    }

    public class AsinFunction : ScalarFunction
    {
        public override string Name => "asin";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Asin(args[0].AsFloat64()));
        }
    }

    public class AcosFunction : ScalarFunction
    {
        public override string Name => "acos";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Acos(args[0].AsFloat64()));
        }
    }

    public class AtanFunction : ScalarFunction
    {
        public override string Name => "atan";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Atan(args[0].AsFloat64()));
        }
    }

    public class Atan2Function : ScalarFunction
    {
        public override string Name => "atan2";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            return new Float64Value(Math.Atan2(args[0].AsFloat64(), args[1].AsFloat64()));
        }
    }

    public class PiFunction : ScalarFunction
    {
        public override string Name => "pi";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new Float64Value(Math.PI);
        }
    }

    public class EFunction : ScalarFunction
    {
        public override string Name => "e";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new Float64Value(Math.E);
        }
    }

    public class SignFunction : ScalarFunction
    {
        public override string Name => "sign";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Int64Value(Math.Sign(args[0].AsFloat64()));
        }
    }

    public class ModFunction : ScalarFunction
    {
        public override string Name => "modulo";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            return new Int64Value(args[0].AsInt64() % args[1].AsInt64());
        }
    }

    public class IntDivFunction : ScalarFunction
    {
        public override string Name => "intDiv";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            return new Int64Value(args[0].AsInt64() / args[1].AsInt64());
        }
    }

    public class GreatestFunction : ScalarFunction
    {
        public override string Name => "greatest";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            ClickHouseValue? max = null;
            foreach (var arg in args)
            {
                if (arg.IsNull) continue;
                if (max == null || arg.CompareTo(max) > 0)
                    max = arg;
            }
            return max ?? NullValue.Instance;
        }
    }

    public class LeastFunction : ScalarFunction
    {
        public override string Name => "least";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            ClickHouseValue? min = null;
            foreach (var arg in args)
            {
                if (arg.IsNull) continue;
                if (min == null || arg.CompareTo(min) < 0)
                    min = arg;
            }
            return min ?? NullValue.Instance;
        }
    }
}
