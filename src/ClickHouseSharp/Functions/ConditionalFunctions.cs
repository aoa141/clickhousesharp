using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class ConditionalFunctions
{
    public class IfFunction : ScalarFunction
    {
        public override string Name => "if";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            var condition = args[0];
            if (condition.IsNull)
                return args.Count > 2 ? args[2] : NullValue.Instance;
            return condition.AsBool() ? args[1] : (args.Count > 2 ? args[2] : NullValue.Instance);
        }
    }

    public class IfNullFunction : ScalarFunction
    {
        public override string Name => "ifNull";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return args[0].IsNull ? args[1] : args[0];
        }
    }

    public class NullIfFunction : ScalarFunction
    {
        public override string Name => "nullIf";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return args[0];
            return args[0].Equals(args[1]) ? NullValue.Instance : args[0];
        }
    }

    public class CoalesceFunction : ScalarFunction
    {
        public override string Name => "coalesce";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            foreach (var arg in args)
            {
                if (!arg.IsNull)
                    return arg;
            }
            return NullValue.Instance;
        }
    }

    public class MultiIfFunction : ScalarFunction
    {
        public override string Name => "multiIf";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            // multiIf(cond1, then1, cond2, then2, ..., else)
            for (int i = 0; i < args.Count - 1; i += 2)
            {
                var condition = args[i];
                if (!condition.IsNull && condition.AsBool())
                    return args[i + 1];
            }
            // Return else value (last argument if odd count)
            return args.Count % 2 == 1 ? args[^1] : NullValue.Instance;
        }
    }

    public class AssumeNotNullFunction : ScalarFunction
    {
        public override string Name => "assumeNotNull";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            // Returns the value, assuming it's not null (used for type conversion)
            return args[0].IsNull ? args[0].Type.DefaultValue : args[0];
        }
    }
}
