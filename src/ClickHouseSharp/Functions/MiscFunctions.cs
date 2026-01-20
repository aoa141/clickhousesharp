using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class MiscFunctions
{
    public class RowNumberFunction : ScalarFunction
    {
        public override string Name => "rowNumberInAllBlocks";
        private long _counter;

        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new Int64Value(++_counter);
        }
    }

    public class RandFunction : ScalarFunction
    {
        public override string Name => "rand";
        private static readonly Random Rng = new();

        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new UInt32Value((uint)Rng.Next());
        }
    }

    public class Rand64Function : ScalarFunction
    {
        public override string Name => "rand64";
        private static readonly Random Rng = new();

        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            var bytes = new byte[8];
            Rng.NextBytes(bytes);
            return new UInt64Value(BitConverter.ToUInt64(bytes));
        }
    }

    public class GenerateUUIDv4Function : ScalarFunction
    {
        public override string Name => "generateUUIDv4";

        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new UuidValue(Guid.NewGuid());
        }
    }

    public class VersionFunction : ScalarFunction
    {
        public override string Name => "version";

        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new StringValue("24.1.0-ClickHouseSharp");
        }
    }

    public class TypeOfFunction : ScalarFunction
    {
        public override string Name => "toTypeName";

        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new StringValue(args[0].Type.Name);
        }
    }
}
