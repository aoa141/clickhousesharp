using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class MapFunctions
{
    public class MapFunction : ScalarFunction
    {
        public override string Name => "map";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            // map(k1, v1, k2, v2, ...)
            var entries = new Dictionary<ClickHouseValue, ClickHouseValue>();
            ClickHouseType? keyType = null;
            ClickHouseType? valueType = null;

            for (int i = 0; i < args.Count - 1; i += 2)
            {
                var key = args[i];
                var value = args[i + 1];
                entries[key] = value;
                keyType ??= key.Type;
                valueType ??= value.Type;
            }

            return new MapValue(entries, new MapType(keyType ?? StringType.Instance, valueType ?? Int64Type.Instance));
        }
    }

    public class MapKeysFunction : ScalarFunction
    {
        public override string Name => "mapKeys";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not MapValue map) throw new InvalidOperationException("Expected map");
            var mapType = (MapType)map.Type;
            return new ArrayValue(map.Entries.Keys.ToList(), new ArrayType(mapType.KeyType));
        }
    }

    public class MapValuesFunction : ScalarFunction
    {
        public override string Name => "mapValues";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not MapValue map) throw new InvalidOperationException("Expected map");
            var mapType = (MapType)map.Type;
            return new ArrayValue(map.Entries.Values.ToList(), new ArrayType(mapType.ValueType));
        }
    }
}
