using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class TupleFunctions
{
    public class TupleFunction : ScalarFunction
    {
        public override string Name => "tuple";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            var types = args.Select(a => a.Type).ToList();
            return new TupleValue(args.ToList(), new TupleType(types));
        }
    }

    public class TupleElementFunction : ScalarFunction
    {
        public override string Name => "tupleElement";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not TupleValue tup) throw new InvalidOperationException("Expected tuple");

            if (args[1] is StringValue nameVal)
            {
                // Access by name
                var tupleType = (TupleType)tup.Type;
                if (tupleType.ElementNames != null)
                {
                    var idx = tupleType.ElementNames.ToList().FindIndex(n =>
                        n.Equals(nameVal.Value, StringComparison.OrdinalIgnoreCase));
                    if (idx >= 0 && idx < tup.Elements.Count)
                        return tup.Elements[idx];
                }
                return NullValue.Instance;
            }
            else
            {
                // Access by index (1-based)
                var idx = (int)args[1].AsInt64() - 1;
                if (idx >= 0 && idx < tup.Elements.Count)
                    return tup.Elements[idx];
                return NullValue.Instance;
            }
        }
    }
}
