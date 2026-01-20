using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class ArrayFunctions
{
    public class ArrayFunction : ScalarFunction
    {
        public override string Name => "array";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            var elementType = args.FirstOrDefault(a => !a.IsNull)?.Type ?? Int64Type.Instance;
            return new ArrayValue(args.ToList(), new ArrayType(elementType));
        }
    }

    public class ArrayLengthFunction : ScalarFunction
    {
        public override string Name => "arrayLength";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            return new Int64Value(arr.Elements.Count);
        }
    }

    public class ArrayElementFunction : ScalarFunction
    {
        public override string Name => "arrayElement";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            var index = (int)args[1].AsInt64();
            // 1-based indexing
            if (index < 1 || index > arr.Elements.Count) return NullValue.Instance;
            return arr.Elements[index - 1];
        }
    }

    public class HasFunction : ScalarFunction
    {
        public override string Name => "has";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            return new BoolValue(arr.Elements.Any(e => e.Equals(args[1])));
        }
    }

    public class IndexOfFunction : ScalarFunction
    {
        public override string Name => "indexOf";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            for (int i = 0; i < arr.Elements.Count; i++)
            {
                if (arr.Elements[i].Equals(args[1]))
                    return new Int64Value(i + 1); // 1-based
            }
            return new Int64Value(0);
        }
    }

    public class ArrayConcatFunction : ScalarFunction
    {
        public override string Name => "arrayConcat";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            var result = new List<ClickHouseValue>();
            ClickHouseType? elementType = null;

            foreach (var arg in args)
            {
                if (arg.IsNull) continue;
                if (arg is not ArrayValue arr) throw new InvalidOperationException("Expected array");
                result.AddRange(arr.Elements);
                elementType ??= ((ArrayType)arr.Type).ElementType;
            }

            return new ArrayValue(result, new ArrayType(elementType ?? Int64Type.Instance));
        }
    }

    public class ArrayPushBackFunction : ScalarFunction
    {
        public override string Name => "arrayPushBack";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            var result = arr.Elements.ToList();
            result.Add(args[1]);
            return new ArrayValue(result, (ArrayType)arr.Type);
        }
    }

    public class ArrayPushFrontFunction : ScalarFunction
    {
        public override string Name => "arrayPushFront";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            var result = new List<ClickHouseValue> { args[1] };
            result.AddRange(arr.Elements);
            return new ArrayValue(result, (ArrayType)arr.Type);
        }
    }

    public class ArrayPopBackFunction : ScalarFunction
    {
        public override string Name => "arrayPopBack";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            if (arr.Elements.Count == 0) return arr;
            return new ArrayValue(arr.Elements.Take(arr.Elements.Count - 1).ToList(), (ArrayType)arr.Type);
        }
    }

    public class ArrayPopFrontFunction : ScalarFunction
    {
        public override string Name => "arrayPopFront";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            if (arr.Elements.Count == 0) return arr;
            return new ArrayValue(arr.Elements.Skip(1).ToList(), (ArrayType)arr.Type);
        }
    }

    public class ArraySliceFunction : ScalarFunction
    {
        public override string Name => "arraySlice";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");

            var offset = (int)args[1].AsInt64();
            var length = args.Count > 2 && !args[2].IsNull ? (int)args[2].AsInt64() : arr.Elements.Count;

            // Handle negative offset (from end)
            var start = offset >= 0 ? offset - 1 : arr.Elements.Count + offset;
            start = Math.Max(0, Math.Min(start, arr.Elements.Count));
            length = Math.Max(0, Math.Min(length, arr.Elements.Count - start));

            return new ArrayValue(arr.Elements.Skip(start).Take(length).ToList(), (ArrayType)arr.Type);
        }
    }

    public class ArrayReverseFunction : ScalarFunction
    {
        public override string Name => "arrayReverse";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            return new ArrayValue(arr.Elements.Reverse().ToList(), (ArrayType)arr.Type);
        }
    }

    public class ArrayDistinctFunction : ScalarFunction
    {
        public override string Name => "arrayDistinct";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            var unique = new List<ClickHouseValue>();
            var seen = new HashSet<ClickHouseValue>();
            foreach (var elem in arr.Elements)
            {
                if (seen.Add(elem))
                    unique.Add(elem);
            }
            return new ArrayValue(unique, (ArrayType)arr.Type);
        }
    }

    public class ArraySortFunction : ScalarFunction
    {
        public override string Name => "arraySort";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            var sorted = arr.Elements.OrderBy(x => x).ToList();
            return new ArrayValue(sorted, (ArrayType)arr.Type);
        }
    }

    public class EmptyArrayToSingleFunction : ScalarFunction
    {
        public override string Name => "emptyArrayToSingle";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array");
            if (arr.Elements.Count > 0) return arr;
            var elementType = ((ArrayType)arr.Type).ElementType;
            return new ArrayValue([elementType.DefaultValue], (ArrayType)arr.Type);
        }
    }

    public class RangeFunction : ScalarFunction
    {
        public override string Name => "range";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;

            long start, end, step;
            if (args.Count == 1)
            {
                start = 0;
                end = args[0].AsInt64();
                step = 1;
            }
            else if (args.Count == 2)
            {
                start = args[0].AsInt64();
                end = args[1].AsInt64();
                step = 1;
            }
            else
            {
                start = args[0].AsInt64();
                end = args[1].AsInt64();
                step = args[2].AsInt64();
            }

            var result = new List<ClickHouseValue>();
            if (step > 0)
            {
                for (var i = start; i < end; i += step)
                    result.Add(new Int64Value(i));
            }
            else if (step < 0)
            {
                for (var i = start; i > end; i += step)
                    result.Add(new Int64Value(i));
            }

            return new ArrayValue(result, new ArrayType(Int64Type.Instance));
        }
    }
}
