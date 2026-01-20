using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class AggregateFunctions
{
    public class CountFunction : AggregateFunction
    {
        public override string Name => "count";

        public override IAggregateState CreateState() => new CountState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (CountState)state;
            // count() counts all rows, count(x) counts non-null values
            if (args.Count == 0 || !args[0].IsNull)
                s.Count++;
        }

        public override ClickHouseValue Finalize(IAggregateState state) =>
            new Int64Value(((CountState)state).Count);

        private class CountState : IAggregateState
        {
            public long Count;
        }
    }

    public class SumFunction : AggregateFunction
    {
        public override string Name => "sum";

        public override IAggregateState CreateState() => new SumState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull) return;
            var s = (SumState)state;
            s.Sum += args[0].AsFloat64();
            s.HasValue = true;
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (SumState)state;
            return s.HasValue ? new Float64Value(s.Sum) : NullValue.Instance;
        }

        private class SumState : IAggregateState
        {
            public double Sum;
            public bool HasValue;
        }
    }

    public class AvgFunction : AggregateFunction
    {
        public override string Name => "avg";

        public override IAggregateState CreateState() => new AvgState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull) return;
            var s = (AvgState)state;
            s.Sum += args[0].AsFloat64();
            s.Count++;
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (AvgState)state;
            return s.Count > 0 ? new Float64Value(s.Sum / s.Count) : NullValue.Instance;
        }

        private class AvgState : IAggregateState
        {
            public double Sum;
            public long Count;
        }
    }

    public class MinFunction : AggregateFunction
    {
        public override string Name => "min";

        public override IAggregateState CreateState() => new MinState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull) return;
            var s = (MinState)state;
            if (s.Value == null || args[0].CompareTo(s.Value) < 0)
                s.Value = args[0];
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (MinState)state;
            return s.Value ?? NullValue.Instance;
        }

        private class MinState : IAggregateState
        {
            public ClickHouseValue? Value;
        }
    }

    public class MaxFunction : AggregateFunction
    {
        public override string Name => "max";

        public override IAggregateState CreateState() => new MaxState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull) return;
            var s = (MaxState)state;
            if (s.Value == null || args[0].CompareTo(s.Value) > 0)
                s.Value = args[0];
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (MaxState)state;
            return s.Value ?? NullValue.Instance;
        }

        private class MaxState : IAggregateState
        {
            public ClickHouseValue? Value;
        }
    }

    public class AnyFunction : AggregateFunction
    {
        public override string Name => "any";

        public override IAggregateState CreateState() => new AnyState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (AnyState)state;
            if (s.Value == null && !args[0].IsNull)
                s.Value = args[0];
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (AnyState)state;
            return s.Value ?? NullValue.Instance;
        }

        private class AnyState : IAggregateState
        {
            public ClickHouseValue? Value;
        }
    }

    public class AnyLastFunction : AggregateFunction
    {
        public override string Name => "anyLast";

        public override IAggregateState CreateState() => new AnyLastState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (AnyLastState)state;
            if (!args[0].IsNull)
                s.Value = args[0];
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (AnyLastState)state;
            return s.Value ?? NullValue.Instance;
        }

        private class AnyLastState : IAggregateState
        {
            public ClickHouseValue? Value;
        }
    }

    public class GroupArrayFunction : AggregateFunction
    {
        public override string Name => "groupArray";

        public override IAggregateState CreateState() => new GroupArrayState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (GroupArrayState)state;
            if (!args[0].IsNull)
            {
                s.Values.Add(args[0]);
                s.ElementType ??= args[0].Type;
            }
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (GroupArrayState)state;
            return new ArrayValue(s.Values, new ArrayType(s.ElementType ?? Int64Type.Instance));
        }

        private class GroupArrayState : IAggregateState
        {
            public List<ClickHouseValue> Values = [];
            public ClickHouseType? ElementType;
        }
    }

    public class GroupUniqArrayFunction : AggregateFunction
    {
        public override string Name => "groupUniqArray";

        public override IAggregateState CreateState() => new GroupUniqArrayState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (GroupUniqArrayState)state;
            if (!args[0].IsNull && s.Seen.Add(args[0]))
            {
                s.Values.Add(args[0]);
                s.ElementType ??= args[0].Type;
            }
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (GroupUniqArrayState)state;
            return new ArrayValue(s.Values, new ArrayType(s.ElementType ?? Int64Type.Instance));
        }

        private class GroupUniqArrayState : IAggregateState
        {
            public List<ClickHouseValue> Values = [];
            public HashSet<ClickHouseValue> Seen = [];
            public ClickHouseType? ElementType;
        }
    }

    public class UniqFunction : AggregateFunction
    {
        public override string Name => "uniq";

        public override IAggregateState CreateState() => new UniqState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (UniqState)state;
            if (!args[0].IsNull)
                s.Seen.Add(args[0]);
        }

        public override ClickHouseValue Finalize(IAggregateState state) =>
            new Int64Value(((UniqState)state).Seen.Count);

        private class UniqState : IAggregateState
        {
            public HashSet<ClickHouseValue> Seen = [];
        }
    }

    public class UniqExactFunction : AggregateFunction
    {
        public override string Name => "uniqExact";

        public override IAggregateState CreateState() => new UniqState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            var s = (UniqState)state;
            if (!args[0].IsNull)
                s.Seen.Add(args[0]);
        }

        public override ClickHouseValue Finalize(IAggregateState state) =>
            new Int64Value(((UniqState)state).Seen.Count);

        private class UniqState : IAggregateState
        {
            public HashSet<ClickHouseValue> Seen = [];
        }
    }

    public class ArgMinFunction : AggregateFunction
    {
        public override string Name => "argMin";

        public override IAggregateState CreateState() => new ArgMinState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull || args[1].IsNull) return;
            var s = (ArgMinState)state;
            if (s.MinValue == null || args[1].CompareTo(s.MinValue) < 0)
            {
                s.ArgValue = args[0];
                s.MinValue = args[1];
            }
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (ArgMinState)state;
            return s.ArgValue ?? NullValue.Instance;
        }

        private class ArgMinState : IAggregateState
        {
            public ClickHouseValue? ArgValue;
            public ClickHouseValue? MinValue;
        }
    }

    public class ArgMaxFunction : AggregateFunction
    {
        public override string Name => "argMax";

        public override IAggregateState CreateState() => new ArgMaxState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull || args[1].IsNull) return;
            var s = (ArgMaxState)state;
            if (s.MaxValue == null || args[1].CompareTo(s.MaxValue) > 0)
            {
                s.ArgValue = args[0];
                s.MaxValue = args[1];
            }
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (ArgMaxState)state;
            return s.ArgValue ?? NullValue.Instance;
        }

        private class ArgMaxState : IAggregateState
        {
            public ClickHouseValue? ArgValue;
            public ClickHouseValue? MaxValue;
        }
    }

    public class SumIfFunction : AggregateFunction
    {
        public override string Name => "sumIf";

        public override IAggregateState CreateState() => new SumIfState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull || args[1].IsNull) return;
            if (!args[1].AsBool()) return;
            var s = (SumIfState)state;
            s.Sum += args[0].AsFloat64();
            s.HasValue = true;
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (SumIfState)state;
            return s.HasValue ? new Float64Value(s.Sum) : new Float64Value(0);
        }

        private class SumIfState : IAggregateState
        {
            public double Sum;
            public bool HasValue;
        }
    }

    public class CountIfFunction : AggregateFunction
    {
        public override string Name => "countIf";

        public override IAggregateState CreateState() => new CountIfState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull) return;
            if (!args[0].AsBool()) return;
            ((CountIfState)state).Count++;
        }

        public override ClickHouseValue Finalize(IAggregateState state) =>
            new Int64Value(((CountIfState)state).Count);

        private class CountIfState : IAggregateState
        {
            public long Count;
        }
    }

    public class AvgIfFunction : AggregateFunction
    {
        public override string Name => "avgIf";

        public override IAggregateState CreateState() => new AvgIfState();

        public override void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args)
        {
            if (args[0].IsNull || args[1].IsNull) return;
            if (!args[1].AsBool()) return;
            var s = (AvgIfState)state;
            s.Sum += args[0].AsFloat64();
            s.Count++;
        }

        public override ClickHouseValue Finalize(IAggregateState state)
        {
            var s = (AvgIfState)state;
            return s.Count > 0 ? new Float64Value(s.Sum / s.Count) : NullValue.Instance;
        }

        private class AvgIfState : IAggregateState
        {
            public double Sum;
            public long Count;
        }
    }
}
