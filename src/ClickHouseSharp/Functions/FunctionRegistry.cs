namespace ClickHouseSharp.Functions;

public class FunctionRegistry
{
    private readonly Dictionary<string, IFunction> _functions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IAggregateFunction> _aggregateFunctions = new(StringComparer.OrdinalIgnoreCase);

    public static FunctionRegistry Default { get; } = CreateDefault();

    public void Register(IFunction function)
    {
        _functions[function.Name] = function;
        if (function is IAggregateFunction agg)
            _aggregateFunctions[function.Name] = agg;
    }

    public void RegisterAlias(string alias, string target)
    {
        if (_functions.TryGetValue(target, out var func))
            _functions[alias] = func;
        if (_aggregateFunctions.TryGetValue(target, out var agg))
            _aggregateFunctions[alias] = agg;
    }

    public bool TryGetFunction(string name, out IFunction? function)
    {
        return _functions.TryGetValue(name, out function);
    }

    public bool TryGetAggregateFunction(string name, out IAggregateFunction? function)
    {
        return _aggregateFunctions.TryGetValue(name, out function);
    }

    public bool IsAggregateFunction(string name) => _aggregateFunctions.ContainsKey(name);

    private static FunctionRegistry CreateDefault()
    {
        var registry = new FunctionRegistry();

        // Math functions
        registry.Register(new MathFunctions.AbsFunction());
        registry.Register(new MathFunctions.CeilFunction());
        registry.Register(new MathFunctions.FloorFunction());
        registry.Register(new MathFunctions.RoundFunction());
        registry.Register(new MathFunctions.SqrtFunction());
        registry.Register(new MathFunctions.PowerFunction());
        registry.Register(new MathFunctions.ExpFunction());
        registry.Register(new MathFunctions.LogFunction());
        registry.Register(new MathFunctions.Log2Function());
        registry.Register(new MathFunctions.Log10Function());
        registry.Register(new MathFunctions.SinFunction());
        registry.Register(new MathFunctions.CosFunction());
        registry.Register(new MathFunctions.TanFunction());
        registry.Register(new MathFunctions.AsinFunction());
        registry.Register(new MathFunctions.AcosFunction());
        registry.Register(new MathFunctions.AtanFunction());
        registry.Register(new MathFunctions.Atan2Function());
        registry.Register(new MathFunctions.PiFunction());
        registry.Register(new MathFunctions.EFunction());
        registry.Register(new MathFunctions.SignFunction());
        registry.Register(new MathFunctions.ModFunction());
        registry.Register(new MathFunctions.IntDivFunction());
        registry.Register(new MathFunctions.GreatestFunction());
        registry.Register(new MathFunctions.LeastFunction());

        // String functions
        registry.Register(new StringFunctions.LengthFunction());
        registry.Register(new StringFunctions.LowerFunction());
        registry.Register(new StringFunctions.UpperFunction());
        registry.Register(new StringFunctions.TrimFunction());
        registry.Register(new StringFunctions.LTrimFunction());
        registry.Register(new StringFunctions.RTrimFunction());
        registry.Register(new StringFunctions.SubstringFunction());
        registry.Register(new StringFunctions.ConcatFunction());
        registry.Register(new StringFunctions.ReplaceFunction());
        registry.Register(new StringFunctions.ReverseFunction());
        registry.Register(new StringFunctions.PositionFunction());
        registry.Register(new StringFunctions.StartsWithFunction());
        registry.Register(new StringFunctions.EndsWithFunction());
        registry.Register(new StringFunctions.RepeatFunction());
        registry.Register(new StringFunctions.LeftFunction());
        registry.Register(new StringFunctions.RightFunction());
        registry.Register(new StringFunctions.SplitByCharFunction());
        registry.Register(new StringFunctions.SplitByStringFunction());
        registry.Register(new StringFunctions.ArrayStringConcatFunction());
        registry.Register(new StringFunctions.FormatFunction());
        registry.Register(new StringFunctions.ToStringFunction());
        registry.Register(new StringFunctions.EmptyFunction());
        registry.Register(new StringFunctions.NotEmptyFunction());

        // Date/Time functions
        registry.Register(new DateTimeFunctions.NowFunction());
        registry.Register(new DateTimeFunctions.TodayFunction());
        registry.Register(new DateTimeFunctions.YesterdayFunction());
        registry.Register(new DateTimeFunctions.ToYearFunction());
        registry.Register(new DateTimeFunctions.ToMonthFunction());
        registry.Register(new DateTimeFunctions.ToDayOfMonthFunction());
        registry.Register(new DateTimeFunctions.ToHourFunction());
        registry.Register(new DateTimeFunctions.ToMinuteFunction());
        registry.Register(new DateTimeFunctions.ToSecondFunction());
        registry.Register(new DateTimeFunctions.ToDayOfWeekFunction());
        registry.Register(new DateTimeFunctions.ToDayOfYearFunction());
        registry.Register(new DateTimeFunctions.ToQuarterFunction());
        registry.Register(new DateTimeFunctions.ToDateFunction());
        registry.Register(new DateTimeFunctions.ToDateTimeFunction());
        registry.Register(new DateTimeFunctions.ToUnixTimestampFunction());
        registry.Register(new DateTimeFunctions.FromUnixTimestampFunction());
        registry.Register(new DateTimeFunctions.DateAddFunction());
        registry.Register(new DateTimeFunctions.DateSubFunction());
        registry.Register(new DateTimeFunctions.DateDiffFunction());
        registry.Register(new DateTimeFunctions.FormatDateTimeFunction());

        // Type conversion functions
        registry.Register(new TypeFunctions.ToInt8Function());
        registry.Register(new TypeFunctions.ToInt16Function());
        registry.Register(new TypeFunctions.ToInt32Function());
        registry.Register(new TypeFunctions.ToInt64Function());
        registry.Register(new TypeFunctions.ToUInt8Function());
        registry.Register(new TypeFunctions.ToUInt16Function());
        registry.Register(new TypeFunctions.ToUInt32Function());
        registry.Register(new TypeFunctions.ToUInt64Function());
        registry.Register(new TypeFunctions.ToFloat32Function());
        registry.Register(new TypeFunctions.ToFloat64Function());
        registry.Register(new TypeFunctions.ToDecimalFunction());

        // Conditional functions
        registry.Register(new ConditionalFunctions.IfFunction());
        registry.Register(new ConditionalFunctions.IfNullFunction());
        registry.Register(new ConditionalFunctions.NullIfFunction());
        registry.Register(new ConditionalFunctions.CoalesceFunction());
        registry.Register(new ConditionalFunctions.MultiIfFunction());
        registry.Register(new ConditionalFunctions.AssumeNotNullFunction());

        // Array functions
        registry.Register(new ArrayFunctions.ArrayFunction());
        registry.Register(new ArrayFunctions.ArrayLengthFunction());
        registry.Register(new ArrayFunctions.ArrayElementFunction());
        registry.Register(new ArrayFunctions.HasFunction());
        registry.Register(new ArrayFunctions.IndexOfFunction());
        registry.Register(new ArrayFunctions.ArrayConcatFunction());
        registry.Register(new ArrayFunctions.ArrayPushBackFunction());
        registry.Register(new ArrayFunctions.ArrayPushFrontFunction());
        registry.Register(new ArrayFunctions.ArrayPopBackFunction());
        registry.Register(new ArrayFunctions.ArrayPopFrontFunction());
        registry.Register(new ArrayFunctions.ArraySliceFunction());
        registry.Register(new ArrayFunctions.ArrayReverseFunction());
        registry.Register(new ArrayFunctions.ArrayDistinctFunction());
        registry.Register(new ArrayFunctions.ArraySortFunction());
        registry.Register(new ArrayFunctions.EmptyArrayToSingleFunction());
        registry.Register(new ArrayFunctions.RangeFunction());

        // Aggregate functions
        registry.Register(new AggregateFunctions.CountFunction());
        registry.Register(new AggregateFunctions.SumFunction());
        registry.Register(new AggregateFunctions.AvgFunction());
        registry.Register(new AggregateFunctions.MinFunction());
        registry.Register(new AggregateFunctions.MaxFunction());
        registry.Register(new AggregateFunctions.AnyFunction());
        registry.Register(new AggregateFunctions.AnyLastFunction());
        registry.Register(new AggregateFunctions.GroupArrayFunction());
        registry.Register(new AggregateFunctions.GroupUniqArrayFunction());
        registry.Register(new AggregateFunctions.UniqFunction());
        registry.Register(new AggregateFunctions.UniqExactFunction());
        registry.Register(new AggregateFunctions.ArgMinFunction());
        registry.Register(new AggregateFunctions.ArgMaxFunction());
        registry.Register(new AggregateFunctions.SumIfFunction());
        registry.Register(new AggregateFunctions.CountIfFunction());
        registry.Register(new AggregateFunctions.AvgIfFunction());

        // Tuple functions
        registry.Register(new TupleFunctions.TupleFunction());
        registry.Register(new TupleFunctions.TupleElementFunction());

        // Map functions
        registry.Register(new MapFunctions.MapFunction());
        registry.Register(new MapFunctions.MapKeysFunction());
        registry.Register(new MapFunctions.MapValuesFunction());

        // Misc functions
        registry.Register(new MiscFunctions.RowNumberFunction());
        registry.Register(new MiscFunctions.RandFunction());
        registry.Register(new MiscFunctions.Rand64Function());
        registry.Register(new MiscFunctions.GenerateUUIDv4Function());
        registry.Register(new MiscFunctions.VersionFunction());
        registry.Register(new MiscFunctions.TypeOfFunction());

        // Aliases
        registry.RegisterAlias("ceiling", "ceil");
        registry.RegisterAlias("ln", "log");
        registry.RegisterAlias("substr", "substring");
        registry.RegisterAlias("mid", "substring");
        registry.RegisterAlias("locate", "position");
        registry.RegisterAlias("lcase", "lower");
        registry.RegisterAlias("ucase", "upper");
        registry.RegisterAlias("size", "length");
        registry.RegisterAlias("char_length", "length");
        registry.RegisterAlias("character_length", "length");
        registry.RegisterAlias("toYear", "toYear");
        registry.RegisterAlias("toMonth", "toMonth");
        registry.RegisterAlias("toDayOfMonth", "toDayOfMonth");
        registry.RegisterAlias("day", "toDayOfMonth");
        registry.RegisterAlias("hour", "toHour");
        registry.RegisterAlias("minute", "toMinute");
        registry.RegisterAlias("second", "toSecond");
        registry.RegisterAlias("unixTimestamp", "toUnixTimestamp");
        registry.RegisterAlias("count_distinct", "uniqExact");

        return registry;
    }
}
