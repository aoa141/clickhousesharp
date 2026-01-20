using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public interface IFunction
{
    string Name { get; }
    ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false);
}

public interface IAggregateFunction : IFunction
{
    IAggregateState CreateState();
    void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args);
    ClickHouseValue Finalize(IAggregateState state);
}

public interface IAggregateState
{
}

public abstract class ScalarFunction : IFunction
{
    public abstract string Name { get; }
    public abstract ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false);
}

public abstract class AggregateFunction : IAggregateFunction
{
    public abstract string Name { get; }
    public abstract IAggregateState CreateState();
    public abstract void Accumulate(IAggregateState state, IReadOnlyList<ClickHouseValue> args);
    public abstract ClickHouseValue Finalize(IAggregateState state);

    public ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
    {
        // For single-value execution (non-aggregated context)
        var state = CreateState();
        Accumulate(state, args);
        return Finalize(state);
    }
}
