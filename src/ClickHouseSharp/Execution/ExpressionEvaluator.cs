using System.Text.RegularExpressions;
using ClickHouseSharp.Functions;
using ClickHouseSharp.Parsing.Ast;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Execution;

public class ExpressionEvaluator
{
    private readonly FunctionRegistry _functions;
    private Func<SelectStatement, QueryResult>? _subqueryExecutor;

    public ExpressionEvaluator(FunctionRegistry? functions = null)
    {
        _functions = functions ?? FunctionRegistry.Default;
    }

    public void SetSubqueryExecutor(Func<SelectStatement, QueryResult> executor)
    {
        _subqueryExecutor = executor;
    }

    public ClickHouseValue Evaluate(Expression expr, RowContext context)
    {
        return expr switch
        {
            LiteralExpression lit => EvaluateLiteral(lit),
            ColumnExpression col => EvaluateColumn(col, context),
            BinaryExpression bin => EvaluateBinary(bin, context),
            UnaryExpression un => EvaluateUnary(un, context),
            FunctionCallExpression func => EvaluateFunction(func, context),
            CastExpression cast => EvaluateCast(cast, context),
            CaseExpression caseExpr => EvaluateCase(caseExpr, context),
            InExpression inExpr => EvaluateIn(inExpr, context),
            BetweenExpression between => EvaluateBetween(between, context),
            ArrayExpression arr => EvaluateArray(arr, context),
            TupleExpression tup => EvaluateTuple(tup, context),
            IndexExpression idx => EvaluateIndex(idx, context),
            ConditionalExpression cond => EvaluateConditional(cond, context),
            AliasedExpression alias => Evaluate(alias.Expression, context),
            StarExpression => throw new InvalidOperationException("Star expressions should be expanded before evaluation"),
            SubqueryExpression => throw new InvalidOperationException("Subqueries should be evaluated by the query executor"),
            ExistsExpression => throw new InvalidOperationException("EXISTS should be evaluated by the query executor"),
            WindowFunctionExpression => throw new InvalidOperationException("Window functions should be evaluated by the query executor"),
            LambdaExpression => throw new InvalidOperationException("Lambda expressions should be evaluated in function context"),
            ParameterExpression => throw new InvalidOperationException("Parameters not supported yet"),
            _ => throw new NotImplementedException($"Expression type {expr.GetType().Name} not implemented")
        };
    }

    private static ClickHouseValue EvaluateLiteral(LiteralExpression lit)
    {
        return lit.Type switch
        {
            LiteralType.Integer => new Int64Value((long)lit.Value!),
            LiteralType.Float => new Float64Value((double)lit.Value!),
            LiteralType.String => new StringValue((string)lit.Value!),
            LiteralType.Boolean => new BoolValue((bool)lit.Value!),
            LiteralType.Null => NullValue.Instance,
            _ => throw new InvalidOperationException($"Unknown literal type: {lit.Type}")
        };
    }

    private static ClickHouseValue EvaluateColumn(ColumnExpression col, RowContext context)
    {
        if (col.TableName != null)
            return context.GetColumn(col.TableName, col.ColumnName);
        return context.GetColumn(col.ColumnName);
    }

    private ClickHouseValue EvaluateBinary(BinaryExpression bin, RowContext context)
    {
        // Short-circuit evaluation for AND/OR
        if (bin.Operator == BinaryOperator.And)
        {
            var left = Evaluate(bin.Left, context);
            if (left.IsNull) return NullValue.Instance;
            if (!left.AsBool()) return new BoolValue(false);
            var right = Evaluate(bin.Right, context);
            if (right.IsNull) return NullValue.Instance;
            return new BoolValue(right.AsBool());
        }

        if (bin.Operator == BinaryOperator.Or)
        {
            var left = Evaluate(bin.Left, context);
            if (!left.IsNull && left.AsBool()) return new BoolValue(true);
            var right = Evaluate(bin.Right, context);
            if (left.IsNull && right.IsNull) return NullValue.Instance;
            if (!right.IsNull && right.AsBool()) return new BoolValue(true);
            if (left.IsNull || right.IsNull) return NullValue.Instance;
            return new BoolValue(false);
        }

        var leftVal = Evaluate(bin.Left, context);
        var rightVal = Evaluate(bin.Right, context);

        // NULL propagation for most operators
        if (leftVal.IsNull || rightVal.IsNull)
        {
            // Comparison with NULL returns NULL (except IS NULL which is handled separately)
            return bin.Operator switch
            {
                BinaryOperator.Concat => leftVal.IsNull ? rightVal : leftVal, // NULL || 'a' = 'a' in some systems
                _ => NullValue.Instance
            };
        }

        return bin.Operator switch
        {
            BinaryOperator.Add => EvaluateAdd(leftVal, rightVal),
            BinaryOperator.Subtract => EvaluateSubtract(leftVal, rightVal),
            BinaryOperator.Multiply => EvaluateMultiply(leftVal, rightVal),
            BinaryOperator.Divide => EvaluateDivide(leftVal, rightVal),
            BinaryOperator.Modulo => EvaluateModulo(leftVal, rightVal),
            BinaryOperator.Equal => new BoolValue(leftVal.Equals(rightVal)),
            BinaryOperator.NotEqual => new BoolValue(!leftVal.Equals(rightVal)),
            BinaryOperator.LessThan => new BoolValue(leftVal.CompareTo(rightVal) < 0),
            BinaryOperator.LessThanOrEqual => new BoolValue(leftVal.CompareTo(rightVal) <= 0),
            BinaryOperator.GreaterThan => new BoolValue(leftVal.CompareTo(rightVal) > 0),
            BinaryOperator.GreaterThanOrEqual => new BoolValue(leftVal.CompareTo(rightVal) >= 0),
            BinaryOperator.Concat => new StringValue(leftVal.AsString() + rightVal.AsString()),
            BinaryOperator.Like => EvaluateLike(leftVal.AsString(), rightVal.AsString(), caseSensitive: true),
            BinaryOperator.ILike => EvaluateLike(leftVal.AsString(), rightVal.AsString(), caseSensitive: false),
            BinaryOperator.NotLike => new BoolValue(!EvaluateLike(leftVal.AsString(), rightVal.AsString(), true).AsBool()),
            BinaryOperator.NotILike => new BoolValue(!EvaluateLike(leftVal.AsString(), rightVal.AsString(), false).AsBool()),
            _ => throw new InvalidOperationException($"Unknown binary operator: {bin.Operator}")
        };
    }

    private static ClickHouseValue EvaluateAdd(ClickHouseValue left, ClickHouseValue right)
    {
        if (left is Int64Value || right is Int64Value)
            return new Int64Value(left.AsInt64() + right.AsInt64());
        return new Float64Value(left.AsFloat64() + right.AsFloat64());
    }

    private static ClickHouseValue EvaluateSubtract(ClickHouseValue left, ClickHouseValue right)
    {
        if (left is Int64Value || right is Int64Value)
            return new Int64Value(left.AsInt64() - right.AsInt64());
        return new Float64Value(left.AsFloat64() - right.AsFloat64());
    }

    private static ClickHouseValue EvaluateMultiply(ClickHouseValue left, ClickHouseValue right)
    {
        if (left is Int64Value || right is Int64Value)
            return new Int64Value(left.AsInt64() * right.AsInt64());
        return new Float64Value(left.AsFloat64() * right.AsFloat64());
    }

    private static ClickHouseValue EvaluateDivide(ClickHouseValue left, ClickHouseValue right)
    {
        var rightVal = right.AsFloat64();
        if (rightVal == 0)
            return new Float64Value(double.PositiveInfinity); // ClickHouse returns inf for division by zero
        return new Float64Value(left.AsFloat64() / rightVal);
    }

    private static ClickHouseValue EvaluateModulo(ClickHouseValue left, ClickHouseValue right)
    {
        return new Int64Value(left.AsInt64() % right.AsInt64());
    }

    private static BoolValue EvaluateLike(string value, string pattern, bool caseSensitive)
    {
        // Convert SQL LIKE pattern to regex
        var regexPattern = "^" + Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";

        var options = caseSensitive ? RegexOptions.None : RegexOptions.IgnoreCase;
        var match = Regex.IsMatch(value, regexPattern, options);
        return new BoolValue(match);
    }

    private ClickHouseValue EvaluateUnary(UnaryExpression un, RowContext context)
    {
        var operand = Evaluate(un.Operand, context);

        return un.Operator switch
        {
            UnaryOperator.Not => operand.IsNull ? NullValue.Instance : new BoolValue(!operand.AsBool()),
            UnaryOperator.Negate => operand.IsNull ? NullValue.Instance :
                operand is Float64Value ? new Float64Value(-operand.AsFloat64()) :
                new Int64Value(-operand.AsInt64()),
            UnaryOperator.IsNull => new BoolValue(operand.IsNull),
            UnaryOperator.IsNotNull => new BoolValue(!operand.IsNull),
            _ => throw new InvalidOperationException($"Unknown unary operator: {un.Operator}")
        };
    }

    private ClickHouseValue EvaluateFunction(FunctionCallExpression func, RowContext context)
    {
        // Evaluate arguments
        var args = func.Arguments.Select(a => Evaluate(a, context)).ToList();

        // Look up and execute function
        if (_functions.TryGetFunction(func.FunctionName, out var function))
        {
            return function.Execute(args, func.Distinct);
        }

        throw new InvalidOperationException($"Unknown function: {func.FunctionName}");
    }

    private ClickHouseValue EvaluateCast(CastExpression cast, RowContext context)
    {
        var value = Evaluate(cast.Operand, context);
        if (value.IsNull) return NullValue.Instance;

        var targetType = ClickHouseType.Parse(cast.TargetType);
        return targetType.CreateValue(value.RawValue);
    }

    private ClickHouseValue EvaluateCase(CaseExpression caseExpr, RowContext context)
    {
        if (caseExpr.Operand != null)
        {
            // Simple CASE: CASE operand WHEN value THEN result ...
            var operand = Evaluate(caseExpr.Operand, context);
            foreach (var when in caseExpr.WhenClauses)
            {
                var value = Evaluate(when.Condition, context);
                if (operand.Equals(value))
                    return Evaluate(when.Result, context);
            }
        }
        else
        {
            // Searched CASE: CASE WHEN condition THEN result ...
            foreach (var when in caseExpr.WhenClauses)
            {
                var condition = Evaluate(when.Condition, context);
                if (!condition.IsNull && condition.AsBool())
                    return Evaluate(when.Result, context);
            }
        }

        return caseExpr.ElseResult != null
            ? Evaluate(caseExpr.ElseResult, context)
            : NullValue.Instance;
    }

    private ClickHouseValue EvaluateIn(InExpression inExpr, RowContext context)
    {
        var left = Evaluate(inExpr.Left, context);
        if (left.IsNull) return NullValue.Instance;

        if (inExpr.Values != null)
        {
            var found = inExpr.Values.Any(v =>
            {
                var val = Evaluate(v, context);
                return ValuesEqual(left, val);
            });
            return new BoolValue(inExpr.Not ? !found : found);
        }

        // Handle subquery IN
        if (inExpr.Subquery != null && _subqueryExecutor != null)
        {
            var result = _subqueryExecutor(inExpr.Subquery);
            var found = result.Rows.Any(row => ValuesEqual(left, row[0]));
            return new BoolValue(inExpr.Not ? !found : found);
        }

        throw new InvalidOperationException("IN subquery should be handled by query executor");
    }

    private static bool ValuesEqual(ClickHouseValue left, ClickHouseValue right)
    {
        if (left.IsNull || right.IsNull) return left.IsNull && right.IsNull;
        if (left.Equals(right)) return true;

        // Try numeric comparison for different integer types
        if (IsNumeric(left) && IsNumeric(right))
        {
            try
            {
                return left.AsFloat64() == right.AsFloat64();
            }
            catch
            {
                return false;
            }
        }

        return false;
    }

    private static bool IsNumeric(ClickHouseValue value)
    {
        return value is Int8Value or Int16Value or Int32Value or Int64Value
            or UInt8Value or UInt16Value or UInt32Value or UInt64Value
            or Float32Value or Float64Value or DecimalValue;
    }

    private ClickHouseValue EvaluateBetween(BetweenExpression between, RowContext context)
    {
        var operand = Evaluate(between.Operand, context);
        var low = Evaluate(between.Low, context);
        var high = Evaluate(between.High, context);

        if (operand.IsNull || low.IsNull || high.IsNull)
            return NullValue.Instance;

        var inRange = operand.CompareTo(low) >= 0 && operand.CompareTo(high) <= 0;
        return new BoolValue(between.Not ? !inRange : inRange);
    }

    private ClickHouseValue EvaluateArray(ArrayExpression arr, RowContext context)
    {
        var elements = arr.Elements.Select(e => Evaluate(e, context)).ToList();

        // Determine element type from first non-null element
        var elementType = elements.FirstOrDefault(e => !e.IsNull)?.Type ?? Int64Type.Instance;
        var arrayType = new ArrayType(elementType);

        return new ArrayValue(elements, arrayType);
    }

    private ClickHouseValue EvaluateTuple(TupleExpression tup, RowContext context)
    {
        var elements = tup.Elements.Select(e => Evaluate(e, context)).ToList();
        var types = elements.Select(e => e.Type).ToList();
        var tupleType = new TupleType(types);
        return new TupleValue(elements, tupleType);
    }

    private ClickHouseValue EvaluateIndex(IndexExpression idx, RowContext context)
    {
        var array = Evaluate(idx.Array, context);
        var index = Evaluate(idx.Index, context);

        if (array.IsNull || index.IsNull)
            return NullValue.Instance;

        if (array is ArrayValue arr)
        {
            var i = (int)index.AsInt64();
            // ClickHouse uses 1-based indexing for arrays
            if (i < 1 || i > arr.Elements.Count)
                return NullValue.Instance; // Out of bounds returns NULL
            return arr.Elements[i - 1];
        }

        if (array is MapValue map)
        {
            if (map.Entries.TryGetValue(index, out var value))
                return value;
            return NullValue.Instance;
        }

        throw new InvalidOperationException($"Cannot index into {array.Type}");
    }

    private ClickHouseValue EvaluateConditional(ConditionalExpression cond, RowContext context)
    {
        var condition = Evaluate(cond.Condition, context);
        if (condition.IsNull)
            return Evaluate(cond.ElseExpr, context);
        return condition.AsBool()
            ? Evaluate(cond.ThenExpr, context)
            : Evaluate(cond.ElseExpr, context);
    }
}
