namespace ClickHouseSharp.Parsing.Ast;

// Literal expressions
public sealed record LiteralExpression(object? Value, LiteralType Type) : Expression;

public enum LiteralType
{
    Integer,
    Float,
    String,
    Boolean,
    Null
}

// Column reference: table.column or just column
public sealed record ColumnExpression(string? TableName, string ColumnName) : Expression
{
    public string FullName => TableName != null ? $"{TableName}.{ColumnName}" : ColumnName;
}

// Binary operations: a + b, a = b, etc.
public sealed record BinaryExpression(Expression Left, BinaryOperator Operator, Expression Right) : Expression;

public enum BinaryOperator
{
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String
    Concat,
    Like,
    ILike,
    NotLike,
    NotILike
}

// Unary operations: NOT x, -x
public sealed record UnaryExpression(UnaryOperator Operator, Expression Operand) : Expression;

public enum UnaryOperator
{
    Not,
    Negate,
    IsNull,
    IsNotNull
}

// Function call: func(arg1, arg2, ...)
public sealed record FunctionCallExpression(
    string FunctionName,
    IReadOnlyList<Expression> Arguments,
    bool Distinct = false
) : Expression;

// CAST(expr AS type)
public sealed record CastExpression(Expression Operand, DataTypeNode TargetType) : Expression;

// CASE expression
public sealed record CaseExpression(
    Expression? Operand,
    IReadOnlyList<WhenClause> WhenClauses,
    Expression? ElseResult
) : Expression;

public sealed record WhenClause(Expression Condition, Expression Result) : SqlNode;

// IN expression: x IN (1, 2, 3) or x IN (SELECT ...)
public sealed record InExpression(Expression Left, IReadOnlyList<Expression>? Values, SelectStatement? Subquery, bool Not) : Expression;

// BETWEEN expression: x BETWEEN a AND b
public sealed record BetweenExpression(Expression Operand, Expression Low, Expression High, bool Not) : Expression;

// Subquery expression: (SELECT ...)
public sealed record SubqueryExpression(SelectStatement Query) : Expression;

// EXISTS expression: EXISTS (SELECT ...)
public sealed record ExistsExpression(SelectStatement Subquery) : Expression;

// Array expression: [1, 2, 3]
public sealed record ArrayExpression(IReadOnlyList<Expression> Elements) : Expression;

// Tuple expression: (1, 'a', 3.14)
public sealed record TupleExpression(IReadOnlyList<Expression> Elements) : Expression;

// Array/Map access: arr[1], map['key']
public sealed record IndexExpression(Expression Array, Expression Index) : Expression;

// Ternary/conditional: condition ? then : else (ClickHouse uses if function, but supports this syntax too)
public sealed record ConditionalExpression(Expression Condition, Expression ThenExpr, Expression ElseExpr) : Expression;

// Window function expression
public sealed record WindowFunctionExpression(
    FunctionCallExpression Function,
    WindowSpec Window
) : Expression;

public sealed record WindowSpec(
    IReadOnlyList<Expression>? PartitionBy,
    IReadOnlyList<OrderByItem>? OrderBy,
    WindowFrame? Frame
) : SqlNode;

public sealed record WindowFrame(
    WindowFrameType Type,
    WindowFrameBound Start,
    WindowFrameBound? End
) : SqlNode;

public enum WindowFrameType
{
    Rows,
    Range
}

public sealed record WindowFrameBound(WindowFrameBoundType Type, Expression? Offset) : SqlNode;

public enum WindowFrameBoundType
{
    UnboundedPreceding,
    Preceding,
    CurrentRow,
    Following,
    UnboundedFollowing
}

// Star expression: * or table.*
public sealed record StarExpression(string? TableName) : Expression;

// Aliased expression: expr AS alias
public sealed record AliasedExpression(Expression Expression, string Alias) : Expression;

// Placeholder for parameter: $1, ?, :name
public sealed record ParameterExpression(string Name) : Expression;

// Lambda expression (for array functions): x -> x * 2
public sealed record LambdaExpression(IReadOnlyList<string> Parameters, Expression Body) : Expression;
