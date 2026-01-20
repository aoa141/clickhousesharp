namespace ClickHouseSharp.Parsing.Ast;

// Simple table reference: table_name [AS alias]
public sealed record TableNameReference(string TableName, string? Alias) : TableReference;

// Subquery as table reference: (SELECT ...) AS alias
// Can be either a SelectStatement or SetOperationStatement
public sealed record SubqueryReference(Statement Query, string Alias) : TableReference;

// JOIN reference
public sealed record JoinReference(
    TableReference Left,
    JoinType Type,
    TableReference Right,
    Expression? Condition,
    IReadOnlyList<string>? Using
) : TableReference;

public enum JoinType
{
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    AsOf
}

// Table function: file('path', 'CSV'), numbers(10), etc.
public sealed record TableFunctionReference(
    string FunctionName,
    IReadOnlyList<Expression> Arguments,
    string? Alias
) : TableReference;

// Array join (ClickHouse specific)
public sealed record ArrayJoinReference(
    TableReference Source,
    IReadOnlyList<Expression> Arrays,
    bool Left
) : TableReference;
