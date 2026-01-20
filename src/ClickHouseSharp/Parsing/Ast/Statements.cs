namespace ClickHouseSharp.Parsing.Ast;

// SELECT statement
public sealed record SelectStatement(
    IReadOnlyList<Expression> Columns,
    TableReference? From,
    Expression? Where,
    IReadOnlyList<Expression>? GroupBy,
    Expression? Having,
    IReadOnlyList<OrderByItem>? OrderBy,
    Expression? Limit,
    Expression? Offset,
    bool Distinct,
    IReadOnlyList<CteDefinition>? With
) : Statement;

// ORDER BY item: column ASC/DESC [NULLS FIRST/LAST]
public sealed record OrderByItem(Expression Expression, bool Descending, NullsPosition? Nulls) : SqlNode;

public enum NullsPosition
{
    First,
    Last
}

// CTE: WITH name AS (SELECT ...)
public sealed record CteDefinition(string Name, IReadOnlyList<string>? Columns, SelectStatement Query) : SqlNode;

// UNION/INTERSECT/EXCEPT
public sealed record SetOperationStatement(
    Statement Left,
    SetOperationType Operation,
    bool All,
    Statement Right
) : Statement;

public enum SetOperationType
{
    Union,
    Intersect,
    Except
}

// INSERT statement
public sealed record InsertStatement(
    string TableName,
    IReadOnlyList<string>? Columns,
    InsertSource Source
) : Statement;

public abstract record InsertSource : SqlNode;
public sealed record ValuesInsertSource(IReadOnlyList<IReadOnlyList<Expression>> Rows) : InsertSource;
public sealed record SelectInsertSource(SelectStatement Query) : InsertSource;

// CREATE TABLE statement
public sealed record CreateTableStatement(
    string TableName,
    IReadOnlyList<ColumnDefinition> Columns,
    bool IfNotExists,
    string? Engine,
    IReadOnlyList<string>? PrimaryKey,
    IReadOnlyList<string>? OrderBy
) : Statement;

public sealed record ColumnDefinition(
    string Name,
    DataTypeNode DataType,
    Expression? DefaultValue,
    bool Nullable
) : SqlNode;

// DROP TABLE statement
public sealed record DropTableStatement(string TableName, bool IfExists) : Statement;

// UPDATE statement (limited in ClickHouse, but we'll support basic form)
public sealed record UpdateStatement(
    string TableName,
    IReadOnlyList<Assignment> Assignments,
    Expression? Where
) : Statement;

public sealed record Assignment(string Column, Expression Value) : SqlNode;

// DELETE statement
public sealed record DeleteStatement(string TableName, Expression? Where) : Statement;

// Data type node
public sealed record DataTypeNode(
    string TypeName,
    IReadOnlyList<DataTypeNode>? TypeParameters,
    IReadOnlyList<int>? NumericParameters
) : SqlNode
{
    public override string ToString()
    {
        var result = TypeName;
        if (TypeParameters?.Count > 0)
            result += $"({string.Join(", ", TypeParameters)})";
        else if (NumericParameters?.Count > 0)
            result += $"({string.Join(", ", NumericParameters)})";
        return result;
    }
}
