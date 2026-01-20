namespace ClickHouseSharp.Parsing.Ast;

public abstract record SqlNode;

// Base types for expressions and statements
public abstract record Expression : SqlNode;
public abstract record Statement : SqlNode;
public abstract record TableReference : SqlNode;
