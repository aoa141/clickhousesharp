namespace ClickHouseSharp.Parsing;

public readonly record struct Token(TokenType Type, string Value, int Line, int Column)
{
    public static Token Eof(int line, int column) => new(TokenType.Eof, "", line, column);
    public static Token Invalid(string value, int line, int column) => new(TokenType.Invalid, value, line, column);

    public override string ToString() => $"{Type}({Value}) at {Line}:{Column}";
}
