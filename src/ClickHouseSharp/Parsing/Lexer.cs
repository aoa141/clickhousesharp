using System.Text;

namespace ClickHouseSharp.Parsing;

public class Lexer
{
    private readonly string _source;
    private int _position;
    private int _line = 1;
    private int _column = 1;

    private static readonly Dictionary<string, TokenType> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        ["SELECT"] = TokenType.Select,
        ["FROM"] = TokenType.From,
        ["WHERE"] = TokenType.Where,
        ["AND"] = TokenType.And,
        ["OR"] = TokenType.Or,
        ["NOT"] = TokenType.Not,
        ["AS"] = TokenType.As,
        ["JOIN"] = TokenType.Join,
        ["INNER"] = TokenType.Inner,
        ["LEFT"] = TokenType.Left,
        ["RIGHT"] = TokenType.Right,
        ["FULL"] = TokenType.Full,
        ["OUTER"] = TokenType.Outer,
        ["CROSS"] = TokenType.Cross,
        ["ON"] = TokenType.On,
        ["USING"] = TokenType.Using,
        ["GROUP"] = TokenType.Group,
        ["BY"] = TokenType.By,
        ["HAVING"] = TokenType.Having,
        ["ORDER"] = TokenType.Order,
        ["ASC"] = TokenType.Asc,
        ["DESC"] = TokenType.Desc,
        ["LIMIT"] = TokenType.Limit,
        ["OFFSET"] = TokenType.Offset,
        ["UNION"] = TokenType.Union,
        ["ALL"] = TokenType.All,
        ["INTERSECT"] = TokenType.Intersect,
        ["EXCEPT"] = TokenType.Except,
        ["DISTINCT"] = TokenType.Distinct,
        ["IN"] = TokenType.In,
        ["IS"] = TokenType.Is,
        ["NULL"] = TokenType.Null,
        ["TRUE"] = TokenType.True,
        ["FALSE"] = TokenType.False,
        ["BETWEEN"] = TokenType.Between,
        ["LIKE"] = TokenType.Like,
        ["ILIKE"] = TokenType.ILike,
        ["CASE"] = TokenType.Case,
        ["WHEN"] = TokenType.When,
        ["THEN"] = TokenType.Then,
        ["ELSE"] = TokenType.Else,
        ["END"] = TokenType.End,
        ["CAST"] = TokenType.Cast,
        ["CREATE"] = TokenType.Create,
        ["TABLE"] = TokenType.Table,
        ["INSERT"] = TokenType.Insert,
        ["INTO"] = TokenType.Into,
        ["VALUES"] = TokenType.Values,
        ["UPDATE"] = TokenType.Update,
        ["SET"] = TokenType.Set,
        ["DELETE"] = TokenType.Delete,
        ["DROP"] = TokenType.Drop,
        ["IF"] = TokenType.If,
        ["EXISTS"] = TokenType.Exists,
        ["WITH"] = TokenType.With,
        ["RECURSIVE"] = TokenType.Recursive,
        ["OVER"] = TokenType.Over,
        ["PARTITION"] = TokenType.Partition,
        ["ROWS"] = TokenType.Rows,
        ["RANGE"] = TokenType.Range,
        ["UNBOUNDED"] = TokenType.Unbounded,
        ["PRECEDING"] = TokenType.Preceding,
        ["FOLLOWING"] = TokenType.Following,
        ["CURRENT"] = TokenType.Current,
        ["ROW"] = TokenType.Row,
        ["ARRAY"] = TokenType.Array,
        ["TUPLE"] = TokenType.Tuple,
        ["MAP"] = TokenType.Map,
        ["NULLABLE"] = TokenType.Nullable,
        ["GLOBAL"] = TokenType.Global,
        ["ANY"] = TokenType.Any,
        ["ASOF"] = TokenType.Asof,
        ["ANTI"] = TokenType.Anti,
        ["SEMI"] = TokenType.Semi,
        ["FORMAT"] = TokenType.Format,
        ["SETTINGS"] = TokenType.Settings,
        ["PREWHERE"] = TokenType.Prewhere,
        ["SAMPLE"] = TokenType.Sample,
        ["FINAL"] = TokenType.Final
    };

    public Lexer(string source)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
    }

    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();
        Token token;
        do
        {
            token = NextToken();
            tokens.Add(token);
        } while (token.Type != TokenType.Eof);

        return tokens;
    }

    public Token NextToken()
    {
        SkipWhitespaceAndComments();

        if (IsAtEnd())
            return Token.Eof(_line, _column);

        var startLine = _line;
        var startColumn = _column;
        var ch = Current();

        // String literals
        if (ch == '\'' || ch == '"')
            return ReadString(ch, startLine, startColumn);

        // Backtick quoted identifier
        if (ch == '`')
            return ReadQuotedIdentifier(startLine, startColumn);

        // Numbers
        if (char.IsDigit(ch) || (ch == '.' && Peek(1) is char p && char.IsDigit(p)))
            return ReadNumber(startLine, startColumn);

        // Identifiers and keywords
        if (char.IsLetter(ch) || ch == '_')
            return ReadIdentifierOrKeyword(startLine, startColumn);

        // Operators and delimiters
        return ReadOperatorOrDelimiter(startLine, startColumn);
    }

    private void SkipWhitespaceAndComments()
    {
        while (!IsAtEnd())
        {
            var ch = Current();
            if (char.IsWhiteSpace(ch))
            {
                Advance();
            }
            else if (ch == '-' && Peek(1) == '-')
            {
                // Single-line comment
                while (!IsAtEnd() && Current() != '\n')
                    Advance();
            }
            else if (ch == '/' && Peek(1) == '*')
            {
                // Multi-line comment
                Advance(); // /
                Advance(); // *
                while (!IsAtEnd())
                {
                    if (Current() == '*' && Peek(1) == '/')
                    {
                        Advance(); // *
                        Advance(); // /
                        break;
                    }
                    Advance();
                }
            }
            else
            {
                break;
            }
        }
    }

    private Token ReadString(char quote, int startLine, int startColumn)
    {
        var sb = new StringBuilder();
        Advance(); // Opening quote

        while (!IsAtEnd())
        {
            var ch = Current();
            if (ch == quote)
            {
                if (Peek(1) == quote) // Escaped quote
                {
                    sb.Append(quote);
                    Advance();
                    Advance();
                }
                else
                {
                    Advance(); // Closing quote
                    break;
                }
            }
            else if (ch == '\\')
            {
                Advance();
                if (!IsAtEnd())
                {
                    var escaped = Current();
                    sb.Append(escaped switch
                    {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '\'' => '\'',
                        '"' => '"',
                        '0' => '\0',
                        _ => escaped
                    });
                    Advance();
                }
            }
            else
            {
                sb.Append(ch);
                Advance();
            }
        }

        return new Token(TokenType.String, sb.ToString(), startLine, startColumn);
    }

    private Token ReadQuotedIdentifier(int startLine, int startColumn)
    {
        var sb = new StringBuilder();
        Advance(); // Opening backtick

        while (!IsAtEnd())
        {
            var ch = Current();
            if (ch == '`')
            {
                if (Peek(1) == '`') // Escaped backtick
                {
                    sb.Append('`');
                    Advance();
                    Advance();
                }
                else
                {
                    Advance(); // Closing backtick
                    break;
                }
            }
            else
            {
                sb.Append(ch);
                Advance();
            }
        }

        return new Token(TokenType.QuotedIdentifier, sb.ToString(), startLine, startColumn);
    }

    private Token ReadNumber(int startLine, int startColumn)
    {
        var sb = new StringBuilder();
        var hasDecimal = false;
        var hasExponent = false;

        while (!IsAtEnd())
        {
            var ch = Current();
            if (char.IsDigit(ch))
            {
                sb.Append(ch);
                Advance();
            }
            else if (ch == '.' && !hasDecimal && !hasExponent)
            {
                hasDecimal = true;
                sb.Append(ch);
                Advance();
            }
            else if ((ch == 'e' || ch == 'E') && !hasExponent)
            {
                hasExponent = true;
                sb.Append(ch);
                Advance();
                if (!IsAtEnd() && (Current() == '+' || Current() == '-'))
                {
                    sb.Append(Current());
                    Advance();
                }
            }
            else
            {
                break;
            }
        }

        var tokenType = hasDecimal || hasExponent ? TokenType.Float : TokenType.Integer;
        return new Token(tokenType, sb.ToString(), startLine, startColumn);
    }

    private Token ReadIdentifierOrKeyword(int startLine, int startColumn)
    {
        var sb = new StringBuilder();
        while (!IsAtEnd())
        {
            var ch = Current();
            if (char.IsLetterOrDigit(ch) || ch == '_')
            {
                sb.Append(ch);
                Advance();
            }
            else
            {
                break;
            }
        }

        var value = sb.ToString();
        var tokenType = Keywords.TryGetValue(value, out var keyword) ? keyword : TokenType.Identifier;
        return new Token(tokenType, value, startLine, startColumn);
    }

    private Token ReadOperatorOrDelimiter(int startLine, int startColumn)
    {
        var ch = Current();
        Advance();

        return ch switch
        {
            '+' => new Token(TokenType.Plus, "+", startLine, startColumn),
            '-' when !IsAtEnd() && Current() == '>' => AdvanceAndReturn(new Token(TokenType.Arrow, "->", startLine, startColumn)),
            '-' => new Token(TokenType.Minus, "-", startLine, startColumn),
            '*' => new Token(TokenType.Star, "*", startLine, startColumn),
            '/' => new Token(TokenType.Slash, "/", startLine, startColumn),
            '%' => new Token(TokenType.Percent, "%", startLine, startColumn),
            '=' => new Token(TokenType.Equals, "=", startLine, startColumn),
            '!' when !IsAtEnd() && Current() == '=' => AdvanceAndReturn(new Token(TokenType.NotEquals, "!=", startLine, startColumn)),
            '<' when !IsAtEnd() && Current() == '=' => AdvanceAndReturn(new Token(TokenType.LessThanOrEqual, "<=", startLine, startColumn)),
            '<' when !IsAtEnd() && Current() == '>' => AdvanceAndReturn(new Token(TokenType.NotEquals, "<>", startLine, startColumn)),
            '<' => new Token(TokenType.LessThan, "<", startLine, startColumn),
            '>' when !IsAtEnd() && Current() == '=' => AdvanceAndReturn(new Token(TokenType.GreaterThanOrEqual, ">=", startLine, startColumn)),
            '>' => new Token(TokenType.GreaterThan, ">", startLine, startColumn),
            '|' when !IsAtEnd() && Current() == '|' => AdvanceAndReturn(new Token(TokenType.Concat, "||", startLine, startColumn)),
            ':' when !IsAtEnd() && Current() == ':' => AdvanceAndReturn(new Token(TokenType.DoubleColon, "::", startLine, startColumn)),
            ':' => new Token(TokenType.Colon, ":", startLine, startColumn),
            '?' => new Token(TokenType.QuestionMark, "?", startLine, startColumn),
            '(' => new Token(TokenType.LeftParen, "(", startLine, startColumn),
            ')' => new Token(TokenType.RightParen, ")", startLine, startColumn),
            '[' => new Token(TokenType.LeftBracket, "[", startLine, startColumn),
            ']' => new Token(TokenType.RightBracket, "]", startLine, startColumn),
            '{' => new Token(TokenType.LeftBrace, "{", startLine, startColumn),
            '}' => new Token(TokenType.RightBrace, "}", startLine, startColumn),
            ',' => new Token(TokenType.Comma, ",", startLine, startColumn),
            '.' => new Token(TokenType.Dot, ".", startLine, startColumn),
            ';' => new Token(TokenType.Semicolon, ";", startLine, startColumn),
            _ => Token.Invalid(ch.ToString(), startLine, startColumn)
        };
    }

    private Token AdvanceAndReturn(Token token)
    {
        Advance();
        return token;
    }

    private char Current() => _source[_position];

    private char? Peek(int offset)
    {
        var pos = _position + offset;
        return pos < _source.Length ? _source[pos] : null;
    }

    private void Advance()
    {
        if (_position < _source.Length)
        {
            if (_source[_position] == '\n')
            {
                _line++;
                _column = 1;
            }
            else
            {
                _column++;
            }
            _position++;
        }
    }

    private bool IsAtEnd() => _position >= _source.Length;
}
