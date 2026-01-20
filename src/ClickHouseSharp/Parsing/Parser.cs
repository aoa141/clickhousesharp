using ClickHouseSharp.Parsing.Ast;

namespace ClickHouseSharp.Parsing;

public class Parser
{
    private readonly List<Token> _tokens;
    private int _position;

    public Parser(List<Token> tokens)
    {
        _tokens = tokens ?? throw new ArgumentNullException(nameof(tokens));
    }

    public static Parser FromSql(string sql)
    {
        var lexer = new Lexer(sql);
        return new Parser(lexer.Tokenize());
    }

    public Statement ParseStatement()
    {
        var stmt = ParseStatementInternal();

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        while (MatchAny(TokenType.Union, TokenType.Intersect, TokenType.Except))
        {
            var op = Previous().Type switch
            {
                TokenType.Union => SetOperationType.Union,
                TokenType.Intersect => SetOperationType.Intersect,
                TokenType.Except => SetOperationType.Except,
                _ => throw new InvalidOperationException()
            };

            var all = Match(TokenType.All);
            if (!all) Match(TokenType.Distinct); // Optional DISTINCT

            var right = ParseStatementInternal();
            stmt = new SetOperationStatement(stmt, op, all, right);
        }

        return stmt;
    }

    private Statement ParseStatementInternal()
    {
        if (Match(TokenType.With))
            return ParseSelectWithCte();
        if (Check(TokenType.Select))
            return ParseSelect();
        if (Match(TokenType.Insert))
            return ParseInsert();
        if (Match(TokenType.Create))
            return ParseCreate();
        if (Match(TokenType.Drop))
            return ParseDrop();
        if (Match(TokenType.Update))
            return ParseUpdate();
        if (Match(TokenType.Delete))
            return ParseDelete();
        if (Match(TokenType.LeftParen))
        {
            var stmt = ParseStatement();
            Consume(TokenType.RightParen, "Expected ')' after subquery");
            return stmt;
        }

        throw new ParseException($"Unexpected token: {Current()}", Current().Line, Current().Column);
    }

    private SelectStatement ParseSelectWithCte()
    {
        var ctes = new List<CteDefinition>();

        do
        {
            var name = ConsumeIdentifier("Expected CTE name");
            IReadOnlyList<string>? columns = null;

            if (Match(TokenType.LeftParen))
            {
                var cols = new List<string>();
                do
                {
                    cols.Add(ConsumeIdentifier("Expected column name"));
                } while (Match(TokenType.Comma));
                Consume(TokenType.RightParen, "Expected ')' after CTE columns");
                columns = cols;
            }

            Consume(TokenType.As, "Expected AS after CTE name");
            Consume(TokenType.LeftParen, "Expected '(' before CTE query");
            var query = ParseSelect();
            Consume(TokenType.RightParen, "Expected ')' after CTE query");

            ctes.Add(new CteDefinition(name, columns, query));
        } while (Match(TokenType.Comma));

        var select = ParseSelect();
        return select with { With = ctes };
    }

    private SelectStatement ParseSelect()
    {
        Consume(TokenType.Select, "Expected SELECT");

        var distinct = Match(TokenType.Distinct);

        var columns = ParseSelectList();

        TableReference? from = null;
        if (Match(TokenType.From))
        {
            from = ParseTableReference();
        }

        Expression? where = null;
        if (Match(TokenType.Where))
        {
            where = ParseExpression();
        }

        IReadOnlyList<Expression>? groupBy = null;
        if (Match(TokenType.Group))
        {
            Consume(TokenType.By, "Expected BY after GROUP");
            groupBy = ParseExpressionList();
        }

        Expression? having = null;
        if (Match(TokenType.Having))
        {
            having = ParseExpression();
        }

        IReadOnlyList<OrderByItem>? orderBy = null;
        if (Match(TokenType.Order))
        {
            Consume(TokenType.By, "Expected BY after ORDER");
            orderBy = ParseOrderByList();
        }

        Expression? limit = null;
        Expression? offset = null;
        if (Match(TokenType.Limit))
        {
            limit = ParseExpression();
            if (Match(TokenType.Offset))
            {
                offset = ParseExpression();
            }
            else if (Match(TokenType.Comma))
            {
                // LIMIT offset, count syntax
                offset = limit;
                limit = ParseExpression();
            }
        }

        return new SelectStatement(columns, from, where, groupBy, having, orderBy, limit, offset, distinct, null);
    }

    private IReadOnlyList<Expression> ParseSelectList()
    {
        var columns = new List<Expression>();
        do
        {
            columns.Add(ParseSelectItem());
        } while (Match(TokenType.Comma));
        return columns;
    }

    private Expression ParseSelectItem()
    {
        var expr = ParseExpression();

        if (Match(TokenType.As))
        {
            var alias = ConsumeIdentifier("Expected alias after AS");
            return new AliasedExpression(expr, alias);
        }

        // Allow alias without AS for identifiers
        if (Check(TokenType.Identifier) && !IsKeyword(Current()))
        {
            var alias = ConsumeIdentifier("Expected alias");
            return new AliasedExpression(expr, alias);
        }

        return expr;
    }

    private IReadOnlyList<Expression> ParseExpressionList()
    {
        var expressions = new List<Expression>();
        do
        {
            expressions.Add(ParseExpression());
        } while (Match(TokenType.Comma));
        return expressions;
    }

    private IReadOnlyList<OrderByItem> ParseOrderByList()
    {
        var items = new List<OrderByItem>();
        do
        {
            var expr = ParseExpression();
            var desc = false;
            if (Match(TokenType.Asc)) { }
            else if (Match(TokenType.Desc)) desc = true;

            NullsPosition? nulls = null;
            // ClickHouse doesn't have NULLS FIRST/LAST but we support it anyway

            items.Add(new OrderByItem(expr, desc, nulls));
        } while (Match(TokenType.Comma));
        return items;
    }

    private TableReference ParseTableReference()
    {
        var left = ParseTablePrimary();

        while (true)
        {
            if (MatchJoin(out var joinType))
            {
                var right = ParseTablePrimary();
                Expression? condition = null;
                IReadOnlyList<string>? usingCols = null;

                if (Match(TokenType.On))
                {
                    condition = ParseExpression();
                }
                else if (Match(TokenType.Using))
                {
                    Consume(TokenType.LeftParen, "Expected '(' after USING");
                    var cols = new List<string>();
                    do
                    {
                        cols.Add(ConsumeIdentifier("Expected column name"));
                    } while (Match(TokenType.Comma));
                    Consume(TokenType.RightParen, "Expected ')' after USING columns");
                    usingCols = cols;
                }

                left = new JoinReference(left, joinType, right, condition, usingCols);
            }
            else if (Match(TokenType.Comma))
            {
                // Comma join (implicit cross join)
                var right = ParseTablePrimary();
                left = new JoinReference(left, JoinType.Cross, right, null, null);
            }
            else
            {
                break;
            }
        }

        return left;
    }

    private bool MatchJoin(out JoinType joinType)
    {
        joinType = JoinType.Inner;

        var global = Match(TokenType.Global);
        var any = Match(TokenType.Any);

        if (Match(TokenType.Inner))
        {
            Match(TokenType.Join);
            joinType = JoinType.Inner;
            return true;
        }
        if (Match(TokenType.Left))
        {
            if (Match(TokenType.Semi))
            {
                Match(TokenType.Join);
                joinType = JoinType.LeftSemi;
                return true;
            }
            if (Match(TokenType.Anti))
            {
                Match(TokenType.Join);
                joinType = JoinType.LeftAnti;
                return true;
            }
            Match(TokenType.Outer);
            Match(TokenType.Join);
            joinType = JoinType.Left;
            return true;
        }
        if (Match(TokenType.Right))
        {
            if (Match(TokenType.Semi))
            {
                Match(TokenType.Join);
                joinType = JoinType.RightSemi;
                return true;
            }
            if (Match(TokenType.Anti))
            {
                Match(TokenType.Join);
                joinType = JoinType.RightAnti;
                return true;
            }
            Match(TokenType.Outer);
            Match(TokenType.Join);
            joinType = JoinType.Right;
            return true;
        }
        if (Match(TokenType.Full))
        {
            Match(TokenType.Outer);
            Match(TokenType.Join);
            joinType = JoinType.Full;
            return true;
        }
        if (Match(TokenType.Cross))
        {
            Match(TokenType.Join);
            joinType = JoinType.Cross;
            return true;
        }
        if (Match(TokenType.Asof))
        {
            Match(TokenType.Join);
            joinType = JoinType.AsOf;
            return true;
        }
        if (Match(TokenType.Join))
        {
            joinType = JoinType.Inner;
            return true;
        }

        return false;
    }

    private TableReference ParseTablePrimary()
    {
        if (Match(TokenType.LeftParen))
        {
            if (Check(TokenType.Select) || Check(TokenType.With))
            {
                var query = ParseStatement();
                Consume(TokenType.RightParen, "Expected ')' after subquery");
                var alias = Match(TokenType.As) ? ConsumeIdentifier("Expected alias") : ConsumeIdentifier("Expected alias after subquery");
                return new SubqueryReference(query, alias);
            }
            else
            {
                var inner = ParseTableReference();
                Consume(TokenType.RightParen, "Expected ')'");
                return inner;
            }
        }

        var name = ConsumeIdentifier("Expected table name");

        // Check if it's a table function
        if (Match(TokenType.LeftParen))
        {
            var args = new List<Expression>();
            if (!Check(TokenType.RightParen))
            {
                do
                {
                    args.Add(ParseExpression());
                } while (Match(TokenType.Comma));
            }
            Consume(TokenType.RightParen, "Expected ')' after function arguments");

            string? alias = null;
            if (Match(TokenType.As))
                alias = ConsumeIdentifier("Expected alias");
            else if (Check(TokenType.Identifier) && !IsKeyword(Current()))
                alias = ConsumeIdentifier("Expected alias");

            return new TableFunctionReference(name, args, alias);
        }

        string? tableAlias = null;
        if (Match(TokenType.As))
            tableAlias = ConsumeIdentifier("Expected alias");
        else if (Check(TokenType.Identifier) && !IsKeyword(Current()))
            tableAlias = ConsumeIdentifier("Expected alias");

        return new TableNameReference(name, tableAlias);
    }

    private InsertStatement ParseInsert()
    {
        Consume(TokenType.Into, "Expected INTO after INSERT");
        var tableName = ConsumeIdentifier("Expected table name");

        IReadOnlyList<string>? columns = null;
        if (Match(TokenType.LeftParen))
        {
            var cols = new List<string>();
            do
            {
                cols.Add(ConsumeIdentifier("Expected column name"));
            } while (Match(TokenType.Comma));
            Consume(TokenType.RightParen, "Expected ')' after column list");
            columns = cols;
        }

        InsertSource source;
        if (Match(TokenType.Values))
        {
            var rows = new List<IReadOnlyList<Expression>>();
            do
            {
                Consume(TokenType.LeftParen, "Expected '(' before values");
                var values = ParseExpressionList();
                Consume(TokenType.RightParen, "Expected ')' after values");
                rows.Add(values);
            } while (Match(TokenType.Comma));
            source = new ValuesInsertSource(rows);
        }
        else if (Check(TokenType.Select) || Check(TokenType.With))
        {
            var query = ParseStatement() as SelectStatement
                ?? throw new ParseException("Expected SELECT after INSERT INTO", Current().Line, Current().Column);
            source = new SelectInsertSource(query);
        }
        else
        {
            throw new ParseException("Expected VALUES or SELECT after INSERT INTO", Current().Line, Current().Column);
        }

        return new InsertStatement(tableName, columns, source);
    }

    private Statement ParseCreate()
    {
        Consume(TokenType.Table, "Expected TABLE after CREATE");

        var ifNotExists = false;
        if (Match(TokenType.If))
        {
            Consume(TokenType.Not, "Expected NOT after IF");
            Consume(TokenType.Exists, "Expected EXISTS after NOT");
            ifNotExists = true;
        }

        var tableName = ConsumeIdentifier("Expected table name");
        Consume(TokenType.LeftParen, "Expected '(' after table name");

        var columns = new List<ColumnDefinition>();
        do
        {
            var colName = ConsumeIdentifier("Expected column name");
            var dataType = ParseDataType();

            Expression? defaultValue = null;
            var nullable = false;

            // Parse column constraints
            while (true)
            {
                if (Match(TokenType.Nullable))
                {
                    nullable = true;
                }
                else if (MatchIdentifier("DEFAULT"))
                {
                    defaultValue = ParseExpression();
                }
                else
                {
                    break;
                }
            }

            columns.Add(new ColumnDefinition(colName, dataType, defaultValue, nullable));
        } while (Match(TokenType.Comma));

        Consume(TokenType.RightParen, "Expected ')' after column definitions");

        string? engine = null;
        IReadOnlyList<string>? primaryKey = null;
        IReadOnlyList<string>? orderBy = null;

        // Parse ENGINE and other clauses
        while (!IsAtEnd() && !Check(TokenType.Semicolon))
        {
            if (MatchIdentifier("ENGINE"))
            {
                Match(TokenType.Equals);
                engine = ConsumeIdentifier("Expected engine name");
                if (Match(TokenType.LeftParen))
                {
                    // Skip engine parameters for now
                    var depth = 1;
                    while (depth > 0 && !IsAtEnd())
                    {
                        if (Match(TokenType.LeftParen)) depth++;
                        else if (Match(TokenType.RightParen)) depth--;
                        else Advance();
                    }
                }
            }
            else if (MatchIdentifier("PRIMARY") && MatchIdentifier("KEY"))
            {
                Consume(TokenType.LeftParen, "Expected '(' after PRIMARY KEY");
                var keys = new List<string>();
                do
                {
                    keys.Add(ConsumeIdentifier("Expected column name"));
                } while (Match(TokenType.Comma));
                Consume(TokenType.RightParen, "Expected ')' after PRIMARY KEY columns");
                primaryKey = keys;
            }
            else if (Match(TokenType.Order) && Match(TokenType.By))
            {
                Consume(TokenType.LeftParen, "Expected '(' after ORDER BY");
                var cols = new List<string>();
                do
                {
                    cols.Add(ConsumeIdentifier("Expected column name"));
                } while (Match(TokenType.Comma));
                Consume(TokenType.RightParen, "Expected ')' after ORDER BY columns");
                orderBy = cols;
            }
            else
            {
                break;
            }
        }

        return new CreateTableStatement(tableName, columns, ifNotExists, engine, primaryKey, orderBy);
    }

    private DataTypeNode ParseDataType()
    {
        var typeName = ConsumeIdentifier("Expected data type");

        IReadOnlyList<DataTypeNode>? typeParams = null;
        IReadOnlyList<int>? numericParams = null;

        if (Match(TokenType.LeftParen))
        {
            // Check if it's numeric parameters or type parameters
            if (Check(TokenType.Integer))
            {
                var nums = new List<int>();
                do
                {
                    var token = Consume(TokenType.Integer, "Expected integer");
                    nums.Add(int.Parse(token.Value));
                } while (Match(TokenType.Comma));
                numericParams = nums;
            }
            else
            {
                var types = new List<DataTypeNode>();
                do
                {
                    types.Add(ParseDataType());
                } while (Match(TokenType.Comma));
                typeParams = types;
            }
            Consume(TokenType.RightParen, "Expected ')' after type parameters");
        }

        return new DataTypeNode(typeName, typeParams, numericParams);
    }

    private DropTableStatement ParseDrop()
    {
        Consume(TokenType.Table, "Expected TABLE after DROP");

        var ifExists = false;
        if (Match(TokenType.If))
        {
            Consume(TokenType.Exists, "Expected EXISTS after IF");
            ifExists = true;
        }

        var tableName = ConsumeIdentifier("Expected table name");
        return new DropTableStatement(tableName, ifExists);
    }

    private UpdateStatement ParseUpdate()
    {
        var tableName = ConsumeIdentifier("Expected table name");
        Consume(TokenType.Set, "Expected SET after table name");

        var assignments = new List<Assignment>();
        do
        {
            var column = ConsumeIdentifier("Expected column name");
            Consume(TokenType.Equals, "Expected '=' in assignment");
            var value = ParseExpression();
            assignments.Add(new Assignment(column, value));
        } while (Match(TokenType.Comma));

        Expression? where = null;
        if (Match(TokenType.Where))
        {
            where = ParseExpression();
        }

        return new UpdateStatement(tableName, assignments, where);
    }

    private DeleteStatement ParseDelete()
    {
        Consume(TokenType.From, "Expected FROM after DELETE");
        var tableName = ConsumeIdentifier("Expected table name");

        Expression? where = null;
        if (Match(TokenType.Where))
        {
            where = ParseExpression();
        }

        return new DeleteStatement(tableName, where);
    }

    // Expression parsing using precedence climbing
    private Expression ParseExpression() => ParseOr();

    private Expression ParseOr()
    {
        var left = ParseAnd();
        while (Match(TokenType.Or))
        {
            var right = ParseAnd();
            left = new BinaryExpression(left, BinaryOperator.Or, right);
        }
        return left;
    }

    private Expression ParseAnd()
    {
        var left = ParseNot();
        while (Match(TokenType.And))
        {
            var right = ParseNot();
            left = new BinaryExpression(left, BinaryOperator.And, right);
        }
        return left;
    }

    private Expression ParseNot()
    {
        if (Match(TokenType.Not))
        {
            var operand = ParseNot();
            return new UnaryExpression(UnaryOperator.Not, operand);
        }
        return ParseComparison();
    }

    private Expression ParseComparison()
    {
        var left = ParseConcat();

        while (true)
        {
            if (Match(TokenType.Equals))
            {
                left = new BinaryExpression(left, BinaryOperator.Equal, ParseConcat());
            }
            else if (Match(TokenType.NotEquals))
            {
                left = new BinaryExpression(left, BinaryOperator.NotEqual, ParseConcat());
            }
            else if (Match(TokenType.LessThan))
            {
                left = new BinaryExpression(left, BinaryOperator.LessThan, ParseConcat());
            }
            else if (Match(TokenType.LessThanOrEqual))
            {
                left = new BinaryExpression(left, BinaryOperator.LessThanOrEqual, ParseConcat());
            }
            else if (Match(TokenType.GreaterThan))
            {
                left = new BinaryExpression(left, BinaryOperator.GreaterThan, ParseConcat());
            }
            else if (Match(TokenType.GreaterThanOrEqual))
            {
                left = new BinaryExpression(left, BinaryOperator.GreaterThanOrEqual, ParseConcat());
            }
            else if (Match(TokenType.Is))
            {
                var not = Match(TokenType.Not);
                Consume(TokenType.Null, "Expected NULL after IS");
                left = new UnaryExpression(not ? UnaryOperator.IsNotNull : UnaryOperator.IsNull, left);
            }
            else if (Check(TokenType.Not) && Peek(1)?.Type == TokenType.In)
            {
                Advance(); // NOT
                Advance(); // IN
                left = ParseInExpression(left, true);
            }
            else if (Match(TokenType.In))
            {
                left = ParseInExpression(left, false);
            }
            else if (Check(TokenType.Not) && Peek(1)?.Type == TokenType.Between)
            {
                Advance(); // NOT
                Advance(); // BETWEEN
                left = ParseBetweenExpression(left, true);
            }
            else if (Match(TokenType.Between))
            {
                left = ParseBetweenExpression(left, false);
            }
            else if (Check(TokenType.Not) && Peek(1)?.Type == TokenType.Like)
            {
                Advance(); // NOT
                Advance(); // LIKE
                left = new BinaryExpression(left, BinaryOperator.NotLike, ParseConcat());
            }
            else if (Match(TokenType.Like))
            {
                left = new BinaryExpression(left, BinaryOperator.Like, ParseConcat());
            }
            else if (Check(TokenType.Not) && Peek(1)?.Type == TokenType.ILike)
            {
                Advance(); // NOT
                Advance(); // ILIKE
                left = new BinaryExpression(left, BinaryOperator.NotILike, ParseConcat());
            }
            else if (Match(TokenType.ILike))
            {
                left = new BinaryExpression(left, BinaryOperator.ILike, ParseConcat());
            }
            else
            {
                break;
            }
        }

        return left;
    }

    private Expression ParseInExpression(Expression left, bool not)
    {
        Consume(TokenType.LeftParen, "Expected '(' after IN");

        if (Check(TokenType.Select) || Check(TokenType.With))
        {
            var subquery = ParseStatement() as SelectStatement
                ?? throw new ParseException("Expected SELECT in IN subquery", Current().Line, Current().Column);
            Consume(TokenType.RightParen, "Expected ')' after IN subquery");
            return new InExpression(left, null, subquery, not);
        }

        var values = ParseExpressionList();
        Consume(TokenType.RightParen, "Expected ')' after IN values");
        return new InExpression(left, values, null, not);
    }

    private Expression ParseBetweenExpression(Expression left, bool not)
    {
        var low = ParseConcat();
        Consume(TokenType.And, "Expected AND in BETWEEN expression");
        var high = ParseConcat();
        return new BetweenExpression(left, low, high, not);
    }

    private Expression ParseConcat()
    {
        var left = ParseAdditive();
        while (Match(TokenType.Concat))
        {
            left = new BinaryExpression(left, BinaryOperator.Concat, ParseAdditive());
        }
        return left;
    }

    private Expression ParseAdditive()
    {
        var left = ParseMultiplicative();
        while (true)
        {
            if (Match(TokenType.Plus))
                left = new BinaryExpression(left, BinaryOperator.Add, ParseMultiplicative());
            else if (Match(TokenType.Minus))
                left = new BinaryExpression(left, BinaryOperator.Subtract, ParseMultiplicative());
            else
                break;
        }
        return left;
    }

    private Expression ParseMultiplicative()
    {
        var left = ParseUnary();
        while (true)
        {
            if (Match(TokenType.Star))
                left = new BinaryExpression(left, BinaryOperator.Multiply, ParseUnary());
            else if (Match(TokenType.Slash))
                left = new BinaryExpression(left, BinaryOperator.Divide, ParseUnary());
            else if (Match(TokenType.Percent))
                left = new BinaryExpression(left, BinaryOperator.Modulo, ParseUnary());
            else
                break;
        }
        return left;
    }

    private Expression ParseUnary()
    {
        if (Match(TokenType.Minus))
            return new UnaryExpression(UnaryOperator.Negate, ParseUnary());
        if (Match(TokenType.Plus))
            return ParseUnary();
        return ParsePostfix();
    }

    private Expression ParsePostfix()
    {
        var expr = ParsePrimary();

        while (true)
        {
            if (Match(TokenType.LeftBracket))
            {
                var index = ParseExpression();
                Consume(TokenType.RightBracket, "Expected ']' after index");
                expr = new IndexExpression(expr, index);
            }
            else if (Match(TokenType.Dot))
            {
                var member = ConsumeIdentifier("Expected member name after '.'");
                if (expr is ColumnExpression col && col.TableName == null)
                {
                    expr = new ColumnExpression(col.ColumnName, member);
                }
                else
                {
                    // Treat as function call on tuple/nested structure
                    expr = new FunctionCallExpression("tupleElement", [expr, new LiteralExpression(member, LiteralType.String)]);
                }
            }
            else if (Match(TokenType.DoubleColon))
            {
                // Type cast: expr::type
                var targetType = ParseDataType();
                expr = new CastExpression(expr, targetType);
            }
            else if (Match(TokenType.Over))
            {
                // Window function
                if (expr is FunctionCallExpression func)
                {
                    var window = ParseWindowSpec();
                    expr = new WindowFunctionExpression(func, window);
                }
                else
                {
                    throw new ParseException("OVER can only be applied to a function call", Current().Line, Current().Column);
                }
            }
            else
            {
                break;
            }
        }

        return expr;
    }

    private WindowSpec ParseWindowSpec()
    {
        Consume(TokenType.LeftParen, "Expected '(' after OVER");

        IReadOnlyList<Expression>? partitionBy = null;
        IReadOnlyList<OrderByItem>? orderBy = null;
        WindowFrame? frame = null;

        if (Match(TokenType.Partition))
        {
            Consume(TokenType.By, "Expected BY after PARTITION");
            partitionBy = ParseExpressionList();
        }

        if (Match(TokenType.Order))
        {
            Consume(TokenType.By, "Expected BY after ORDER");
            orderBy = ParseOrderByList();
        }

        if (Match(TokenType.Rows) || Match(TokenType.Range))
        {
            var frameType = Previous().Type == TokenType.Rows ? WindowFrameType.Rows : WindowFrameType.Range;

            if (Match(TokenType.Between))
            {
                var start = ParseFrameBound();
                Consume(TokenType.And, "Expected AND in BETWEEN");
                var end = ParseFrameBound();
                frame = new WindowFrame(frameType, start, end);
            }
            else
            {
                var start = ParseFrameBound();
                frame = new WindowFrame(frameType, start, null);
            }
        }

        Consume(TokenType.RightParen, "Expected ')' after window specification");

        return new WindowSpec(partitionBy, orderBy, frame);
    }

    private WindowFrameBound ParseFrameBound()
    {
        if (Match(TokenType.Unbounded))
        {
            if (Match(TokenType.Preceding))
                return new WindowFrameBound(WindowFrameBoundType.UnboundedPreceding, null);
            Consume(TokenType.Following, "Expected PRECEDING or FOLLOWING after UNBOUNDED");
            return new WindowFrameBound(WindowFrameBoundType.UnboundedFollowing, null);
        }

        if (Match(TokenType.Current))
        {
            Consume(TokenType.Row, "Expected ROW after CURRENT");
            return new WindowFrameBound(WindowFrameBoundType.CurrentRow, null);
        }

        var offset = ParseExpression();
        if (Match(TokenType.Preceding))
            return new WindowFrameBound(WindowFrameBoundType.Preceding, offset);
        Consume(TokenType.Following, "Expected PRECEDING or FOLLOWING");
        return new WindowFrameBound(WindowFrameBoundType.Following, offset);
    }

    private Expression ParsePrimary()
    {
        // Literals
        if (Match(TokenType.Integer))
            return new LiteralExpression(long.Parse(Previous().Value), LiteralType.Integer);
        if (Match(TokenType.Float))
            return new LiteralExpression(double.Parse(Previous().Value), LiteralType.Float);
        if (Match(TokenType.String))
            return new LiteralExpression(Previous().Value, LiteralType.String);
        if (Match(TokenType.True))
            return new LiteralExpression(true, LiteralType.Boolean);
        if (Match(TokenType.False))
            return new LiteralExpression(false, LiteralType.Boolean);
        if (Match(TokenType.Null))
            return new LiteralExpression(null, LiteralType.Null);

        // Star expression
        if (Match(TokenType.Star))
            return new StarExpression(null);

        // Array literal
        if (Match(TokenType.LeftBracket))
        {
            var elements = new List<Expression>();
            if (!Check(TokenType.RightBracket))
            {
                do
                {
                    elements.Add(ParseExpression());
                } while (Match(TokenType.Comma));
            }
            Consume(TokenType.RightBracket, "Expected ']' after array elements");
            return new ArrayExpression(elements);
        }

        // Parenthesized expression, tuple, or subquery
        if (Match(TokenType.LeftParen))
        {
            if (Check(TokenType.Select) || Check(TokenType.With))
            {
                var subquery = ParseStatement() as SelectStatement
                    ?? throw new ParseException("Expected SELECT in subquery", Current().Line, Current().Column);
                Consume(TokenType.RightParen, "Expected ')' after subquery");
                return new SubqueryExpression(subquery);
            }

            var first = ParseExpression();
            if (Match(TokenType.Comma))
            {
                // Tuple
                var elements = new List<Expression> { first };
                do
                {
                    elements.Add(ParseExpression());
                } while (Match(TokenType.Comma));
                Consume(TokenType.RightParen, "Expected ')' after tuple elements");
                return new TupleExpression(elements);
            }

            Consume(TokenType.RightParen, "Expected ')'");
            return first;
        }

        // CASE expression
        if (Match(TokenType.Case))
            return ParseCaseExpression();

        // CAST expression
        if (Match(TokenType.Cast))
            return ParseCastExpression();

        // EXISTS expression
        if (Match(TokenType.Exists))
        {
            Consume(TokenType.LeftParen, "Expected '(' after EXISTS");
            var subquery = ParseStatement() as SelectStatement
                ?? throw new ParseException("Expected SELECT in EXISTS", Current().Line, Current().Column);
            Consume(TokenType.RightParen, "Expected ')' after EXISTS subquery");
            return new ExistsExpression(subquery);
        }

        // Identifier (column or function call)
        if (Check(TokenType.Identifier) || Check(TokenType.QuotedIdentifier))
        {
            var name = ConsumeIdentifier("Expected identifier");

            // Check for table.column or function call
            if (Match(TokenType.Dot))
            {
                if (Match(TokenType.Star))
                    return new StarExpression(name);
                var colName = ConsumeIdentifier("Expected column name");
                return new ColumnExpression(name, colName);
            }

            if (Match(TokenType.LeftParen))
            {
                // Function call
                var distinct = Match(TokenType.Distinct);
                var args = new List<Expression>();
                if (!Check(TokenType.RightParen))
                {
                    do
                    {
                        // Check for lambda: x -> expr or (x, y) -> expr
                        if (TryParseLambda(out var lambda))
                        {
                            args.Add(lambda!);
                        }
                        else
                        {
                            args.Add(ParseExpression());
                        }
                    } while (Match(TokenType.Comma));
                }
                Consume(TokenType.RightParen, "Expected ')' after function arguments");
                return new FunctionCallExpression(name, args, distinct);
            }

            return new ColumnExpression(null, name);
        }

        // Handle keywords that might be used as identifiers/functions (ClickHouse allows this)
        if (IsKeywordAsIdentifier())
        {
            var name = Current().Value;
            Advance();

            // Check for function call
            if (Match(TokenType.LeftParen))
            {
                var args = new List<Expression>();
                if (!Check(TokenType.RightParen))
                {
                    do
                    {
                        args.Add(ParseExpression());
                    } while (Match(TokenType.Comma));
                }
                Consume(TokenType.RightParen, "Expected ')' after function arguments");
                return new FunctionCallExpression(name, args);
            }

            return new ColumnExpression(null, name);
        }

        throw new ParseException($"Unexpected token: {Current()}", Current().Line, Current().Column);
    }

    private bool TryParseLambda(out Expression? lambda)
    {
        lambda = null;
        var savedPos = _position;

        try
        {
            var parameters = new List<string>();

            if (Check(TokenType.Identifier))
            {
                // Single parameter: x -> expr
                parameters.Add(ConsumeIdentifier(""));
                if (!Match(TokenType.Arrow))
                {
                    _position = savedPos;
                    return false;
                }
            }
            else if (Match(TokenType.LeftParen))
            {
                // Multiple parameters: (x, y) -> expr
                if (!Check(TokenType.RightParen))
                {
                    do
                    {
                        parameters.Add(ConsumeIdentifier(""));
                    } while (Match(TokenType.Comma));
                }
                Consume(TokenType.RightParen, "");
                if (!Match(TokenType.Arrow))
                {
                    _position = savedPos;
                    return false;
                }
            }
            else
            {
                return false;
            }

            var body = ParseExpression();
            lambda = new LambdaExpression(parameters, body);
            return true;
        }
        catch
        {
            _position = savedPos;
            return false;
        }
    }

    private Expression ParseCaseExpression()
    {
        Expression? operand = null;
        if (!Check(TokenType.When))
        {
            operand = ParseExpression();
        }

        var whenClauses = new List<WhenClause>();
        while (Match(TokenType.When))
        {
            var condition = ParseExpression();
            Consume(TokenType.Then, "Expected THEN after WHEN condition");
            var result = ParseExpression();
            whenClauses.Add(new WhenClause(condition, result));
        }

        Expression? elseResult = null;
        if (Match(TokenType.Else))
        {
            elseResult = ParseExpression();
        }

        Consume(TokenType.End, "Expected END after CASE expression");

        return new CaseExpression(operand, whenClauses, elseResult);
    }

    private Expression ParseCastExpression()
    {
        Consume(TokenType.LeftParen, "Expected '(' after CAST");
        var operand = ParseExpression();
        Consume(TokenType.As, "Expected AS in CAST");
        var targetType = ParseDataType();
        Consume(TokenType.RightParen, "Expected ')' after CAST");
        return new CastExpression(operand, targetType);
    }

    // Helper methods
    private Token Current() => _position < _tokens.Count ? _tokens[_position] : Token.Eof(0, 0);
    private Token Previous() => _tokens[_position - 1];
    private Token? Peek(int offset) => _position + offset < _tokens.Count ? _tokens[_position + offset] : null;
    private bool IsAtEnd() => Current().Type == TokenType.Eof;

    private bool Check(TokenType type) => !IsAtEnd() && Current().Type == type;

    private bool Match(TokenType type)
    {
        if (Check(type))
        {
            Advance();
            return true;
        }
        return false;
    }

    private bool MatchAny(params TokenType[] types)
    {
        foreach (var type in types)
        {
            if (Check(type))
            {
                Advance();
                return true;
            }
        }
        return false;
    }

    private bool MatchIdentifier(string name)
    {
        if (Check(TokenType.Identifier) && Current().Value.Equals(name, StringComparison.OrdinalIgnoreCase))
        {
            Advance();
            return true;
        }
        return false;
    }

    private void Advance()
    {
        if (!IsAtEnd()) _position++;
    }

    private Token Consume(TokenType type, string message)
    {
        if (Check(type)) return _tokens[_position++];
        throw new ParseException($"{message}, got {Current()}", Current().Line, Current().Column);
    }

    private string ConsumeIdentifier(string message)
    {
        if (Check(TokenType.Identifier) || Check(TokenType.QuotedIdentifier))
            return _tokens[_position++].Value;

        // Allow some keywords as identifiers
        if (IsKeywordAsIdentifier())
        {
            var value = Current().Value;
            Advance();
            return value;
        }

        throw new ParseException($"{message}, got {Current()}", Current().Line, Current().Column);
    }

    private bool IsKeyword(Token token)
    {
        return token.Type >= TokenType.Select && token.Type <= TokenType.Final;
    }

    private bool IsKeywordAsIdentifier()
    {
        var type = Current().Type;
        // Allow type keywords and certain others to be used as identifiers/functions
        return type switch
        {
            TokenType.Array => true,
            TokenType.Tuple => true,
            TokenType.Map => true,
            TokenType.Nullable => true,
            TokenType.Format => true,
            TokenType.Settings => true,
            TokenType.Final => true,
            TokenType.Range => true,
            TokenType.Rows => true,
            TokenType.Any => true,
            TokenType.Global => true,
            TokenType.If => true,  // if() function
            TokenType.Values => true,  // "values" can be used as alias
            _ => false
        };
    }
}

public class ParseException : Exception
{
    public int Line { get; }
    public int Column { get; }

    public ParseException(string message, int line, int column)
        : base($"{message} at line {line}, column {column}")
    {
        Line = line;
        Column = column;
    }
}
