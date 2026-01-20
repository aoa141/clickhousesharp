using ClickHouseSharp;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Tests;

public class BasicSelectTests
{
    [Fact]
    public void Select_LiteralValues()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT 1, 2, 3");

        Assert.Single(result.Rows);
        Assert.Equal(3, result.Columns.Count);
        Assert.Equal(1L, result.Rows[0][0].AsInt64());
        Assert.Equal(2L, result.Rows[0][1].AsInt64());
        Assert.Equal(3L, result.Rows[0][2].AsInt64());
    }

    [Fact]
    public void Select_StringLiteral()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT 'hello world'");

        Assert.Single(result.Rows);
        Assert.Equal("hello world", result.Rows[0][0].AsString());
    }

    [Fact]
    public void Select_Arithmetic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT 1 + 2, 10 - 3, 4 * 5, 20 / 4");

        Assert.Single(result.Rows);
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
        Assert.Equal(7L, result.Rows[0][1].AsInt64());
        Assert.Equal(20L, result.Rows[0][2].AsInt64());
        Assert.Equal(5.0, result.Rows[0][3].AsFloat64());
    }

    [Fact]
    public void Select_WithAlias()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT 42 AS answer, 'test' AS text");

        Assert.Single(result.Rows);
        Assert.Equal("answer", result.Columns[0].Name);
        Assert.Equal("text", result.Columns[1].Name);
        Assert.Equal(42L, result.Rows[0][0].AsInt64());
        Assert.Equal("test", result.Rows[0][1].AsString());
    }

    [Fact]
    public void Select_BooleanExpressions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT 1 = 1, 1 = 2, 1 < 2, 1 > 2");

        Assert.Single(result.Rows);
        Assert.True(result.Rows[0][0].AsBool());
        Assert.False(result.Rows[0][1].AsBool());
        Assert.True(result.Rows[0][2].AsBool());
        Assert.False(result.Rows[0][3].AsBool());
    }

    [Fact]
    public void Select_NullHandling()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT NULL, NULL IS NULL, NULL IS NOT NULL");

        Assert.Single(result.Rows);
        Assert.True(result.Rows[0][0].IsNull);
        Assert.True(result.Rows[0][1].AsBool());
        Assert.False(result.Rows[0][2].AsBool());
    }

    [Fact]
    public void Select_FromNumbers()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(5)");

        Assert.Equal(5, result.Rows.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal((ulong)i, result.Rows[i][0].As<ulong>());
        }
    }

    [Fact]
    public void Select_WithWhere()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(10) WHERE number > 5");

        Assert.Equal(4, result.Rows.Count);
        Assert.Equal(6UL, result.Rows[0][0].As<ulong>());
        Assert.Equal(9UL, result.Rows[3][0].As<ulong>());
    }

    [Fact]
    public void Select_WithOrderBy()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(5) ORDER BY number DESC");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(4UL, result.Rows[0][0].As<ulong>());
        Assert.Equal(0UL, result.Rows[4][0].As<ulong>());
    }

    [Fact]
    public void Select_WithLimit()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(100) LIMIT 5");

        Assert.Equal(5, result.Rows.Count);
    }

    [Fact]
    public void Select_WithOffset()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(10) LIMIT 3 OFFSET 5");

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal(5UL, result.Rows[0][0].As<ulong>());
    }

    [Fact]
    public void Select_WithDistinct()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Int64)");
        ch.Execute("INSERT INTO test VALUES (1), (2), (1), (3), (2), (1)");

        var result = ch.Execute("SELECT DISTINCT value FROM test ORDER BY value");

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal(1L, result.Rows[0][0].AsInt64());
        Assert.Equal(2L, result.Rows[1][0].AsInt64());
        Assert.Equal(3L, result.Rows[2][0].AsInt64());
    }

    [Fact]
    public void Select_CaseExpression()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                CASE
                    WHEN 1 = 1 THEN 'yes'
                    ELSE 'no'
                END AS result
        ");

        Assert.Single(result.Rows);
        Assert.Equal("yes", result.Rows[0][0].AsString());
    }

    [Fact]
    public void Select_InExpression()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(10) WHERE number IN (1, 3, 5, 7)");

        Assert.Equal(4, result.Rows.Count);
    }

    [Fact]
    public void Select_BetweenExpression()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT number FROM numbers(10) WHERE number BETWEEN 3 AND 7");

        Assert.Equal(5, result.Rows.Count);
    }

    [Fact]
    public void Select_LikeExpression()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (name String)");
        ch.Execute("INSERT INTO test VALUES ('apple'), ('banana'), ('apricot'), ('cherry')");

        var result = ch.Execute("SELECT name FROM test WHERE name LIKE 'ap%' ORDER BY name");

        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("apple", result.Rows[0][0].AsString());
        Assert.Equal("apricot", result.Rows[1][0].AsString());
    }
}
