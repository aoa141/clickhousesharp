using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class CteTests
{
    [Fact]
    public void Cte_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            WITH doubled AS (
                SELECT number * 2 AS value FROM numbers(5)
            )
            SELECT value FROM doubled ORDER BY value
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(0L, result.Rows[0][0].AsInt64());
        Assert.Equal(8L, result.Rows[4][0].AsInt64());
    }

    [Fact]
    public void Cte_MultipleCtes()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            WITH
                evens AS (SELECT number * 2 AS n FROM numbers(5)),
                odds AS (SELECT number * 2 + 1 AS n FROM numbers(5))
            SELECT * FROM (
                SELECT n FROM evens
                UNION ALL
                SELECT n FROM odds
            ) combined
            ORDER BY n
        ");

        Assert.Equal(10, result.Rows.Count);
    }

    [Fact]
    public void Cte_ReferencedMultipleTimes()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            WITH base AS (
                SELECT number AS n FROM numbers(3)
            )
            SELECT a.n AS x, b.n AS y
            FROM base a
            CROSS JOIN base b
            ORDER BY x, y
        ");

        Assert.Equal(9, result.Rows.Count); // 3 * 3
    }

    [Fact]
    public void Cte_WithJoin()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE orders (id Int64, customer_id Int64, amount Int64)");
        ch.Execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)");

        var result = ch.Execute(@"
            WITH customer_totals AS (
                SELECT customer_id, sum(amount) AS total
                FROM orders
                GROUP BY customer_id
            )
            SELECT customer_id, total
            FROM customer_totals
            ORDER BY total DESC
        ");

        Assert.Equal(2, result.Rows.Count);
        Assert.Equal(1L, result.Rows[0][0].AsInt64()); // Customer 1 has 300 total
        Assert.Equal(300.0, result.Rows[0][1].AsFloat64());
    }

    [Fact]
    public void Cte_WithColumnList()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            WITH data(a, b) AS (
                SELECT 1, 2
            )
            SELECT a, b FROM data
        ");

        Assert.Single(result.Rows);
        Assert.Equal("a", result.Columns[0].Name);
        Assert.Equal("b", result.Columns[1].Name);
    }
}
