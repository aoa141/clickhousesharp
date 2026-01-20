using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class AggregationTests
{
    [Fact]
    public void Count_AllRows()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT count() FROM numbers(10)");

        Assert.Single(result.Rows);
        Assert.Equal(10L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void Count_NonNull()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Nullable(Int64))");
        ch.Execute("INSERT INTO test VALUES (1), (NULL), (2), (NULL), (3)");

        var result = ch.Execute("SELECT count(value) FROM test");

        Assert.Single(result.Rows);
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void Sum_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT sum(number) FROM numbers(10)");

        Assert.Single(result.Rows);
        // 0+1+2+3+4+5+6+7+8+9 = 45
        Assert.Equal(45.0, result.Rows[0][0].AsFloat64());
    }

    [Fact]
    public void Avg_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT avg(number) FROM numbers(10)");

        Assert.Single(result.Rows);
        // Average of 0-9 = 4.5
        Assert.Equal(4.5, result.Rows[0][0].AsFloat64());
    }

    [Fact]
    public void MinMax_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT min(number), max(number) FROM numbers(10)");

        Assert.Single(result.Rows);
        Assert.Equal(0UL, result.Rows[0][0].As<ulong>());
        Assert.Equal(9UL, result.Rows[0][1].As<ulong>());
    }

    [Fact]
    public void GroupBy_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE sales (category String, amount Int64)");
        ch.Execute(@"INSERT INTO sales VALUES
            ('A', 100), ('B', 200), ('A', 150),
            ('B', 300), ('A', 50), ('C', 400)");

        var result = ch.Execute(@"
            SELECT category, sum(amount) AS total
            FROM sales
            GROUP BY category
            ORDER BY category
        ");

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("A", result.Rows[0][0].AsString());
        Assert.Equal(300.0, result.Rows[0][1].AsFloat64());
        Assert.Equal("B", result.Rows[1][0].AsString());
        Assert.Equal(500.0, result.Rows[1][1].AsFloat64());
        Assert.Equal("C", result.Rows[2][0].AsString());
        Assert.Equal(400.0, result.Rows[2][1].AsFloat64());
    }

    [Fact]
    public void GroupBy_MultipleColumns()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE data (year Int64, quarter Int64, revenue Int64)");
        ch.Execute(@"INSERT INTO data VALUES
            (2023, 1, 100), (2023, 1, 200), (2023, 2, 300),
            (2024, 1, 150), (2024, 2, 250)");

        var result = ch.Execute(@"
            SELECT year, quarter, sum(revenue) AS total
            FROM data
            GROUP BY year, quarter
            ORDER BY year, quarter
        ");

        Assert.Equal(4, result.Rows.Count);
    }

    [Fact]
    public void GroupArray_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (category String, value Int64)");
        ch.Execute("INSERT INTO test VALUES ('A', 1), ('A', 2), ('B', 3), ('A', 4)");

        var result = ch.Execute(@"
            SELECT category, groupArray(value) AS values
            FROM test
            GROUP BY category
            ORDER BY category
        ");

        Assert.Equal(2, result.Rows.Count);
    }

    [Fact]
    public void Uniq_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Int64)");
        ch.Execute("INSERT INTO test VALUES (1), (2), (1), (3), (2), (1)");

        var result = ch.Execute("SELECT uniq(value) FROM test");

        Assert.Single(result.Rows);
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void CountDistinct()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Int64)");
        ch.Execute("INSERT INTO test VALUES (1), (2), (1), (3), (2), (1)");

        var result = ch.Execute("SELECT count(DISTINCT value) FROM test");

        Assert.Single(result.Rows);
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void ArgMin_ArgMax()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (name String, score Int64)");
        ch.Execute("INSERT INTO test VALUES ('Alice', 85), ('Bob', 92), ('Charlie', 78)");

        var minResult = ch.Execute("SELECT argMin(name, score) FROM test");
        var maxResult = ch.Execute("SELECT argMax(name, score) FROM test");

        Assert.Equal("Charlie", minResult.Rows[0][0].AsString());
        Assert.Equal("Bob", maxResult.Rows[0][0].AsString());
    }

    [Fact]
    public void SumIf_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Int64, active Bool)");
        ch.Execute("INSERT INTO test VALUES (10, true), (20, false), (30, true), (40, false)");

        var result = ch.Execute("SELECT sumIf(value, active) FROM test");

        Assert.Single(result.Rows);
        Assert.Equal(40.0, result.Rows[0][0].AsFloat64());
    }

    [Fact]
    public void CountIf_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Int64)");
        ch.Execute("INSERT INTO test VALUES (1), (5), (10), (15), (20)");

        var result = ch.Execute("SELECT countIf(value > 5) FROM test");

        Assert.Single(result.Rows);
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
    }
}
