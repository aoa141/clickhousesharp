using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class WindowFunctionTests
{
    [Fact]
    public void RowNumber_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, row_number() OVER (ORDER BY number) AS rn
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(1L, result.Rows[0][1].AsInt64());
        Assert.Equal(2L, result.Rows[1][1].AsInt64());
        Assert.Equal(5L, result.Rows[4][1].AsInt64());
    }

    [Fact]
    public void RowNumber_WithPartition()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (category String, value Int64)");
        ch.Execute("INSERT INTO test VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 200)");

        var result = ch.Execute(@"
            SELECT category, value, row_number() OVER (PARTITION BY category ORDER BY value) AS rn
            FROM test
            ORDER BY category, value
        ");

        Assert.Equal(5, result.Rows.Count);
        // Category A
        Assert.Equal("A", result.Rows[0][0].AsString());
        Assert.Equal(1L, result.Rows[0][2].AsInt64());
        Assert.Equal(2L, result.Rows[1][2].AsInt64());
        Assert.Equal(3L, result.Rows[2][2].AsInt64());
        // Category B
        Assert.Equal("B", result.Rows[3][0].AsString());
        Assert.Equal(1L, result.Rows[3][2].AsInt64());
        Assert.Equal(2L, result.Rows[4][2].AsInt64());
    }

    [Fact]
    public void Rank_WithTies()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (name String, score Int64)");
        ch.Execute("INSERT INTO test VALUES ('Alice', 90), ('Bob', 90), ('Charlie', 80), ('Dave', 80), ('Eve', 70)");

        var result = ch.Execute(@"
            SELECT name, score, rank() OVER (ORDER BY score DESC) AS rnk
            FROM test
            ORDER BY score DESC, name
        ");

        Assert.Equal(5, result.Rows.Count);
        // Alice and Bob tie at rank 1
        Assert.Equal(1L, result.Rows[0][2].AsInt64());
        Assert.Equal(1L, result.Rows[1][2].AsInt64());
        // Charlie and Dave are at rank 3 (skips 2)
        Assert.Equal(3L, result.Rows[2][2].AsInt64());
        Assert.Equal(3L, result.Rows[3][2].AsInt64());
        // Eve is at rank 5
        Assert.Equal(5L, result.Rows[4][2].AsInt64());
    }

    [Fact]
    public void DenseRank_WithTies()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (name String, score Int64)");
        ch.Execute("INSERT INTO test VALUES ('Alice', 90), ('Bob', 90), ('Charlie', 80), ('Dave', 80), ('Eve', 70)");

        var result = ch.Execute(@"
            SELECT name, score, dense_rank() OVER (ORDER BY score DESC) AS rnk
            FROM test
            ORDER BY score DESC, name
        ");

        Assert.Equal(5, result.Rows.Count);
        // Alice and Bob tie at rank 1
        Assert.Equal(1L, result.Rows[0][2].AsInt64());
        Assert.Equal(1L, result.Rows[1][2].AsInt64());
        // Charlie and Dave are at rank 2 (no gap)
        Assert.Equal(2L, result.Rows[2][2].AsInt64());
        Assert.Equal(2L, result.Rows[3][2].AsInt64());
        // Eve is at rank 3
        Assert.Equal(3L, result.Rows[4][2].AsInt64());
    }

    [Fact]
    public void Lag_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, lag(number) OVER (ORDER BY number) AS prev
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.True(result.Rows[0][1].IsNull); // First row has no previous
        Assert.Equal(0UL, result.Rows[1][1].As<ulong>()); // Previous of 1 is 0
        Assert.Equal(1UL, result.Rows[2][1].As<ulong>()); // Previous of 2 is 1
    }

    [Fact]
    public void Lag_WithOffset()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, lag(number, 2) OVER (ORDER BY number) AS prev2
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.True(result.Rows[0][1].IsNull); // First row has no prev-2
        Assert.True(result.Rows[1][1].IsNull); // Second row has no prev-2
        Assert.Equal(0UL, result.Rows[2][1].As<ulong>()); // Prev-2 of 2 is 0
        Assert.Equal(1UL, result.Rows[3][1].As<ulong>()); // Prev-2 of 3 is 1
    }

    [Fact]
    public void Lag_WithDefault()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, lag(number, 1, -1) OVER (ORDER BY number) AS prev
            FROM numbers(3)
        ");

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal(-1L, result.Rows[0][1].AsInt64()); // First row uses default
        Assert.Equal(0UL, result.Rows[1][1].As<ulong>()); // Previous of 1 is 0
    }

    [Fact]
    public void Lead_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, lead(number) OVER (ORDER BY number) AS next
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(1UL, result.Rows[0][1].As<ulong>()); // Next of 0 is 1
        Assert.Equal(2UL, result.Rows[1][1].As<ulong>()); // Next of 1 is 2
        Assert.True(result.Rows[4][1].IsNull); // Last row has no next
    }

    [Fact]
    public void FirstValue_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (category String, value Int64)");
        ch.Execute("INSERT INTO test VALUES ('A', 30), ('A', 10), ('A', 20), ('B', 200), ('B', 100)");

        var result = ch.Execute(@"
            SELECT category, value, first_value(value) OVER (PARTITION BY category ORDER BY value) AS first_val
            FROM test
            ORDER BY category, value
        ");

        Assert.Equal(5, result.Rows.Count);
        // Category A - first value is 10 (smallest)
        Assert.Equal(10L, result.Rows[0][2].AsInt64());
        Assert.Equal(10L, result.Rows[1][2].AsInt64());
        Assert.Equal(10L, result.Rows[2][2].AsInt64());
        // Category B - first value is 100
        Assert.Equal(100L, result.Rows[3][2].AsInt64());
        Assert.Equal(100L, result.Rows[4][2].AsInt64());
    }

    [Fact]
    public void SumOver_RunningTotal()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, sum(number) OVER (ORDER BY number) AS running_sum
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(0.0, result.Rows[0][1].AsFloat64()); // sum of 0
        Assert.Equal(1.0, result.Rows[1][1].AsFloat64()); // sum of 0+1
        Assert.Equal(3.0, result.Rows[2][1].AsFloat64()); // sum of 0+1+2
        Assert.Equal(6.0, result.Rows[3][1].AsFloat64()); // sum of 0+1+2+3
        Assert.Equal(10.0, result.Rows[4][1].AsFloat64()); // sum of 0+1+2+3+4
    }

    [Fact]
    public void SumOver_WithPartition()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE sales (category String, amount Int64)");
        ch.Execute("INSERT INTO sales VALUES ('A', 10), ('A', 20), ('A', 30), ('B', 100), ('B', 200)");

        var result = ch.Execute(@"
            SELECT category, amount, sum(amount) OVER (PARTITION BY category ORDER BY amount) AS running_sum
            FROM sales
            ORDER BY category, amount
        ");

        Assert.Equal(5, result.Rows.Count);
        // Category A running sums
        Assert.Equal(10.0, result.Rows[0][2].AsFloat64());
        Assert.Equal(30.0, result.Rows[1][2].AsFloat64()); // 10+20
        Assert.Equal(60.0, result.Rows[2][2].AsFloat64()); // 10+20+30
        // Category B running sums
        Assert.Equal(100.0, result.Rows[3][2].AsFloat64());
        Assert.Equal(300.0, result.Rows[4][2].AsFloat64()); // 100+200
    }

    [Fact]
    public void AvgOver_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, avg(number) OVER (ORDER BY number) AS running_avg
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(0.0, result.Rows[0][1].AsFloat64()); // avg of 0
        Assert.Equal(0.5, result.Rows[1][1].AsFloat64()); // avg of 0,1
        Assert.Equal(1.0, result.Rows[2][1].AsFloat64()); // avg of 0,1,2
        Assert.Equal(1.5, result.Rows[3][1].AsFloat64()); // avg of 0,1,2,3
        Assert.Equal(2.0, result.Rows[4][1].AsFloat64()); // avg of 0,1,2,3,4
    }

    [Fact]
    public void CountOver_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, count() OVER (ORDER BY number) AS running_count
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        Assert.Equal(1L, result.Rows[0][1].AsInt64());
        Assert.Equal(2L, result.Rows[1][1].AsInt64());
        Assert.Equal(3L, result.Rows[2][1].AsInt64());
        Assert.Equal(4L, result.Rows[3][1].AsInt64());
        Assert.Equal(5L, result.Rows[4][1].AsInt64());
    }

    [Fact]
    public void MinMaxOver_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number,
                   min(number) OVER (ORDER BY number) AS running_min,
                   max(number) OVER (ORDER BY number) AS running_max
            FROM numbers(5)
        ");

        Assert.Equal(5, result.Rows.Count);
        // Running min is always 0 since we order by number
        Assert.Equal(0UL, result.Rows[0][1].As<ulong>());
        Assert.Equal(0UL, result.Rows[4][1].As<ulong>());
        // Running max increases
        Assert.Equal(0UL, result.Rows[0][2].As<ulong>());
        Assert.Equal(2UL, result.Rows[2][2].As<ulong>());
        Assert.Equal(4UL, result.Rows[4][2].As<ulong>());
    }

    [Fact]
    public void Ntile_Basic()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number, ntile(3) OVER (ORDER BY number) AS bucket
            FROM numbers(9)
        ");

        Assert.Equal(9, result.Rows.Count);
        // 9 rows divided into 3 buckets = 3 rows each
        Assert.Equal(1L, result.Rows[0][1].AsInt64());
        Assert.Equal(1L, result.Rows[1][1].AsInt64());
        Assert.Equal(1L, result.Rows[2][1].AsInt64());
        Assert.Equal(2L, result.Rows[3][1].AsInt64());
        Assert.Equal(2L, result.Rows[4][1].AsInt64());
        Assert.Equal(2L, result.Rows[5][1].AsInt64());
        Assert.Equal(3L, result.Rows[6][1].AsInt64());
        Assert.Equal(3L, result.Rows[7][1].AsInt64());
        Assert.Equal(3L, result.Rows[8][1].AsInt64());
    }

    [Fact]
    public void MultipleWindowFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number,
                   row_number() OVER (ORDER BY number) AS rn,
                   sum(number) OVER (ORDER BY number) AS running_sum,
                   lag(number) OVER (ORDER BY number) AS prev
            FROM numbers(3)
        ");

        Assert.Equal(3, result.Rows.Count);
        // Row 0
        Assert.Equal(1L, result.Rows[0][1].AsInt64()); // row_number
        Assert.Equal(0.0, result.Rows[0][2].AsFloat64()); // sum
        Assert.True(result.Rows[0][3].IsNull); // lag
        // Row 1
        Assert.Equal(2L, result.Rows[1][1].AsInt64());
        Assert.Equal(1.0, result.Rows[1][2].AsFloat64());
        Assert.Equal(0UL, result.Rows[1][3].As<ulong>());
        // Row 2
        Assert.Equal(3L, result.Rows[2][1].AsInt64());
        Assert.Equal(3.0, result.Rows[2][2].AsFloat64());
        Assert.Equal(1UL, result.Rows[2][3].As<ulong>());
    }

    [Fact]
    public void WindowFunction_WithAlias()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT number AS n, row_number() OVER (ORDER BY number) AS rn
            FROM numbers(3)
        ");

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("n", result.Columns[0].Name);
        Assert.Equal("rn", result.Columns[1].Name);
    }
}
