using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class UnionSubqueryTests
{
    [Fact]
    public void Union_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE t1 (value Int64)");
        ch.Execute("CREATE TABLE t2 (value Int64)");
        ch.Execute("INSERT INTO t1 VALUES (1), (2), (3)");
        ch.Execute("INSERT INTO t2 VALUES (4), (5), (6)");

        var result = ch.Execute(@"
            SELECT value FROM t1
            UNION ALL
            SELECT value FROM t2
            ORDER BY value
        ");

        Assert.Equal(6, result.Rows.Count);
    }

    [Fact]
    public void Union_Distinct()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE t1 (value Int64)");
        ch.Execute("CREATE TABLE t2 (value Int64)");
        ch.Execute("INSERT INTO t1 VALUES (1), (2), (3)");
        ch.Execute("INSERT INTO t2 VALUES (2), (3), (4)");

        var result = ch.Execute(@"
            SELECT value FROM t1
            UNION
            SELECT value FROM t2
            ORDER BY value
        ");

        Assert.Equal(4, result.Rows.Count); // 1, 2, 3, 4 (duplicates removed)
    }

    [Fact]
    public void Intersect_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE t1 (value Int64)");
        ch.Execute("CREATE TABLE t2 (value Int64)");
        ch.Execute("INSERT INTO t1 VALUES (1), (2), (3), (4)");
        ch.Execute("INSERT INTO t2 VALUES (3), (4), (5), (6)");

        var result = ch.Execute(@"
            SELECT value FROM t1
            INTERSECT
            SELECT value FROM t2
            ORDER BY value
        ");

        Assert.Equal(2, result.Rows.Count); // 3, 4
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
        Assert.Equal(4L, result.Rows[1][0].AsInt64());
    }

    [Fact]
    public void Except_Basic()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE t1 (value Int64)");
        ch.Execute("CREATE TABLE t2 (value Int64)");
        ch.Execute("INSERT INTO t1 VALUES (1), (2), (3), (4)");
        ch.Execute("INSERT INTO t2 VALUES (3), (4), (5), (6)");

        var result = ch.Execute(@"
            SELECT value FROM t1
            EXCEPT
            SELECT value FROM t2
            ORDER BY value
        ");

        Assert.Equal(2, result.Rows.Count); // 1, 2
        Assert.Equal(1L, result.Rows[0][0].AsInt64());
        Assert.Equal(2L, result.Rows[1][0].AsInt64());
    }

    [Fact]
    public void Subquery_InFrom()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (value Int64)");
        ch.Execute("INSERT INTO test VALUES (1), (2), (3), (4), (5)");

        var result = ch.Execute(@"
            SELECT doubled FROM (
                SELECT value * 2 AS doubled FROM test
            ) sub
            WHERE doubled > 4
            ORDER BY doubled
        ");

        // values 1,2,3,4,5 * 2 = 2,4,6,8,10
        // > 4 means: 6, 8, 10 = 3 rows
        Assert.Equal(3, result.Rows.Count);
    }

    [Fact]
    public void Subquery_InWhere()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE products (id Int64, category_id Int64, name String)");
        ch.Execute("CREATE TABLE categories (id Int64, name String)");
        ch.Execute("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books')");
        ch.Execute("INSERT INTO products VALUES (1, 1, 'Laptop'), (2, 1, 'Phone'), (3, 2, 'Novel')");

        var result = ch.Execute(@"
            SELECT name FROM products
            WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')
            ORDER BY name
        ");

        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("Laptop", result.Rows[0][0].AsString());
        Assert.Equal("Phone", result.Rows[1][0].AsString());
    }

    [Fact]
    public void MultipleUnions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT 1 AS n
            UNION ALL
            SELECT 2
            UNION ALL
            SELECT 3
            ORDER BY n
        ");

        Assert.Equal(3, result.Rows.Count);
    }

    [Fact]
    public void NestedSubqueries()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT val FROM (
                SELECT inner_val * 2 AS val FROM (
                    SELECT number AS inner_val FROM numbers(5)
                ) inner_sub
            ) outer_sub
            WHERE val > 4
            ORDER BY val
        ");

        // numbers(5) = 0,1,2,3,4 => *2 = 0,2,4,6,8
        // > 4 means: 6, 8 = 2 rows
        Assert.Equal(2, result.Rows.Count);
    }
}
