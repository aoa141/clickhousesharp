using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class JoinTests
{
    private ClickHouseLocal CreateTestDatabase()
    {
        var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE users (id Int64, name String)");
        ch.Execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        ch.Execute("CREATE TABLE orders (id Int64, user_id Int64, product String)");
        ch.Execute("INSERT INTO orders VALUES (1, 1, 'Laptop'), (2, 1, 'Phone'), (3, 2, 'Tablet'), (4, 4, 'Monitor')");

        return ch;
    }

    [Fact]
    public void InnerJoin_Basic()
    {
        using var ch = CreateTestDatabase();

        var result = ch.Execute(@"
            SELECT u.name, o.product
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id
            ORDER BY u.name, o.product
        ");

        Assert.Equal(3, result.Rows.Count);
        Assert.Equal("Alice", result.Rows[0][0].AsString());
        Assert.Equal("Laptop", result.Rows[0][1].AsString());
        Assert.Equal("Alice", result.Rows[1][0].AsString());
        Assert.Equal("Phone", result.Rows[1][1].AsString());
        Assert.Equal("Bob", result.Rows[2][0].AsString());
        Assert.Equal("Tablet", result.Rows[2][1].AsString());
    }

    [Fact]
    public void LeftJoin_IncludesUnmatchedLeft()
    {
        using var ch = CreateTestDatabase();

        var result = ch.Execute(@"
            SELECT u.name, o.product
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            ORDER BY u.name
        ");

        Assert.Equal(4, result.Rows.Count);
        // Charlie has no orders, but should still appear
        var charlieRow = result.Rows.FirstOrDefault(r => r[0].AsString() == "Charlie");
        Assert.NotNull(charlieRow);
    }

    [Fact]
    public void RightJoin_IncludesUnmatchedRight()
    {
        using var ch = CreateTestDatabase();

        var result = ch.Execute(@"
            SELECT u.name, o.product
            FROM users u
            RIGHT JOIN orders o ON u.id = o.user_id
            ORDER BY o.product
        ");

        Assert.Equal(4, result.Rows.Count);
        // Order with user_id=4 has no matching user
        var monitorRow = result.Rows.FirstOrDefault(r => r[1].AsString() == "Monitor");
        Assert.NotNull(monitorRow);
    }

    [Fact]
    public void CrossJoin_CartesianProduct()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE a (x Int64)");
        ch.Execute("CREATE TABLE b (y Int64)");
        ch.Execute("INSERT INTO a VALUES (1), (2)");
        ch.Execute("INSERT INTO b VALUES (10), (20), (30)");

        var result = ch.Execute("SELECT x, y FROM a CROSS JOIN b ORDER BY x, y");

        Assert.Equal(6, result.Rows.Count); // 2 * 3 = 6
    }

    [Fact]
    public void Join_WithUsing()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE t1 (id Int64, value1 String)");
        ch.Execute("CREATE TABLE t2 (id Int64, value2 String)");
        ch.Execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')");
        ch.Execute("INSERT INTO t2 VALUES (1, 'x'), (3, 'y')");

        var result = ch.Execute(@"
            SELECT t1.id, t1.value1, t2.value2
            FROM t1
            INNER JOIN t2 USING (id)
        ");

        Assert.Single(result.Rows);
        Assert.Equal(1L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void MultipleJoins()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE categories (id Int64, name String)");
        ch.Execute("CREATE TABLE products (id Int64, name String, category_id Int64)");
        ch.Execute("CREATE TABLE orders (id Int64, product_id Int64, quantity Int64)");

        ch.Execute("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books')");
        ch.Execute("INSERT INTO products VALUES (1, 'Laptop', 1), (2, 'Novel', 2)");
        ch.Execute("INSERT INTO orders VALUES (1, 1, 5), (2, 2, 10)");

        var result = ch.Execute(@"
            SELECT c.name AS category, p.name AS product, o.quantity
            FROM orders o
            INNER JOIN products p ON o.product_id = p.id
            INNER JOIN categories c ON p.category_id = c.id
            ORDER BY category
        ");

        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("Books", result.Rows[0][0].AsString());
        Assert.Equal("Electronics", result.Rows[1][0].AsString());
    }

    [Fact]
    public void SelfJoin()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE employees (id Int64, name String, manager_id Nullable(Int64))");
        ch.Execute("INSERT INTO employees VALUES (1, 'CEO', NULL), (2, 'Manager', 1), (3, 'Developer', 2)");

        var result = ch.Execute(@"
            SELECT e.name AS employee, m.name AS manager
            FROM employees e
            LEFT JOIN employees m ON e.manager_id = m.id
            ORDER BY e.id
        ");

        Assert.Equal(3, result.Rows.Count);
    }
}
