using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class DdlDmlTests
{
    [Fact]
    public void CreateTable_Basic()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute(@"
            CREATE TABLE users (
                id Int64,
                name String,
                email String
            )
        ");

        Assert.True(ch.TableExists("users"));
    }

    [Fact]
    public void CreateTable_IfNotExists()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64)");
        ch.Execute("CREATE TABLE IF NOT EXISTS test (id Int64, name String)");

        Assert.True(ch.TableExists("test"));
    }

    [Fact]
    public void CreateTable_WithNullable()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute(@"
            CREATE TABLE test (
                id Int64,
                optional_value Nullable(Int64)
            )
        ");

        Assert.True(ch.TableExists("test"));
    }

    [Fact]
    public void CreateTable_WithArray()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute(@"
            CREATE TABLE test (
                id Int64,
                tags Array(String)
            )
        ");

        Assert.True(ch.TableExists("test"));
    }

    [Fact]
    public void DropTable_Basic()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64)");
        Assert.True(ch.TableExists("test"));

        ch.Execute("DROP TABLE test");
        Assert.False(ch.TableExists("test"));
    }

    [Fact]
    public void DropTable_IfExists()
    {
        using var ch = new ClickHouseLocal();

        // Should not throw
        ch.Execute("DROP TABLE IF EXISTS nonexistent");
    }

    [Fact]
    public void Insert_SingleRow()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64, name String)");
        ch.Execute("INSERT INTO test VALUES (1, 'Alice')");

        var result = ch.Execute("SELECT * FROM test");
        Assert.Single(result.Rows);
        Assert.Equal(1L, result.Rows[0][0].AsInt64());
        Assert.Equal("Alice", result.Rows[0][1].AsString());
    }

    [Fact]
    public void Insert_MultipleRows()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64, name String)");
        ch.Execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        var result = ch.Execute("SELECT count() FROM test");
        Assert.Equal(3L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void Insert_WithColumnList()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64, name String, value Int64)");
        ch.Execute("INSERT INTO test (id, name) VALUES (1, 'Alice')");

        var result = ch.Execute("SELECT * FROM test");
        Assert.Single(result.Rows);
        Assert.Equal(1L, result.Rows[0][0].AsInt64());
        Assert.Equal("Alice", result.Rows[0][1].AsString());
    }

    [Fact]
    public void Insert_FromSelect()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE source (value Int64)");
        ch.Execute("INSERT INTO source VALUES (1), (2), (3)");

        ch.Execute("CREATE TABLE dest (value Int64)");
        ch.Execute("INSERT INTO dest SELECT value * 10 FROM source");

        var result = ch.Execute("SELECT * FROM dest ORDER BY value");
        Assert.Equal(3, result.Rows.Count);
        Assert.Equal(10L, result.Rows[0][0].AsInt64());
        Assert.Equal(20L, result.Rows[1][0].AsInt64());
        Assert.Equal(30L, result.Rows[2][0].AsInt64());
    }

    [Fact]
    public void Update_Basic()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64, value Int64)");
        ch.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)");

        ch.Execute("UPDATE test SET value = value * 2 WHERE id = 2");

        var result = ch.Execute("SELECT value FROM test WHERE id = 2");
        Assert.Equal(40L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void Update_MultipleColumns()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64, name String, active Bool)");
        ch.Execute("INSERT INTO test VALUES (1, 'old', false)");

        ch.Execute("UPDATE test SET name = 'new', active = true WHERE id = 1");

        var result = ch.Execute("SELECT name, active FROM test WHERE id = 1");
        Assert.Equal("new", result.Rows[0][0].AsString());
        Assert.True(result.Rows[0][1].AsBool());
    }

    [Fact]
    public void Delete_Basic()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64, value Int64)");
        ch.Execute("INSERT INTO test VALUES (1, 10), (2, 20), (3, 30)");

        ch.Execute("DELETE FROM test WHERE id = 2");

        var result = ch.Execute("SELECT count() FROM test");
        Assert.Equal(2L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void Delete_All()
    {
        using var ch = new ClickHouseLocal();

        ch.Execute("CREATE TABLE test (id Int64)");
        ch.Execute("INSERT INTO test VALUES (1), (2), (3)");

        ch.Execute("DELETE FROM test WHERE 1 = 1");

        var result = ch.Execute("SELECT count() FROM test");
        Assert.Equal(0L, result.Rows[0][0].AsInt64());
    }

    [Fact]
    public void ExecuteMultiple_Statements()
    {
        using var ch = new ClickHouseLocal();

        var results = ch.ExecuteMultiple(@"
            CREATE TABLE test (id Int64);
            INSERT INTO test VALUES (1), (2), (3);
            SELECT count() FROM test
        ").ToList();

        Assert.Equal(3, results.Count);
        Assert.Equal(3L, results[2].Rows[0][0].AsInt64());
    }

    [Fact]
    public void Query_DynamicResults()
    {
        using var ch = new ClickHouseLocal();
        ch.Execute("CREATE TABLE test (id Int64, name String)");
        ch.Execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')");

        var results = ch.Query("SELECT * FROM test ORDER BY id").ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal(1L, (long)results[0].id);
        Assert.Equal("Alice", (string)results[0].name);
    }

    [Fact]
    public void Scalar_Query()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Scalar<long>("SELECT 42");

        Assert.Equal(42L, result);
    }
}
