using ClickHouseSharp.Parsing.Ast;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Storage;

public class Database
{
    public string Name { get; }
    private readonly Dictionary<string, Table> _tables = new(StringComparer.OrdinalIgnoreCase);

    public Database(string name = "default")
    {
        Name = name;
    }

    public void CreateTable(string tableName, IReadOnlyList<Column> columns)
    {
        if (_tables.ContainsKey(tableName))
            throw new InvalidOperationException($"Table '{tableName}' already exists");

        _tables[tableName] = new Table(tableName, columns);
    }

    public void CreateTable(CreateTableStatement stmt)
    {
        if (stmt.IfNotExists && _tables.ContainsKey(stmt.TableName))
            return;

        var columns = stmt.Columns.Select(c => new Column(
            c.Name,
            ClickHouseType.Parse(c.DataType),
            c.Nullable,
            null // Default values would need expression evaluation
        )).ToList();

        CreateTable(stmt.TableName, columns);
    }

    public void DropTable(string tableName, bool ifExists = false)
    {
        if (!_tables.ContainsKey(tableName))
        {
            if (ifExists) return;
            throw new InvalidOperationException($"Table '{tableName}' does not exist");
        }

        _tables.Remove(tableName);
    }

    public Table GetTable(string tableName)
    {
        if (_tables.TryGetValue(tableName, out var table))
            return table;
        throw new InvalidOperationException($"Table '{tableName}' does not exist");
    }

    public bool TryGetTable(string tableName, out Table? table)
    {
        return _tables.TryGetValue(tableName, out table);
    }

    public bool TableExists(string tableName) => _tables.ContainsKey(tableName);

    public IEnumerable<string> GetTableNames() => _tables.Keys;

    public IEnumerable<Table> GetTables() => _tables.Values;
}
