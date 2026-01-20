using ClickHouseSharp.Types;

namespace ClickHouseSharp.Execution;

public class RowContext
{
    private readonly Dictionary<string, ClickHouseValue> _columns = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Dictionary<string, ClickHouseValue>> _tableColumns = new(StringComparer.OrdinalIgnoreCase);

    public RowContext() { }

    public RowContext(IEnumerable<KeyValuePair<string, ClickHouseValue>> columns)
    {
        foreach (var kv in columns)
            _columns[kv.Key] = kv.Value;
    }

    public void SetColumn(string name, ClickHouseValue value)
    {
        _columns[name] = value;
    }

    public void SetColumn(string tableName, string columnName, ClickHouseValue value)
    {
        if (!_tableColumns.TryGetValue(tableName, out var table))
        {
            table = new Dictionary<string, ClickHouseValue>(StringComparer.OrdinalIgnoreCase);
            _tableColumns[tableName] = table;
        }
        table[columnName] = value;
        // Also store without table prefix for unqualified access
        _columns[columnName] = value;
    }

    public ClickHouseValue GetColumn(string name)
    {
        if (_columns.TryGetValue(name, out var value))
            return value;
        throw new KeyNotFoundException($"Column '{name}' not found");
    }

    public ClickHouseValue GetColumn(string tableName, string columnName)
    {
        if (_tableColumns.TryGetValue(tableName, out var table) && table.TryGetValue(columnName, out var value))
            return value;
        // Fall back to unqualified lookup
        if (_columns.TryGetValue(columnName, out value))
            return value;
        throw new KeyNotFoundException($"Column '{tableName}.{columnName}' not found");
    }

    public bool TryGetColumn(string name, out ClickHouseValue? value)
    {
        if (_columns.TryGetValue(name, out var v))
        {
            value = v;
            return true;
        }
        value = null;
        return false;
    }

    public IEnumerable<string> GetColumnNames() => _columns.Keys;

    public RowContext Clone()
    {
        var clone = new RowContext();
        foreach (var kv in _columns)
            clone._columns[kv.Key] = kv.Value;
        foreach (var table in _tableColumns)
        {
            clone._tableColumns[table.Key] = new Dictionary<string, ClickHouseValue>(table.Value, StringComparer.OrdinalIgnoreCase);
        }
        return clone;
    }

    public void Merge(RowContext other)
    {
        foreach (var kv in other._columns)
            _columns[kv.Key] = kv.Value;
        foreach (var table in other._tableColumns)
        {
            if (!_tableColumns.TryGetValue(table.Key, out var existing))
            {
                existing = new Dictionary<string, ClickHouseValue>(StringComparer.OrdinalIgnoreCase);
                _tableColumns[table.Key] = existing;
            }
            foreach (var col in table.Value)
                existing[col.Key] = col.Value;
        }
    }
}
