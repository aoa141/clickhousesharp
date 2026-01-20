using ClickHouseSharp.Types;

namespace ClickHouseSharp.Storage;

public class Table
{
    public string Name { get; }
    public IReadOnlyList<Column> Columns { get; }
    private readonly List<Row> _rows = [];

    public Table(string name, IReadOnlyList<Column> columns)
    {
        Name = name;
        Columns = columns;
    }

    public int RowCount => _rows.Count;

    public void InsertRow(IReadOnlyList<ClickHouseValue> values)
    {
        if (values.Count != Columns.Count)
            throw new ArgumentException($"Expected {Columns.Count} values, got {values.Count}");

        var row = new Row(values.ToArray());
        _rows.Add(row);
    }

    public void InsertRows(IEnumerable<IReadOnlyList<ClickHouseValue>> rows)
    {
        foreach (var row in rows)
            InsertRow(row);
    }

    public IEnumerable<Row> GetRows() => _rows;

    public int GetColumnIndex(string columnName)
    {
        for (int i = 0; i < Columns.Count; i++)
        {
            if (Columns[i].Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        return -1;
    }

    public Column? GetColumn(string columnName)
    {
        var index = GetColumnIndex(columnName);
        return index >= 0 ? Columns[index] : null;
    }

    public void Clear() => _rows.Clear();

    public void DeleteWhere(Func<Row, bool> predicate)
    {
        _rows.RemoveAll(r => predicate(r));
    }

    public void UpdateWhere(Func<Row, bool> predicate, Action<Row> update)
    {
        foreach (var row in _rows)
        {
            if (predicate(row))
                update(row);
        }
    }
}

public class Column
{
    public string Name { get; }
    public ClickHouseType Type { get; }
    public bool Nullable { get; }
    public ClickHouseValue? DefaultValue { get; }

    public Column(string name, ClickHouseType type, bool nullable = false, ClickHouseValue? defaultValue = null)
    {
        Name = name;
        Type = type;
        Nullable = nullable;
        DefaultValue = defaultValue;
    }
}

public class Row
{
    private readonly ClickHouseValue[] _values;

    public Row(ClickHouseValue[] values)
    {
        _values = values;
    }

    public ClickHouseValue this[int index]
    {
        get => _values[index];
        set => _values[index] = value;
    }

    public int ColumnCount => _values.Length;

    public ClickHouseValue[] ToArray() => _values.ToArray();
}
