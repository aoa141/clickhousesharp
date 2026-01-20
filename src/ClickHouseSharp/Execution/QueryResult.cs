using ClickHouseSharp.Types;

namespace ClickHouseSharp.Execution;

public class QueryResult
{
    public IReadOnlyList<ResultColumn> Columns { get; }
    public IReadOnlyList<ResultRow> Rows { get; }
    public long RowsAffected { get; }

    public QueryResult(IReadOnlyList<ResultColumn> columns, IReadOnlyList<ResultRow> rows, long rowsAffected = 0)
    {
        Columns = columns;
        Rows = rows;
        RowsAffected = rowsAffected;
    }

    public static QueryResult Empty => new([], [], 0);

    public static QueryResult Affected(long count) => new([], [], count);

    public void Print(TextWriter writer)
    {
        if (Columns.Count == 0)
        {
            if (RowsAffected > 0)
                writer.WriteLine($"{RowsAffected} row(s) affected");
            return;
        }

        // Calculate column widths
        var widths = new int[Columns.Count];
        for (int i = 0; i < Columns.Count; i++)
        {
            widths[i] = Columns[i].Name.Length;
            foreach (var row in Rows)
            {
                var valStr = FormatValue(row[i]);
                widths[i] = Math.Max(widths[i], valStr.Length);
            }
        }

        // Print header
        var separator = "+" + string.Join("+", widths.Select(w => new string('-', w + 2))) + "+";
        writer.WriteLine(separator);
        writer.Write("|");
        for (int i = 0; i < Columns.Count; i++)
        {
            writer.Write($" {Columns[i].Name.PadRight(widths[i])} |");
        }
        writer.WriteLine();
        writer.WriteLine(separator);

        // Print rows
        foreach (var row in Rows)
        {
            writer.Write("|");
            for (int i = 0; i < Columns.Count; i++)
            {
                var val = FormatValue(row[i]);
                writer.Write($" {val.PadRight(widths[i])} |");
            }
            writer.WriteLine();
        }
        writer.WriteLine(separator);
        writer.WriteLine($"{Rows.Count} row(s)");
    }

    private static string FormatValue(ClickHouseValue value)
    {
        if (value.IsNull) return "NULL";
        return value switch
        {
            StringValue s => s.Value,
            _ => value.RawValue?.ToString() ?? ""
        };
    }

    public override string ToString()
    {
        using var sw = new StringWriter();
        Print(sw);
        return sw.ToString();
    }
}

public class ResultColumn
{
    public string Name { get; }
    public ClickHouseType Type { get; }

    public ResultColumn(string name, ClickHouseType type)
    {
        Name = name;
        Type = type;
    }
}

public class ResultRow
{
    private readonly ClickHouseValue[] _values;

    public ResultRow(ClickHouseValue[] values)
    {
        _values = values;
    }

    public ClickHouseValue this[int index] => _values[index];
    public int Count => _values.Length;

    public T GetValue<T>(int index) => (T)_values[index].RawValue!;
}
