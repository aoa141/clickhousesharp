using System.Text;
using ClickHouseSharp.Execution;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Storage;

public static class FileIO
{
    /// <summary>
    /// Load a CSV file into a table.
    /// </summary>
    public static void LoadCsv(Database database, string tableName, string filePath, CsvOptions? options = null)
    {
        options ??= new CsvOptions();

        using var reader = new StreamReader(filePath, Encoding.UTF8);
        LoadCsvFromReader(database, tableName, reader, options);
    }

    /// <summary>
    /// Load CSV data from a string into a table.
    /// </summary>
    public static void LoadCsvFromString(Database database, string tableName, string csvData, CsvOptions? options = null)
    {
        options ??= new CsvOptions();

        using var reader = new StringReader(csvData);
        LoadCsvFromReader(database, tableName, new StreamReaderWrapper(reader), options);
    }

    private static void LoadCsvFromReader(Database database, string tableName, TextReader reader, CsvOptions options)
    {
        var lines = new List<string[]>();
        string? line;

        while ((line = reader.ReadLine()) != null)
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var fields = ParseCsvLine(line, options.Delimiter, options.Quote);
            lines.Add(fields);
        }

        if (lines.Count == 0) return;

        // Determine column names and types
        List<Column> columns;
        int dataStartIndex;

        if (options.HasHeader)
        {
            var headers = lines[0];
            columns = headers.Select(h => new Column(h.Trim(), StringType.Instance)).ToList();
            dataStartIndex = 1;
        }
        else
        {
            columns = Enumerable.Range(0, lines[0].Length)
                .Select(i => new Column($"column{i + 1}", StringType.Instance))
                .ToList();
            dataStartIndex = 0;
        }

        // Infer types from data if requested
        if (options.InferTypes && lines.Count > dataStartIndex)
        {
            columns = InferColumnTypes(columns, lines.Skip(dataStartIndex));
        }

        // Create table if it doesn't exist
        if (!database.TableExists(tableName))
        {
            database.CreateTable(tableName, columns);
        }

        var table = database.GetTable(tableName);

        // Insert data
        for (int i = dataStartIndex; i < lines.Count; i++)
        {
            var fields = lines[i];
            var values = new List<ClickHouseValue>();

            for (int j = 0; j < columns.Count; j++)
            {
                var fieldValue = j < fields.Length ? fields[j] : "";
                var column = columns[j];

                try
                {
                    if (string.IsNullOrEmpty(fieldValue) && column.Type.IsNullable)
                    {
                        values.Add(NullValue.Instance);
                    }
                    else
                    {
                        values.Add(column.Type.CreateValue(ParseValue(fieldValue, column.Type)));
                    }
                }
                catch
                {
                    values.Add(column.Type.DefaultValue);
                }
            }

            table.InsertRow(values);
        }
    }

    private static string[] ParseCsvLine(string line, char delimiter, char quote)
    {
        var fields = new List<string>();
        var field = new StringBuilder();
        bool inQuotes = false;
        int i = 0;

        while (i < line.Length)
        {
            char c = line[i];

            if (inQuotes)
            {
                if (c == quote)
                {
                    if (i + 1 < line.Length && line[i + 1] == quote)
                    {
                        field.Append(quote);
                        i += 2;
                        continue;
                    }
                    else
                    {
                        inQuotes = false;
                        i++;
                        continue;
                    }
                }
                field.Append(c);
            }
            else
            {
                if (c == quote)
                {
                    inQuotes = true;
                }
                else if (c == delimiter)
                {
                    fields.Add(field.ToString());
                    field.Clear();
                }
                else
                {
                    field.Append(c);
                }
            }
            i++;
        }

        fields.Add(field.ToString());
        return fields.ToArray();
    }

    private static List<Column> InferColumnTypes(List<Column> columns, IEnumerable<string[]> dataLines)
    {
        var sampleRows = dataLines.Take(100).ToList();
        var result = new List<Column>();

        for (int i = 0; i < columns.Count; i++)
        {
            var values = sampleRows.Select(r => i < r.Length ? r[i] : "").ToList();
            var inferredType = InferType(values);
            result.Add(new Column(columns[i].Name, inferredType));
        }

        return result;
    }

    private static ClickHouseType InferType(List<string> values)
    {
        bool allInts = true;
        bool allFloats = true;
        bool allBools = true;
        bool allDates = true;
        bool hasEmpty = false;

        foreach (var v in values)
        {
            if (string.IsNullOrWhiteSpace(v))
            {
                hasEmpty = true;
                continue;
            }

            if (!long.TryParse(v, out _))
                allInts = false;

            if (!double.TryParse(v, out _))
                allFloats = false;

            var lower = v.ToLowerInvariant();
            if (lower != "true" && lower != "false" && lower != "0" && lower != "1")
                allBools = false;

            if (!DateTime.TryParse(v, out _) && !DateOnly.TryParse(v, out _))
                allDates = false;
        }

        ClickHouseType baseType;
        if (allBools && values.Count > 0)
            baseType = BoolType.Instance;
        else if (allInts && values.Count > 0)
            baseType = Int64Type.Instance;
        else if (allFloats && values.Count > 0)
            baseType = Float64Type.Instance;
        else if (allDates && values.Count > 0)
            baseType = DateTimeType.Instance;
        else
            baseType = StringType.Instance;

        return hasEmpty ? new NullableType(baseType) : baseType;
    }

    private static object ParseValue(string value, ClickHouseType type)
    {
        if (type is NullableType nullable)
            type = nullable.InnerType;

        return type switch
        {
            Int8Type => sbyte.Parse(value),
            Int16Type => short.Parse(value),
            Int32Type => int.Parse(value),
            Int64Type => long.Parse(value),
            UInt8Type => byte.Parse(value),
            UInt16Type => ushort.Parse(value),
            UInt32Type => uint.Parse(value),
            UInt64Type => ulong.Parse(value),
            Float32Type => float.Parse(value),
            Float64Type => double.Parse(value),
            BoolType => ParseBool(value),
            DateType => DateOnly.Parse(value),
            DateTimeType => DateTime.Parse(value),
            _ => value
        };
    }

    private static bool ParseBool(string value)
    {
        var lower = value.ToLowerInvariant();
        return lower == "true" || lower == "1";
    }

    /// <summary>
    /// Export a query result to CSV format.
    /// </summary>
    public static void ExportCsv(QueryResult result, string filePath, CsvOptions? options = null)
    {
        options ??= new CsvOptions();

        using var writer = new StreamWriter(filePath, false, Encoding.UTF8);
        ExportCsvToWriter(result, writer, options);
    }

    /// <summary>
    /// Export a query result to CSV string.
    /// </summary>
    public static string ExportCsvToString(QueryResult result, CsvOptions? options = null)
    {
        options ??= new CsvOptions();

        using var writer = new StringWriter();
        ExportCsvToWriter(result, writer, options);
        return writer.ToString();
    }

    private static void ExportCsvToWriter(QueryResult result, TextWriter writer, CsvOptions options)
    {
        if (options.HasHeader)
        {
            var headers = result.Columns.Select(c => EscapeCsvField(c.Name, options));
            writer.WriteLine(string.Join(options.Delimiter, headers));
        }

        foreach (var row in result.Rows)
        {
            var fields = new List<string>();
            for (int i = 0; i < result.Columns.Count; i++)
            {
                var value = row[i];
                var str = value.IsNull ? "" : (value.RawValue?.ToString() ?? "");
                fields.Add(EscapeCsvField(str, options));
            }
            writer.WriteLine(string.Join(options.Delimiter, fields));
        }
    }

    private static string EscapeCsvField(string value, CsvOptions options)
    {
        if (value.Contains(options.Delimiter) || value.Contains(options.Quote) || value.Contains('\n') || value.Contains('\r'))
        {
            return $"{options.Quote}{value.Replace(options.Quote.ToString(), $"{options.Quote}{options.Quote}")}{options.Quote}";
        }
        return value;
    }

    private class StreamReaderWrapper : TextReader
    {
        private readonly TextReader _inner;

        public StreamReaderWrapper(TextReader inner) => _inner = inner;

        public override string? ReadLine() => _inner.ReadLine();
    }
}

public class CsvOptions
{
    public char Delimiter { get; set; } = ',';
    public char Quote { get; set; } = '"';
    public bool HasHeader { get; set; } = true;
    public bool InferTypes { get; set; } = true;
}
