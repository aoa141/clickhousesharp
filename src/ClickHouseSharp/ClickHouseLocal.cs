using ClickHouseSharp.Execution;
using ClickHouseSharp.Functions;
using ClickHouseSharp.Storage;

namespace ClickHouseSharp;

/// <summary>
/// Main entry point for ClickHouseSharp - a local ClickHouse-compatible SQL engine.
/// Equivalent to clickhouse-local functionality.
/// </summary>
public class ClickHouseLocal : IDisposable
{
    private readonly Database _database;
    private readonly FunctionRegistry _functions;
    private readonly QueryExecutor _executor;

    public ClickHouseLocal()
    {
        _database = new Database();
        _functions = FunctionRegistry.Default;
        _executor = new QueryExecutor(_database, _functions);
    }

    /// <summary>
    /// Execute a SQL query and return the result.
    /// </summary>
    public QueryResult Execute(string sql)
    {
        return _executor.Execute(sql);
    }

    /// <summary>
    /// Execute multiple SQL statements separated by semicolons.
    /// </summary>
    public IEnumerable<QueryResult> ExecuteMultiple(string sql)
    {
        var statements = sql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (var stmt in statements)
        {
            if (!string.IsNullOrWhiteSpace(stmt))
                yield return Execute(stmt);
        }
    }

    /// <summary>
    /// Execute a SQL query and return the result as an enumerable of dynamic objects.
    /// </summary>
    public IEnumerable<dynamic> Query(string sql)
    {
        var result = Execute(sql);
        foreach (var row in result.Rows)
        {
            var expando = new System.Dynamic.ExpandoObject() as IDictionary<string, object?>;
            for (int i = 0; i < result.Columns.Count; i++)
            {
                expando[result.Columns[i].Name] = row[i].RawValue;
            }
            yield return expando;
        }
    }

    /// <summary>
    /// Execute a SQL query and return the result as an enumerable of the specified type.
    /// </summary>
    public IEnumerable<T> Query<T>(string sql) where T : new()
    {
        var result = Execute(sql);
        var type = typeof(T);
        var properties = type.GetProperties().ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var row in result.Rows)
        {
            var obj = new T();
            for (int i = 0; i < result.Columns.Count; i++)
            {
                if (properties.TryGetValue(result.Columns[i].Name, out var prop))
                {
                    var value = row[i].RawValue;
                    if (value != null && prop.CanWrite)
                    {
                        try
                        {
                            prop.SetValue(obj, Convert.ChangeType(value, prop.PropertyType));
                        }
                        catch
                        {
                            // Ignore conversion errors
                        }
                    }
                }
            }
            yield return obj;
        }
    }

    /// <summary>
    /// Execute a scalar query and return the first column of the first row.
    /// </summary>
    public T? Scalar<T>(string sql)
    {
        var result = Execute(sql);
        if (result.Rows.Count == 0 || result.Columns.Count == 0)
            return default;

        var value = result.Rows[0][0].RawValue;
        if (value == null)
            return default;

        return (T)Convert.ChangeType(value, typeof(T));
    }

    /// <summary>
    /// Get the underlying database for advanced operations.
    /// </summary>
    public Database Database => _database;

    /// <summary>
    /// Get the function registry for registering custom functions.
    /// </summary>
    public FunctionRegistry Functions => _functions;

    /// <summary>
    /// Check if a table exists.
    /// </summary>
    public bool TableExists(string tableName) => _database.TableExists(tableName);

    /// <summary>
    /// Get the list of table names.
    /// </summary>
    public IEnumerable<string> GetTableNames() => _database.GetTableNames();

    public void Dispose()
    {
        // No unmanaged resources, but implementing IDisposable for future extensibility
        GC.SuppressFinalize(this);
    }
}
