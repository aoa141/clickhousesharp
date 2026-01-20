using ClickHouseSharp.Functions;
using ClickHouseSharp.Parsing;
using ClickHouseSharp.Parsing.Ast;
using ClickHouseSharp.Storage;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Execution;

public class QueryExecutor
{
    private readonly Database _database;
    private readonly FunctionRegistry _functions;
    private readonly ExpressionEvaluator _evaluator;
    private readonly Dictionary<string, CteDefinition> _ctes = new(StringComparer.OrdinalIgnoreCase);

    public QueryExecutor(Database database, FunctionRegistry? functions = null)
    {
        _database = database;
        _functions = functions ?? FunctionRegistry.Default;
        _evaluator = new ExpressionEvaluator(_functions);
        _evaluator.SetSubqueryExecutor(ExecuteSelect);
    }

    public QueryResult Execute(string sql)
    {
        var parser = Parser.FromSql(sql);
        var statement = parser.ParseStatement();
        return Execute(statement);
    }

    public QueryResult Execute(Statement statement)
    {
        return statement switch
        {
            SelectStatement select => ExecuteSelect(select),
            SetOperationStatement setOp => ExecuteSetOperation(setOp),
            InsertStatement insert => ExecuteInsert(insert),
            CreateTableStatement create => ExecuteCreateTable(create),
            DropTableStatement drop => ExecuteDropTable(drop),
            UpdateStatement update => ExecuteUpdate(update),
            DeleteStatement delete => ExecuteDelete(delete),
            _ => throw new NotImplementedException($"Statement type {statement.GetType().Name} not implemented")
        };
    }

    private QueryResult ExecuteSelect(SelectStatement select)
    {
        // Register CTEs
        if (select.With != null)
        {
            foreach (var cte in select.With)
                _ctes[cte.Name] = cte;
        }

        try
        {
            // Get source rows from FROM clause
            var sourceRows = select.From != null
                ? GetTableRows(select.From)
                : new List<RowContext> { new() }; // No FROM = single row with no columns

            // Apply WHERE filter
            if (select.Where != null)
            {
                sourceRows = sourceRows.Where(row =>
                {
                    var result = _evaluator.Evaluate(select.Where, row);
                    return !result.IsNull && result.AsBool();
                }).ToList();
            }

            // Handle GROUP BY
            if (select.GroupBy != null && select.GroupBy.Count > 0)
            {
                return ExecuteGroupBy(select, sourceRows);
            }

            // Check if we have aggregate functions without GROUP BY
            if (HasAggregateFunctions(select.Columns))
            {
                return ExecuteAggregateWithoutGroupBy(select, sourceRows);
            }

            // Apply DISTINCT if needed
            if (select.Distinct)
            {
                sourceRows = ApplyDistinct(select, sourceRows);
            }

            // Apply ORDER BY
            if (select.OrderBy != null && select.OrderBy.Count > 0)
            {
                sourceRows = ApplyOrderBy(select.OrderBy, sourceRows, select.Columns);
            }

            // Apply LIMIT/OFFSET
            if (select.Offset != null)
            {
                var offset = (int)_evaluator.Evaluate(select.Offset, new RowContext()).AsInt64();
                sourceRows = sourceRows.Skip(offset).ToList();
            }
            if (select.Limit != null)
            {
                var limit = (int)_evaluator.Evaluate(select.Limit, new RowContext()).AsInt64();
                sourceRows = sourceRows.Take(limit).ToList();
            }

            // Project columns
            var (columns, resultRows) = ProjectColumns(select.Columns, sourceRows);

            return new QueryResult(columns, resultRows);
        }
        finally
        {
            // Clean up CTEs
            if (select.With != null)
            {
                foreach (var cte in select.With)
                    _ctes.Remove(cte.Name);
            }
        }
    }

    private List<RowContext> GetTableRows(TableReference tableRef)
    {
        return tableRef switch
        {
            TableNameReference tnr => GetTableNameRows(tnr),
            JoinReference jr => ExecuteJoin(jr),
            SubqueryReference sq => ExecuteSubquery(sq),
            TableFunctionReference tf => ExecuteTableFunction(tf),
            _ => throw new NotImplementedException($"Table reference type {tableRef.GetType().Name} not implemented")
        };
    }

    private List<RowContext> GetTableNameRows(TableNameReference tnr)
    {
        // Check if it's a CTE
        if (_ctes.TryGetValue(tnr.TableName, out var cteDef))
        {
            var result = ExecuteSelect(cteDef.Query);
            var tableAlias = tnr.Alias ?? tnr.TableName;

            // Apply column renames if CTE has explicit column list
            if (cteDef.Columns != null && cteDef.Columns.Count > 0)
            {
                var renamedRows = new List<RowContext>();
                foreach (var row in result.Rows)
                {
                    var context = new RowContext();
                    for (int i = 0; i < result.Columns.Count && i < cteDef.Columns.Count; i++)
                    {
                        context.SetColumn(tableAlias, cteDef.Columns[i], row[i]);
                    }
                    renamedRows.Add(context);
                }
                return renamedRows;
            }

            return ConvertResultToRows(result, tableAlias);
        }

        var table = _database.GetTable(tnr.TableName);
        var alias = tnr.Alias ?? tnr.TableName;

        var rows = new List<RowContext>();
        foreach (var row in table.GetRows())
        {
            var context = new RowContext();
            for (int i = 0; i < table.Columns.Count; i++)
            {
                context.SetColumn(alias, table.Columns[i].Name, row[i]);
            }
            rows.Add(context);
        }
        return rows;
    }

    private List<RowContext> ExecuteJoin(JoinReference jr)
    {
        var leftRows = GetTableRows(jr.Left);
        var rightRows = GetTableRows(jr.Right);

        return jr.Type switch
        {
            JoinType.Cross => ExecuteCrossJoin(leftRows, rightRows),
            JoinType.Inner => ExecuteInnerJoin(leftRows, rightRows, jr.Condition, jr.Using),
            JoinType.Left => ExecuteLeftJoin(leftRows, rightRows, jr.Condition, jr.Using),
            JoinType.Right => ExecuteRightJoin(leftRows, rightRows, jr.Condition, jr.Using),
            JoinType.Full => ExecuteFullJoin(leftRows, rightRows, jr.Condition, jr.Using),
            JoinType.LeftSemi => ExecuteLeftSemiJoin(leftRows, rightRows, jr.Condition, jr.Using),
            JoinType.LeftAnti => ExecuteLeftAntiJoin(leftRows, rightRows, jr.Condition, jr.Using),
            _ => throw new NotImplementedException($"Join type {jr.Type} not implemented")
        };
    }

    private List<RowContext> ExecuteCrossJoin(List<RowContext> left, List<RowContext> right)
    {
        var result = new List<RowContext>();
        foreach (var l in left)
        {
            foreach (var r in right)
            {
                var merged = l.Clone();
                merged.Merge(r);
                result.Add(merged);
            }
        }
        return result;
    }

    private List<RowContext> ExecuteInnerJoin(List<RowContext> left, List<RowContext> right, Expression? condition, IReadOnlyList<string>? usingCols)
    {
        var result = new List<RowContext>();
        foreach (var l in left)
        {
            foreach (var r in right)
            {
                var merged = l.Clone();
                merged.Merge(r);

                if (JoinMatches(merged, condition, usingCols, l, r))
                    result.Add(merged);
            }
        }
        return result;
    }

    private List<RowContext> ExecuteLeftJoin(List<RowContext> left, List<RowContext> right, Expression? condition, IReadOnlyList<string>? usingCols)
    {
        var result = new List<RowContext>();

        // Get column names from right side (for NULL filling)
        var rightColumnNames = right.Count > 0
            ? right[0].GetColumnNames().ToList()
            : new List<string>();

        foreach (var l in left)
        {
            bool matched = false;
            foreach (var r in right)
            {
                var merged = l.Clone();
                merged.Merge(r);

                if (JoinMatches(merged, condition, usingCols, l, r))
                {
                    result.Add(merged);
                    matched = true;
                }
            }
            if (!matched)
            {
                var row = l.Clone();
                // Add NULL values for right-side columns
                foreach (var colName in rightColumnNames)
                {
                    if (!row.TryGetColumn(colName, out _))
                        row.SetColumn(colName, Types.NullValue.Instance);
                }
                result.Add(row);
            }
        }
        return result;
    }

    private List<RowContext> ExecuteRightJoin(List<RowContext> left, List<RowContext> right, Expression? condition, IReadOnlyList<string>? usingCols)
    {
        return ExecuteLeftJoin(right, left, condition, usingCols);
    }

    private List<RowContext> ExecuteFullJoin(List<RowContext> left, List<RowContext> right, Expression? condition, IReadOnlyList<string>? usingCols)
    {
        var result = new List<RowContext>();
        var matchedRight = new HashSet<int>();

        foreach (var l in left)
        {
            bool matched = false;
            for (int i = 0; i < right.Count; i++)
            {
                var r = right[i];
                var merged = l.Clone();
                merged.Merge(r);

                if (JoinMatches(merged, condition, usingCols, l, r))
                {
                    result.Add(merged);
                    matched = true;
                    matchedRight.Add(i);
                }
            }
            if (!matched)
            {
                result.Add(l.Clone());
            }
        }

        for (int i = 0; i < right.Count; i++)
        {
            if (!matchedRight.Contains(i))
                result.Add(right[i].Clone());
        }

        return result;
    }

    private List<RowContext> ExecuteLeftSemiJoin(List<RowContext> left, List<RowContext> right, Expression? condition, IReadOnlyList<string>? usingCols)
    {
        var result = new List<RowContext>();
        foreach (var l in left)
        {
            foreach (var r in right)
            {
                var merged = l.Clone();
                merged.Merge(r);

                if (JoinMatches(merged, condition, usingCols, l, r))
                {
                    result.Add(l.Clone());
                    break;
                }
            }
        }
        return result;
    }

    private List<RowContext> ExecuteLeftAntiJoin(List<RowContext> left, List<RowContext> right, Expression? condition, IReadOnlyList<string>? usingCols)
    {
        var result = new List<RowContext>();
        foreach (var l in left)
        {
            bool matched = false;
            foreach (var r in right)
            {
                var merged = l.Clone();
                merged.Merge(r);

                if (JoinMatches(merged, condition, usingCols, l, r))
                {
                    matched = true;
                    break;
                }
            }
            if (!matched)
            {
                result.Add(l.Clone());
            }
        }
        return result;
    }

    private bool JoinMatches(RowContext merged, Expression? condition, IReadOnlyList<string>? usingCols, RowContext left, RowContext right)
    {
        if (usingCols != null)
        {
            foreach (var col in usingCols)
            {
                var leftVal = left.GetColumn(col);
                var rightVal = right.GetColumn(col);
                if (!leftVal.Equals(rightVal))
                    return false;
            }
            return true;
        }

        if (condition != null)
        {
            var result = _evaluator.Evaluate(condition, merged);
            return !result.IsNull && result.AsBool();
        }

        return true;
    }

    private List<RowContext> ExecuteSubquery(SubqueryReference sq)
    {
        var result = Execute(sq.Query);
        return ConvertResultToRows(result, sq.Alias);
    }

    private List<RowContext> ConvertResultToRows(QueryResult result, string alias)
    {
        var rows = new List<RowContext>();
        foreach (var row in result.Rows)
        {
            var context = new RowContext();
            for (int i = 0; i < result.Columns.Count; i++)
            {
                context.SetColumn(alias, result.Columns[i].Name, row[i]);
            }
            rows.Add(context);
        }
        return rows;
    }

    private List<RowContext> ExecuteTableFunction(TableFunctionReference tf)
    {
        var funcName = tf.FunctionName.ToLowerInvariant();
        var args = tf.Arguments.Select(a => _evaluator.Evaluate(a, new RowContext())).ToList();

        return funcName switch
        {
            "numbers" => ExecuteNumbersFunction(args, tf.Alias),
            "zeros" => ExecuteZerosFunction(args, tf.Alias),
            "one" => ExecuteOneFunction(tf.Alias),
            _ => throw new NotImplementedException($"Table function '{funcName}' not implemented")
        };
    }

    private List<RowContext> ExecuteNumbersFunction(IReadOnlyList<ClickHouseValue> args, string? alias)
    {
        var count = args[0].AsInt64();
        var start = args.Count > 1 ? args[1].AsInt64() : 0L;

        var rows = new List<RowContext>();
        var tableName = alias ?? "numbers";
        for (long i = 0; i < count; i++)
        {
            var context = new RowContext();
            context.SetColumn(tableName, "number", new UInt64Value((ulong)(start + i)));
            rows.Add(context);
        }
        return rows;
    }

    private List<RowContext> ExecuteZerosFunction(IReadOnlyList<ClickHouseValue> args, string? alias)
    {
        var count = args[0].AsInt64();
        var rows = new List<RowContext>();
        var tableName = alias ?? "zeros";
        for (long i = 0; i < count; i++)
        {
            var context = new RowContext();
            context.SetColumn(tableName, "zero", new UInt64Value(0));
            rows.Add(context);
        }
        return rows;
    }

    private List<RowContext> ExecuteOneFunction(string? alias)
    {
        var context = new RowContext();
        context.SetColumn(alias ?? "one", "dummy", new UInt8Value(0));
        return [context];
    }

    private QueryResult ExecuteGroupBy(SelectStatement select, List<RowContext> sourceRows)
    {
        // Group rows by GROUP BY expressions
        var groups = new Dictionary<string, List<RowContext>>();
        foreach (var row in sourceRows)
        {
            var keyParts = select.GroupBy!.Select(e => _evaluator.Evaluate(e, row).RawValue?.ToString() ?? "NULL");
            var key = string.Join("\0", keyParts);

            if (!groups.TryGetValue(key, out var group))
            {
                group = [];
                groups[key] = group;
            }
            group.Add(row);
        }

        // Project columns for each group
        var resultRows = new List<ResultRow>();
        var columns = new List<ResultColumn>();
        bool columnsSet = false;

        foreach (var group in groups.Values)
        {
            var rowValues = new List<ClickHouseValue>();
            var representative = group[0];

            foreach (var col in select.Columns)
            {
                var (name, value, type) = EvaluateSelectExpression(col, representative, group);
                rowValues.Add(value);
                if (!columnsSet)
                    columns.Add(new ResultColumn(name, type));
            }

            resultRows.Add(new ResultRow(rowValues.ToArray()));
            columnsSet = true;
        }

        // Apply HAVING
        if (select.Having != null)
        {
            // Re-evaluate with group context - simplified for now
        }

        // Apply ORDER BY
        if (select.OrderBy != null)
        {
            // Order result rows
            resultRows = OrderResultRows(select.OrderBy, columns, resultRows);
        }

        return new QueryResult(columns, resultRows);
    }

    private QueryResult ExecuteAggregateWithoutGroupBy(SelectStatement select, List<RowContext> sourceRows)
    {
        var columns = new List<ResultColumn>();
        var rowValues = new List<ClickHouseValue>();
        var representative = sourceRows.Count > 0 ? sourceRows[0] : new RowContext();

        foreach (var col in select.Columns)
        {
            var (name, value, type) = EvaluateSelectExpression(col, representative, sourceRows);
            columns.Add(new ResultColumn(name, type));
            rowValues.Add(value);
        }

        return new QueryResult(columns, [new ResultRow(rowValues.ToArray())]);
    }

    private (string Name, ClickHouseValue Value, ClickHouseType Type) EvaluateSelectExpression(
        Expression expr, RowContext row, List<RowContext>? groupRows = null)
    {
        if (expr is AliasedExpression alias)
        {
            var (_, value, type) = EvaluateSelectExpression(alias.Expression, row, groupRows);
            return (alias.Alias, value, type);
        }

        if (expr is FunctionCallExpression func && _functions.IsAggregateFunction(func.FunctionName))
        {
            return EvaluateAggregateFunction(func, groupRows ?? [row]);
        }

        var val = _evaluator.Evaluate(expr, row);
        var name = GetExpressionName(expr);
        return (name, val, val.Type);
    }

    private (string Name, ClickHouseValue Value, ClickHouseType Type) EvaluateAggregateFunction(
        FunctionCallExpression func, List<RowContext> rows)
    {
        if (!_functions.TryGetAggregateFunction(func.FunctionName, out var aggFunc))
            throw new InvalidOperationException($"Unknown aggregate function: {func.FunctionName}");

        var state = aggFunc!.CreateState();

        // Handle DISTINCT
        var processedValues = new HashSet<ClickHouseValue>();

        foreach (var row in rows)
        {
            var args = func.Arguments.Select(a => _evaluator.Evaluate(a, row)).ToList();

            if (func.Distinct && args.Count > 0)
            {
                if (!processedValues.Add(args[0]))
                    continue;
            }

            aggFunc.Accumulate(state, args);
        }

        var result = aggFunc.Finalize(state);
        return (func.FunctionName, result, result.Type);
    }

    private static string GetExpressionName(Expression expr)
    {
        return expr switch
        {
            ColumnExpression col => col.FullName,
            AliasedExpression alias => alias.Alias,
            FunctionCallExpression func => func.FunctionName,
            StarExpression star => star.TableName != null ? $"{star.TableName}.*" : "*",
            LiteralExpression => "literal",
            _ => expr.GetType().Name
        };
    }

    private (List<ResultColumn>, List<ResultRow>) ProjectColumns(IReadOnlyList<Expression> columns, List<RowContext> rows)
    {
        var resultColumns = new List<ResultColumn>();
        var resultRows = new List<ResultRow>();

        // Expand star expressions first
        var expandedColumns = ExpandStarExpressions(columns, rows.Count > 0 ? rows[0] : null);

        // Build column metadata
        if (rows.Count > 0)
        {
            foreach (var col in expandedColumns)
            {
                var (name, value, type) = EvaluateSelectExpression(col, rows[0]);
                resultColumns.Add(new ResultColumn(name, type));
            }
        }

        // Project each row
        foreach (var row in rows)
        {
            var values = new List<ClickHouseValue>();
            foreach (var col in expandedColumns)
            {
                var (_, value, _) = EvaluateSelectExpression(col, row);
                values.Add(value);
            }
            resultRows.Add(new ResultRow(values.ToArray()));
        }

        return (resultColumns, resultRows);
    }

    private List<Expression> ExpandStarExpressions(IReadOnlyList<Expression> columns, RowContext? sampleRow)
    {
        var expanded = new List<Expression>();
        foreach (var col in columns)
        {
            if (col is StarExpression star)
            {
                if (sampleRow != null)
                {
                    foreach (var colName in sampleRow.GetColumnNames())
                    {
                        expanded.Add(new ColumnExpression(null, colName));
                    }
                }
            }
            else
            {
                expanded.Add(col);
            }
        }
        return expanded;
    }

    private bool HasAggregateFunctions(IReadOnlyList<Expression> columns)
    {
        foreach (var col in columns)
        {
            if (ContainsAggregateFunction(col))
                return true;
        }
        return false;
    }

    private bool ContainsAggregateFunction(Expression expr)
    {
        return expr switch
        {
            FunctionCallExpression func => _functions.IsAggregateFunction(func.FunctionName) ||
                func.Arguments.Any(ContainsAggregateFunction),
            AliasedExpression alias => ContainsAggregateFunction(alias.Expression),
            BinaryExpression bin => ContainsAggregateFunction(bin.Left) || ContainsAggregateFunction(bin.Right),
            UnaryExpression un => ContainsAggregateFunction(un.Operand),
            CaseExpression caseExpr => caseExpr.WhenClauses.Any(w =>
                ContainsAggregateFunction(w.Condition) || ContainsAggregateFunction(w.Result)) ||
                (caseExpr.ElseResult != null && ContainsAggregateFunction(caseExpr.ElseResult)),
            _ => false
        };
    }

    private List<RowContext> ApplyDistinct(SelectStatement select, List<RowContext> rows)
    {
        var seen = new HashSet<string>();
        var result = new List<RowContext>();

        foreach (var row in rows)
        {
            var keyParts = select.Columns.Select(c =>
            {
                var val = _evaluator.Evaluate(c is AliasedExpression a ? a.Expression : c, row);
                return val.RawValue?.ToString() ?? "NULL";
            });
            var key = string.Join("\0", keyParts);

            if (seen.Add(key))
                result.Add(row);
        }

        return result;
    }

    private List<RowContext> ApplyOrderBy(IReadOnlyList<OrderByItem> orderBy, List<RowContext> rows, IReadOnlyList<Expression>? selectColumns = null)
    {
        // Build alias map from SELECT columns
        var aliasMap = new Dictionary<string, Expression>(StringComparer.OrdinalIgnoreCase);
        if (selectColumns != null)
        {
            foreach (var col in selectColumns)
            {
                if (col is AliasedExpression alias)
                {
                    aliasMap[alias.Alias] = alias.Expression;
                }
            }
        }

        var sorted = rows.ToList();
        sorted.Sort((a, b) =>
        {
            foreach (var item in orderBy)
            {
                // Resolve alias if ORDER BY references a SELECT alias
                var expr = item.Expression;
                if (expr is ColumnExpression colExpr && colExpr.TableName == null)
                {
                    if (aliasMap.TryGetValue(colExpr.ColumnName, out var aliasedExpr))
                        expr = aliasedExpr;
                }

                var aVal = _evaluator.Evaluate(expr, a);
                var bVal = _evaluator.Evaluate(expr, b);

                int cmp;
                if (aVal.IsNull && bVal.IsNull) cmp = 0;
                else if (aVal.IsNull) cmp = -1;
                else if (bVal.IsNull) cmp = 1;
                else cmp = aVal.CompareTo(bVal);

                if (item.Descending) cmp = -cmp;
                if (cmp != 0) return cmp;
            }
            return 0;
        });
        return sorted;
    }

    private List<ResultRow> OrderResultRows(IReadOnlyList<OrderByItem> orderBy, List<ResultColumn> columns, List<ResultRow> rows)
    {
        var sorted = rows.ToList();
        sorted.Sort((a, b) =>
        {
            foreach (var item in orderBy)
            {
                int colIndex = -1;
                if (item.Expression is ColumnExpression col)
                {
                    for (int i = 0; i < columns.Count; i++)
                    {
                        if (columns[i].Name.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase))
                        {
                            colIndex = i;
                            break;
                        }
                    }
                }

                if (colIndex < 0) continue;

                var aVal = a[colIndex];
                var bVal = b[colIndex];

                int cmp;
                if (aVal.IsNull && bVal.IsNull) cmp = 0;
                else if (aVal.IsNull) cmp = -1;
                else if (bVal.IsNull) cmp = 1;
                else cmp = aVal.CompareTo(bVal);

                if (item.Descending) cmp = -cmp;
                if (cmp != 0) return cmp;
            }
            return 0;
        });
        return sorted;
    }

    private QueryResult ExecuteSetOperation(SetOperationStatement setOp)
    {
        var leftResult = Execute(setOp.Left);
        var rightResult = Execute(setOp.Right);

        // Verify compatible columns
        if (leftResult.Columns.Count != rightResult.Columns.Count)
            throw new InvalidOperationException("UNION/INTERSECT/EXCEPT requires same number of columns");

        List<ResultRow> resultRows;

        switch (setOp.Operation)
        {
            case SetOperationType.Union:
                resultRows = leftResult.Rows.Concat(rightResult.Rows).ToList();
                if (!setOp.All)
                {
                    resultRows = resultRows.DistinctBy(r =>
                        string.Join("\0", Enumerable.Range(0, r.Count).Select(i => r[i].RawValue?.ToString() ?? "NULL"))
                    ).ToList();
                }
                break;

            case SetOperationType.Intersect:
                var rightKeys = rightResult.Rows.Select(r =>
                    string.Join("\0", Enumerable.Range(0, r.Count).Select(i => r[i].RawValue?.ToString() ?? "NULL"))
                ).ToHashSet();
                resultRows = leftResult.Rows.Where(r =>
                    rightKeys.Contains(string.Join("\0", Enumerable.Range(0, r.Count).Select(i => r[i].RawValue?.ToString() ?? "NULL")))
                ).ToList();
                break;

            case SetOperationType.Except:
                var excludeKeys = rightResult.Rows.Select(r =>
                    string.Join("\0", Enumerable.Range(0, r.Count).Select(i => r[i].RawValue?.ToString() ?? "NULL"))
                ).ToHashSet();
                resultRows = leftResult.Rows.Where(r =>
                    !excludeKeys.Contains(string.Join("\0", Enumerable.Range(0, r.Count).Select(i => r[i].RawValue?.ToString() ?? "NULL")))
                ).ToList();
                break;

            default:
                throw new NotImplementedException($"Set operation {setOp.Operation} not implemented");
        }

        return new QueryResult(leftResult.Columns, resultRows);
    }

    private QueryResult ExecuteInsert(InsertStatement insert)
    {
        var table = _database.GetTable(insert.TableName);

        if (insert.Source is ValuesInsertSource values)
        {
            foreach (var row in values.Rows)
            {
                var rowValues = new List<ClickHouseValue>();
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (insert.Columns != null)
                    {
                        var colIndex = insert.Columns.ToList().FindIndex(c =>
                            c.Equals(table.Columns[i].Name, StringComparison.OrdinalIgnoreCase));
                        if (colIndex >= 0 && colIndex < row.Count)
                        {
                            var val = _evaluator.Evaluate(row[colIndex], new RowContext());
                            rowValues.Add(table.Columns[i].Type.CreateValue(val.RawValue));
                        }
                        else
                        {
                            rowValues.Add(table.Columns[i].Type.DefaultValue);
                        }
                    }
                    else if (i < row.Count)
                    {
                        var val = _evaluator.Evaluate(row[i], new RowContext());
                        rowValues.Add(table.Columns[i].Type.CreateValue(val.RawValue));
                    }
                    else
                    {
                        rowValues.Add(table.Columns[i].Type.DefaultValue);
                    }
                }
                table.InsertRow(rowValues);
            }
            return QueryResult.Affected(values.Rows.Count);
        }

        if (insert.Source is SelectInsertSource selectSource)
        {
            var result = ExecuteSelect(selectSource.Query);
            foreach (var row in result.Rows)
            {
                var rowValues = new List<ClickHouseValue>();
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    if (i < row.Count)
                        rowValues.Add(table.Columns[i].Type.CreateValue(row[i].RawValue));
                    else
                        rowValues.Add(table.Columns[i].Type.DefaultValue);
                }
                table.InsertRow(rowValues);
            }
            return QueryResult.Affected(result.Rows.Count);
        }

        throw new NotImplementedException("Unknown insert source type");
    }

    private QueryResult ExecuteCreateTable(CreateTableStatement create)
    {
        _database.CreateTable(create);
        return QueryResult.Empty;
    }

    private QueryResult ExecuteDropTable(DropTableStatement drop)
    {
        _database.DropTable(drop.TableName, drop.IfExists);
        return QueryResult.Empty;
    }

    private QueryResult ExecuteUpdate(UpdateStatement update)
    {
        var table = _database.GetTable(update.TableName);
        long count = 0;

        foreach (var row in table.GetRows())
        {
            var context = new RowContext();
            for (int i = 0; i < table.Columns.Count; i++)
                context.SetColumn(table.Columns[i].Name, row[i]);

            bool matches = true;
            if (update.Where != null)
            {
                var result = _evaluator.Evaluate(update.Where, context);
                matches = !result.IsNull && result.AsBool();
            }

            if (matches)
            {
                foreach (var assignment in update.Assignments)
                {
                    var colIndex = table.GetColumnIndex(assignment.Column);
                    if (colIndex >= 0)
                    {
                        var newVal = _evaluator.Evaluate(assignment.Value, context);
                        row[colIndex] = table.Columns[colIndex].Type.CreateValue(newVal.RawValue);
                    }
                }
                count++;
            }
        }

        return QueryResult.Affected(count);
    }

    private QueryResult ExecuteDelete(DeleteStatement delete)
    {
        var table = _database.GetTable(delete.TableName);
        var toDelete = new List<Row>();

        foreach (var row in table.GetRows())
        {
            var context = new RowContext();
            for (int i = 0; i < table.Columns.Count; i++)
                context.SetColumn(table.Columns[i].Name, row[i]);

            bool matches = true;
            if (delete.Where != null)
            {
                var result = _evaluator.Evaluate(delete.Where, context);
                matches = !result.IsNull && result.AsBool();
            }

            if (matches)
                toDelete.Add(row);
        }

        table.DeleteWhere(r => toDelete.Contains(r));
        return QueryResult.Affected(toDelete.Count);
    }
}
