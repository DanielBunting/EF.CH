using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.QueryProfiling;

/// <summary>
/// Implementation of <see cref="IClickHouseQueryProfiler"/> for ClickHouse query profiling.
/// </summary>
public sealed class ClickHouseQueryProfiler : IClickHouseQueryProfiler
{
    private readonly ICurrentDbContext _currentDbContext;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly IRelationalConnection _relationalConnection;

    // Regex to match ClickHouse parameter placeholders: {name:Type} or {name:Type(N)} or {name:Type(N,M)}
    private static readonly Regex ParameterPlaceholderRegex = new(@"\{(\w+):([^}]+)\}", RegexOptions.Compiled);

    public ClickHouseQueryProfiler(
        ICurrentDbContext currentDbContext,
        IRelationalTypeMappingSource typeMappingSource,
        IRelationalConnection relationalConnection)
    {
        _currentDbContext = currentDbContext;
        _typeMappingSource = typeMappingSource;
        _relationalConnection = relationalConnection;
    }

    /// <inheritdoc />
    public async Task<ExplainResult> ExplainAsync<T>(
        IQueryable<T> query,
        Action<ExplainOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        // Get SQL and parameters, then substitute parameter values into the SQL
        var (sql, parameterValues) = GetQueryStringAndParameters(query);
        var substitutedSql = ReplaceParametersWithLiterals(sql, parameterValues);

        return await ExplainSqlCoreAsync(substitutedSql, sql, configure, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ExplainResult> ExplainSqlAsync(
        string sql,
        Action<ExplainOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        return await ExplainSqlCoreAsync(sql, sql, configure, cancellationToken);
    }

    private async Task<ExplainResult> ExplainSqlCoreAsync(
        string sqlToExecute,
        string originalSql,
        Action<ExplainOptions>? configure,
        CancellationToken cancellationToken)
    {
        var options = new ExplainOptions();
        configure?.Invoke(options);

        var explainSql = BuildExplainSql(sqlToExecute, options);
        var stopwatch = Stopwatch.StartNew();

        var output = new List<string>();
        var jsonOutput = new StringBuilder();

        await _relationalConnection.OpenAsync(cancellationToken);
        try
        {
            await using var command = _relationalConnection.DbConnection.CreateCommand();
            command.CommandText = explainSql;

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var line = reader.GetString(0);
                output.Add(line);

                if (options.Json)
                {
                    jsonOutput.AppendLine(line);
                }
            }
        }
        finally
        {
            await _relationalConnection.CloseAsync();
        }

        stopwatch.Stop();

        return new ExplainResult
        {
            Type = options.Type,
            Output = output,
            OriginalSql = originalSql,
            ExplainSql = explainSql,
            Elapsed = stopwatch.Elapsed,
            JsonOutput = options.Json ? jsonOutput.ToString().Trim() : null
        };
    }

    /// <inheritdoc />
    public async Task<QueryResultWithStats<T>> ToListWithStatsAsync<T>(
        IQueryable<T> query,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        var sql = query.ToQueryString();
        var stopwatch = Stopwatch.StartNew();

        // Execute the actual query
        var results = await query.ToListAsync(cancellationToken);

        // Try to retrieve statistics from system.query_log
        // Note: This is best-effort as ClickHouse logs asynchronously
        var statistics = await TryGetQueryStatisticsAsync(cancellationToken);

        stopwatch.Stop();

        return new QueryResultWithStats<T>
        {
            Results = results,
            Statistics = statistics,
            Sql = sql,
            Elapsed = stopwatch.Elapsed
        };
    }

    /// <summary>
    /// Gets the SQL string and parameter values from an IQueryable by accessing EF Core internals.
    /// </summary>
    private static (string Sql, Dictionary<string, object?> Parameters) GetQueryStringAndParameters<T>(IQueryable<T> query)
    {
        var sql = query.ToQueryString();
        var parameterValues = new Dictionary<string, object?>();

        // Method 1: Use AsEnumerable().GetEnumerator() to trigger query compilation
        // and access the QueryContext which contains parameter values
        try
        {
            var enumerator = query.AsEnumerable().GetEnumerator();
            try
            {
                // Access the QueryContext from the internal enumerable
                var enumeratorType = enumerator.GetType();

                // Try _relationalQueryContext field (used in some EF Core versions)
                var contextField = enumeratorType.GetField("_relationalQueryContext",
                    BindingFlags.NonPublic | BindingFlags.Instance);

                object? queryContext = contextField?.GetValue(enumerator);

                // Try _queryContext field if _relationalQueryContext not found
                if (queryContext == null)
                {
                    contextField = enumeratorType.GetField("_queryContext",
                        BindingFlags.NonPublic | BindingFlags.Instance);
                    queryContext = contextField?.GetValue(enumerator);
                }

                // Walk up the object graph looking for QueryContext
                if (queryContext == null)
                {
                    foreach (var field in enumeratorType.GetFields(BindingFlags.NonPublic | BindingFlags.Instance))
                    {
                        var fieldValue = field.GetValue(enumerator);
                        if (fieldValue != null)
                        {
                            var paramValuesProp = fieldValue.GetType().GetProperty("ParameterValues");
                            if (paramValuesProp != null)
                            {
                                queryContext = fieldValue;
                                break;
                            }
                        }
                    }
                }

                if (queryContext != null)
                {
                    var parameterValuesProperty = queryContext.GetType().GetProperty("ParameterValues");
                    if (parameterValuesProperty?.GetValue(queryContext) is IDictionary<string, object?> pv)
                    {
                        foreach (var kvp in pv)
                        {
                            parameterValues[kvp.Key] = kvp.Value;
                        }
                    }
                }
            }
            finally
            {
                (enumerator as IDisposable)?.Dispose();
            }
        }
        catch
        {
            // Fall through to expression-based extraction
        }

        // Method 2: Fall back to expression visitor for parameter extraction
        if (parameterValues.Count == 0)
        {
            var extractor = new ParameterValueExtractor();
            extractor.Visit(query.Expression);
            foreach (var kvp in extractor.Parameters)
            {
                parameterValues[kvp.Key] = kvp.Value;
            }
        }

        return (sql, parameterValues);
    }

    /// <summary>
    /// Replaces ClickHouse parameter placeholders {name:Type} with their literal values.
    /// </summary>
    private string ReplaceParametersWithLiterals(string sql, Dictionary<string, object?> parameterValues)
    {
        // If no parameters, return the SQL as-is
        if (!ParameterPlaceholderRegex.IsMatch(sql))
        {
            return sql;
        }

        return ParameterPlaceholderRegex.Replace(sql, match =>
        {
            var paramName = match.Groups[1].Value;
            var typeName = match.Groups[2].Value;

            // Try exact match first
            if (parameterValues.TryGetValue(paramName, out var value))
            {
                return GenerateSqlLiteral(value, typeName);
            }

            // Try with __ prefix (EF Core internal format)
            if (parameterValues.TryGetValue($"__{paramName}", out value))
            {
                return GenerateSqlLiteral(value, typeName);
            }

            // Try stripping __ prefix from SQL param name
            if (paramName.StartsWith("__") &&
                parameterValues.TryGetValue(paramName[2..], out value))
            {
                return GenerateSqlLiteral(value, typeName);
            }

            // Fuzzy match by base name (strip __prefix and _N suffix)
            var baseName = ExtractBaseName(paramName);
            foreach (var (key, val) in parameterValues)
            {
                if (ExtractBaseName(key).Equals(baseName, StringComparison.OrdinalIgnoreCase))
                {
                    return GenerateSqlLiteral(val, typeName);
                }
            }

            // Parameter not found - return original placeholder with warning
            // This allows EXPLAIN to still work for queries where parameters can't be resolved
            return match.Value;
        });
    }

    /// <summary>
    /// Extracts the base name from a parameter name by stripping leading underscores and trailing index.
    /// </summary>
    private static string ExtractBaseName(string paramName)
    {
        var name = paramName.TrimStart('_');
        var indexMatch = Regex.Match(name, @"^(.+?)_\d+$");
        return indexMatch.Success ? indexMatch.Groups[1].Value : name;
    }

    /// <summary>
    /// Generates a SQL literal for a value based on its type.
    /// </summary>
    private string GenerateSqlLiteral(object? value, string clickHouseType)
    {
        if (value is null)
        {
            return "NULL";
        }

        // Try to get a type mapping for proper literal generation
        var clrType = value.GetType();
        var typeMapping = _typeMappingSource.FindMapping(clrType);

        if (typeMapping != null)
        {
            return typeMapping.GenerateSqlLiteral(value);
        }

        // Fallback literal generation
        return value switch
        {
            string s => $"'{s.Replace("\\", "\\\\").Replace("'", "\\'")}'",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss}'",
            DateOnly d => $"'{d:yyyy-MM-dd}'",
            bool b => b ? "true" : "false",
            Guid g => $"'{g}'",
            decimal m => m.ToString(System.Globalization.CultureInfo.InvariantCulture),
            double d => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            float f => f.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "NULL"
        };
    }

    private static string BuildExplainSql(string sql, ExplainOptions options)
    {
        var type = options.Type switch
        {
            ExplainType.Ast => "AST",
            ExplainType.Syntax => "SYNTAX",
            ExplainType.QueryTree => "QUERY TREE",
            ExplainType.Plan => "PLAN",
            ExplainType.Pipeline => "PIPELINE",
            ExplainType.Estimate => "ESTIMATE",
            _ => "PLAN"
        };

        var settings = BuildSettings(options);
        return $"EXPLAIN {type}{settings} {sql}";
    }

    private static string BuildSettings(ExplainOptions options)
    {
        var settings = new List<string>();

        // Plan-specific settings
        if (options.Type == ExplainType.Plan)
        {
            if (options.Json) settings.Add("json = 1");
            if (options.Indexes) settings.Add("indexes = 1");
            if (options.Actions) settings.Add("actions = 1");
            if (options.Header) settings.Add("header = 1");
            if (options.Description) settings.Add("description = 1");
        }

        // Pipeline-specific settings
        if (options.Type == ExplainType.Pipeline)
        {
            if (options.Graph) settings.Add("graph = 1");
            if (options.Header) settings.Add("header = 1");
            if (options.Compact) settings.Add("compact = 1");
        }

        // QueryTree-specific settings
        if (options.Type == ExplainType.QueryTree)
        {
            if (options.Passes) settings.Add("passes = 1");
            if (options.Description) settings.Add("description = 1");
        }

        return settings.Count > 0 ? " " + string.Join(", ", settings) : string.Empty;
    }

    private async Task<QueryStatistics?> TryGetQueryStatisticsAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Wait briefly for the query log to be flushed
            await Task.Delay(100, cancellationToken);

            await _relationalConnection.OpenAsync(cancellationToken);
            try
            {
                await using var command = _relationalConnection.DbConnection.CreateCommand();
                command.CommandText = """
                    SELECT
                        read_rows,
                        read_bytes,
                        query_duration_ms,
                        memory_usage,
                        peak_memory_usage
                    FROM system.query_log
                    WHERE type = 'QueryFinish'
                      AND query_kind = 'Select'
                      AND event_time >= now() - INTERVAL 5 SECOND
                      AND query NOT LIKE '%system.query_log%'
                    ORDER BY event_time DESC
                    LIMIT 1
                    """;

                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    return new QueryStatistics
                    {
                        RowsRead = reader.GetInt64(0),
                        BytesRead = reader.GetInt64(1),
                        QueryDurationMs = reader.GetDouble(2),
                        MemoryUsage = reader.GetInt64(3),
                        PeakMemoryUsage = reader.GetInt64(4)
                    };
                }
            }
            finally
            {
                await _relationalConnection.CloseAsync();
            }
        }
        catch
        {
            // Statistics retrieval is best-effort; swallow exceptions
        }

        return null;
    }

    /// <summary>
    /// Expression visitor that extracts parameter values from closure captures.
    /// </summary>
    private sealed class ParameterValueExtractor : ExpressionVisitor
    {
        private readonly Dictionary<string, object?> _parameters = new();
        private readonly Dictionary<string, int> _nameIndexes = new();

        public Dictionary<string, object?> Parameters => _parameters;

        protected override Expression VisitMember(MemberExpression node)
        {
            if (CanEvaluate(node))
            {
                try
                {
                    var value = EvaluateExpression(node);
                    if (value != null && !IsQueryableOrDbContext(value))
                    {
                        var name = node.Member.Name;

                        if (!_nameIndexes.TryGetValue(name, out var index))
                        {
                            index = 0;
                        }
                        _nameIndexes[name] = index + 1;

                        _parameters.TryAdd($"__{name}_{index}", value);
                        _parameters.TryAdd($"__{name}", value);
                        _parameters.TryAdd(name, value);
                    }
                }
                catch
                {
                    // Ignore if we can't evaluate
                }
            }

            return base.VisitMember(node);
        }

        private static bool CanEvaluate(Expression expression)
        {
            return expression switch
            {
                ConstantExpression => true,
                MemberExpression me => CanEvaluate(me.Expression!),
                _ => false
            };
        }

        private static object? EvaluateExpression(Expression expression)
        {
            return expression switch
            {
                ConstantExpression ce => ce.Value,
                MemberExpression me => GetMemberValue(me),
                _ => Expression.Lambda(expression).Compile().DynamicInvoke()
            };
        }

        private static object? GetMemberValue(MemberExpression node)
        {
            var container = node.Expression != null ? EvaluateExpression(node.Expression) : null;

            return node.Member switch
            {
                FieldInfo field => field.GetValue(container),
                PropertyInfo property => property.GetValue(container),
                _ => null
            };
        }

        private static bool IsQueryableOrDbContext(object value)
        {
            return value is IQueryable || value is DbContext;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            return base.VisitConstant(node);
        }
    }
}
