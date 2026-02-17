using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using EF.CH.BulkInsert.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.InsertSelect;

/// <summary>
/// Implementation of server-side INSERT ... SELECT operations for ClickHouse.
/// Enables efficient data movement without client round-trips.
/// </summary>
public sealed class ClickHouseInsertSelectExecutor : IClickHouseInsertSelectExecutor
{
    private readonly ICurrentDbContext _currentDbContext;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IRelationalConnection _relationalConnection;
    private readonly EntityPropertyCache _propertyCache;

    // Regex to match ClickHouse parameter placeholders: {name:Type} or {name:Type(N)} or {name:Type(N,M)}
    private static readonly Regex ParameterPlaceholderRegex = new(@"\{(\w+):([^}]+)\}", RegexOptions.Compiled);

    public ClickHouseInsertSelectExecutor(
        ICurrentDbContext currentDbContext,
        IRelationalTypeMappingSource typeMappingSource,
        ISqlGenerationHelper sqlGenerationHelper,
        IRelationalConnection relationalConnection)
    {
        _currentDbContext = currentDbContext;
        _typeMappingSource = typeMappingSource;
        _sqlGenerationHelper = sqlGenerationHelper;
        _relationalConnection = relationalConnection;

        _propertyCache = new EntityPropertyCache(
            typeMappingSource,
            sqlGenerationHelper,
            currentDbContext.Context.Model);
    }

    /// <inheritdoc />
    public async Task<ClickHouseInsertSelectResult> ExecuteAsync<TTarget>(
        IQueryable<TTarget> sourceQuery,
        CancellationToken cancellationToken = default) where TTarget : class
    {
        var propertyInfo = _propertyCache.GetPropertyInfo<TTarget>();

        // Get SQL and parameters using reflection on EF Core internals
        var (selectSql, parameterValues) = GetQueryStringAndParameters(sourceQuery);

        // Replace parameter placeholders with literal values
        var selectSqlWithValues = ReplaceParametersWithLiterals(selectSql, parameterValues);

        var sql = BuildInsertSelectSql(propertyInfo, selectSqlWithValues);

        return await ExecuteSqlAsync(sql, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ClickHouseInsertSelectResult> ExecuteAsync<TSource, TTarget>(
        IQueryable<TSource> sourceQuery,
        Expression<Func<TSource, TTarget>> mapping,
        CancellationToken cancellationToken = default)
        where TSource : class
        where TTarget : class
    {
        // Apply the mapping expression to transform the query
        var mappedQuery = sourceQuery.Select(mapping);

        var propertyInfo = _propertyCache.GetPropertyInfo<TTarget>();

        // Get SQL and parameters using reflection on EF Core internals
        var (selectSql, parameterValues) = GetQueryStringAndParameters(mappedQuery);

        // Replace parameter placeholders with literal values
        var selectSqlWithValues = ReplaceParametersWithLiterals(selectSql, parameterValues);

        var sql = BuildInsertSelectSql(propertyInfo, selectSqlWithValues);

        return await ExecuteSqlAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Gets the SQL string and parameter values from an IQueryable by accessing EF Core internals.
    /// Uses the query enumerator to trigger query compilation and access QueryContext.ParameterValues.
    /// </summary>
    private (string Sql, Dictionary<string, object?> Parameters) GetQueryStringAndParameters<T>(IQueryable<T> query)
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
                // EF Core's enumerator wraps a RelationalQueryContext that has ParameterValues
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
                    // Try to find any field that has ParameterValues property
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
                // Dispose the enumerator immediately - we don't need to iterate
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
                parameterValues.TryGetValue(paramName.Substring(2), out value))
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

            // Parameter not found - throw helpful error
            throw new InvalidOperationException(
                $"INSERT...SELECT: Could not resolve parameter '{paramName}' (type: {typeName}). " +
                $"Available parameters: [{string.Join(", ", parameterValues.Keys)}]. " +
                "For queries with captured variables that cannot be resolved, consider using " +
                "string literals or raw SQL with ExecuteSqlRawAsync().");
        });
    }

    /// <summary>
    /// Extracts the base name from a parameter name by stripping leading underscores and trailing index.
    /// </summary>
    private static string ExtractBaseName(string paramName)
    {
        // Strip leading underscores
        var name = paramName.TrimStart('_');
        // Strip trailing _N index
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

    /// <summary>
    /// Builds the INSERT ... SELECT SQL statement, wrapping the SELECT in a subquery
    /// to ensure only the required columns are selected (excluding computed columns).
    /// </summary>
    private static string BuildInsertSelectSql(EntityPropertyInfo propertyInfo, string selectSql)
    {
        // Count columns in INSERT list
        var insertColumnCount = propertyInfo.Properties.Count;

        // Check if we need to wrap the SELECT to filter columns
        // This is needed when the SELECT includes computed columns that aren't in the INSERT
        // We wrap the original SELECT in a subquery and select only the columns we need
        var wrappedSelect = $"SELECT {propertyInfo.ColumnList} FROM (\n{selectSql}\n) AS __subquery";

        return $"INSERT INTO {propertyInfo.QuotedTableName} ({propertyInfo.ColumnList})\n{wrappedSelect}";
    }

    private async Task<ClickHouseInsertSelectResult> ExecuteSqlAsync(string sql, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        await _relationalConnection.OpenAsync(cancellationToken);
        try
        {
            await using var command = _relationalConnection.DbConnection.CreateCommand();
            command.CommandText = sql;

            var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);

            return ClickHouseInsertSelectResult.Create(stopwatch, rowsAffected, sql);
        }
        finally
        {
            await _relationalConnection.CloseAsync();
        }
    }

    /// <summary>
    /// Expression visitor that extracts parameter values from closure captures.
    /// EF Core parameterizes captured variables, and this extracts their values.
    /// </summary>
    private sealed class ParameterValueExtractor : ExpressionVisitor
    {
        private readonly Dictionary<string, object?> _parameters = new();
        // Track seen names to generate matching EF Core indices
        private readonly Dictionary<string, int> _nameIndexes = new();

        public Dictionary<string, object?> Parameters => _parameters;

        protected override Expression VisitMember(MemberExpression node)
        {
            // Try to evaluate the member expression to get its value
            if (CanEvaluate(node))
            {
                try
                {
                    var value = EvaluateExpression(node);
                    if (value != null && !IsQueryableOrDbContext(value))
                    {
                        var name = node.Member.Name;

                        // EF Core uses __variableName_index naming convention
                        // where index is per-variable-name, starting at 0
                        if (!_nameIndexes.TryGetValue(name, out var index))
                        {
                            index = 0;
                        }
                        _nameIndexes[name] = index + 1;

                        // Store with all naming variations EF Core might use
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
            // Can evaluate if it's a constant, or a member access on something we can evaluate
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
            // Skip IQueryable and DbContext constants
            if (node.Value is IQueryable || node.Value is DbContext)
            {
                return base.VisitConstant(node);
            }

            return base.VisitConstant(node);
        }
    }
}
