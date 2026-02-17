using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using EF.CH.BulkInsert.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.TempTable;

/// <summary>
/// Handle for a ClickHouse temporary table. Provides methods for inserting data,
/// querying, and automatic cleanup via <see cref="IAsyncDisposable"/>.
/// </summary>
/// <typeparam name="T">The entity type mapped to this temp table.</typeparam>
public sealed class TempTableHandle<T> : IAsyncDisposable where T : class
{
    private readonly IRelationalConnection _relationalConnection;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly DbContext _context;
    private readonly EntityPropertyInfo _propertyInfo;
    private bool _disposed;

    // Regex to match ClickHouse parameter placeholders: {name:Type} or {name:Type(N)} or {name:Type(N,M)}
    private static readonly Regex ParameterPlaceholderRegex = new(@"\{(\w+):([^}]+)\}", RegexOptions.Compiled);

    internal TempTableHandle(
        string tableName,
        string quotedTableName,
        EntityPropertyInfo propertyInfo,
        IRelationalConnection relationalConnection,
        IRelationalTypeMappingSource typeMappingSource,
        DbContext context)
    {
        TableName = tableName;
        QuotedTableName = quotedTableName;
        _propertyInfo = propertyInfo;
        _relationalConnection = relationalConnection;
        _typeMappingSource = typeMappingSource;
        _context = context;
    }

    /// <summary>
    /// Gets the name of the temporary table.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// Gets the quoted name of the temporary table.
    /// </summary>
    public string QuotedTableName { get; }

    /// <summary>
    /// Returns a queryable for the temporary table with full LINQ composability.
    /// </summary>
    public IQueryable<T> Query()
    {
        ThrowIfDisposed();
        return _context.Set<T>().FromSqlRaw($"SELECT * FROM {QuotedTableName}");
    }

    /// <summary>
    /// Inserts a collection of entities into the temporary table.
    /// </summary>
    /// <param name="entities">The entities to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task InsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var entityList = entities as IReadOnlyList<T> ?? entities.ToList();
        if (entityList.Count == 0) return;

        // Use the property info but with the temp table name
        var tempPropertyInfo = _propertyInfo with { QuotedTableName = QuotedTableName };

        var sqlBuilder = new BulkInsertSqlBuilder(_typeMappingSource);
        var sql = sqlBuilder.Build(entityList, tempPropertyInfo, new Dictionary<string, object>());

        await using var command = _relationalConnection.DbConnection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Inserts the results of a query into the temporary table using server-side INSERT ... SELECT.
    /// </summary>
    /// <param name="sourceQuery">The source query.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task InsertFromQueryAsync(IQueryable<T> sourceQuery, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var (selectSql, parameterValues) = GetQueryStringAndParameters(sourceQuery);
        var selectSqlWithValues = ReplaceParametersWithLiterals(selectSql, parameterValues);

        var sql = $"INSERT INTO {QuotedTableName} ({_propertyInfo.ColumnList})\nSELECT {_propertyInfo.ColumnList} FROM (\n{selectSqlWithValues}\n) AS __subquery";

        await using var command = _relationalConnection.DbConnection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            await using var command = _relationalConnection.DbConnection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS {QuotedTableName}";
            await command.ExecuteNonQueryAsync();
        }
        finally
        {
            await _relationalConnection.CloseAsync();
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private (string Sql, Dictionary<string, object?> Parameters) GetQueryStringAndParameters(IQueryable<T> query)
    {
        var sql = query.ToQueryString();
        var parameterValues = new Dictionary<string, object?>();

        try
        {
            var enumerator = query.AsEnumerable().GetEnumerator();
            try
            {
                var enumeratorType = enumerator.GetType();

                var contextField = enumeratorType.GetField("_relationalQueryContext",
                    BindingFlags.NonPublic | BindingFlags.Instance);
                object? queryContext = contextField?.GetValue(enumerator);

                if (queryContext == null)
                {
                    contextField = enumeratorType.GetField("_queryContext",
                        BindingFlags.NonPublic | BindingFlags.Instance);
                    queryContext = contextField?.GetValue(enumerator);
                }

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

    private string ReplaceParametersWithLiterals(string sql, Dictionary<string, object?> parameterValues)
    {
        return ParameterPlaceholderRegex.Replace(sql, match =>
        {
            var paramName = match.Groups[1].Value;
            var typeName = match.Groups[2].Value;

            if (parameterValues.TryGetValue(paramName, out var value))
                return GenerateSqlLiteral(value, typeName);

            if (parameterValues.TryGetValue($"__{paramName}", out value))
                return GenerateSqlLiteral(value, typeName);

            if (paramName.StartsWith("__") &&
                parameterValues.TryGetValue(paramName.Substring(2), out value))
                return GenerateSqlLiteral(value, typeName);

            var baseName = ExtractBaseName(paramName);
            foreach (var (key, val) in parameterValues)
            {
                if (ExtractBaseName(key).Equals(baseName, StringComparison.OrdinalIgnoreCase))
                    return GenerateSqlLiteral(val, typeName);
            }

            throw new InvalidOperationException(
                $"Could not resolve parameter '{paramName}' (type: {typeName}). " +
                $"Available parameters: [{string.Join(", ", parameterValues.Keys)}].");
        });
    }

    private static string ExtractBaseName(string paramName)
    {
        var name = paramName.TrimStart('_');
        var indexMatch = Regex.Match(name, @"^(.+?)_\d+$");
        return indexMatch.Success ? indexMatch.Groups[1].Value : name;
    }

    private string GenerateSqlLiteral(object? value, string clickHouseType)
    {
        if (value is null) return "NULL";

        var clrType = value.GetType();
        var typeMapping = _typeMappingSource.FindMapping(clrType);
        if (typeMapping != null)
            return typeMapping.GenerateSqlLiteral(value);

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
                    if (value != null && value is not IQueryable && value is not DbContext)
                    {
                        var name = node.Member.Name;
                        if (!_nameIndexes.TryGetValue(name, out var index))
                            index = 0;
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
    }
}
