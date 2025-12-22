using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using EF.CH.Query.Internal.Expressions;
using EF.CH.Query.Internal.WithFill;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal;

/// <summary>
/// Generates ClickHouse SQL from query expressions.
/// </summary>
public class ClickHouseQuerySqlGenerator : QuerySqlGenerator
{
    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly HashSet<string> _visitedParameters = new();
    private bool _inDeleteContext;

    // Thread-local storage for query settings (set during translation, read during SQL generation)
    [ThreadStatic]
    private static Dictionary<string, object>? _currentQuerySettings;

    // Thread-local storage for WITH FILL / INTERPOLATE options
    [ThreadStatic]
    private static ClickHouseQueryCompilationContextOptions? _currentWithFillOptions;

    // Thread-local storage for PREWHERE expression
    [ThreadStatic]
    private static SqlExpression? _currentPreWhereExpression;

    public ClickHouseQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        _sqlGenerationHelper = dependencies.SqlGenerationHelper;
        _typeMappingSource = typeMappingSource;
    }

    /// <summary>
    /// Sets query settings to be appended as SETTINGS clause.
    /// This is called during query translation and read during SQL generation.
    /// Settings are automatically cleared after being consumed by GenerateSettings().
    /// </summary>
    internal static void SetQuerySettings(Dictionary<string, object> settings)
    {
        _currentQuerySettings = settings.Count > 0 ? new Dictionary<string, object>(settings) : null;
    }

    /// <summary>
    /// Sets WITH FILL / INTERPOLATE options for SQL generation.
    /// </summary>
    internal static void SetWithFillOptions(ClickHouseQueryCompilationContextOptions options)
    {
        _currentWithFillOptions = (options.HasWithFill || options.HasInterpolate) ? options : null;
    }

    /// <summary>
    /// Sets PREWHERE expression to be generated before WHERE clause.
    /// </summary>
    internal static void SetPreWhereExpression(SqlExpression expression)
    {
        _currentPreWhereExpression = expression;
    }

    /// <summary>
    /// Clears query settings after SQL generation.
    /// </summary>
    /// <remarks>
    /// This method is no longer needed as settings are automatically cleared
    /// when consumed by GenerateSettings(). Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Settings are now auto-cleared after consumption. This method is no longer needed.")]
    internal static void ClearQuerySettings()
    {
        _currentQuerySettings = null;
    }

    /// <summary>
    /// Visits a SQL parameter expression and generates ClickHouse-format parameter placeholder.
    /// ClickHouse requires {name:Type} format for parameters.
    /// </summary>
    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var parameterName = sqlParameterExpression.Name;
        var typeMapping = sqlParameterExpression.TypeMapping;

        // Get the ClickHouse type name
        var storeType = typeMapping?.StoreType ?? "String";

        // Strip Nullable() wrapper if present - ClickHouse.Driver handles nullability differently
        if (storeType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase) &&
            storeType.EndsWith(")"))
        {
            storeType = storeType.Substring(9, storeType.Length - 10);
        }

        // Register the parameter with EF Core's command builder (only once per parameter name)
        if (_visitedParameters.Add(parameterName) && typeMapping != null)
        {
            Sql.AddParameter(
                parameterName,
                _sqlGenerationHelper.GenerateParameterName(parameterName),
                typeMapping,
                sqlParameterExpression.IsNullable);
        }

        // Generate ClickHouse parameter format: {name:Type}
        Sql.Append("{")
           .Append(parameterName)
           .Append(":")
           .Append(storeType)
           .Append("}");

        return sqlParameterExpression;
    }

    /// <summary>
    /// Generates LIMIT/OFFSET clauses using ClickHouse's syntax.
    /// ClickHouse uses: LIMIT [offset,] count
    /// Also generates INTERPOLATE and SETTINGS clauses if present.
    /// </summary>
    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        ArgumentNullException.ThrowIfNull(selectExpression);

        // Generate INTERPOLATE clause after ORDER BY but before LIMIT
        // (INTERPOLATE is consumed and cleared here to prevent state leakage)
        GenerateInterpolateClause();

        if (selectExpression.Limit is not null)
        {
            Sql.AppendLine()
                .Append("LIMIT ");

            if (selectExpression.Offset is not null)
            {
                Visit(selectExpression.Offset);
                Sql.Append(", ");
            }

            Visit(selectExpression.Limit);
        }
        else if (selectExpression.Offset is not null)
        {
            // ClickHouse requires a LIMIT when using OFFSET
            // Use max UInt64 value as effectively "unlimited"
            Sql.AppendLine()
                .Append("LIMIT ");
            Visit(selectExpression.Offset);
            Sql.Append(", 18446744073709551615");
        }

        // Generate SETTINGS clause if any query settings are present
        GenerateSettings();
    }

    /// <summary>
    /// Generates the SETTINGS clause for ClickHouse query settings.
    /// Clears the settings after consumption to prevent state leakage between queries.
    /// </summary>
    private void GenerateSettings()
    {
        // Capture and clear settings atomically to prevent leakage
        var settings = Interlocked.Exchange(ref _currentQuerySettings, null);

        if (settings == null || settings.Count == 0)
        {
            return;
        }

        Sql.AppendLine()
            .Append("SETTINGS ");

        var first = true;
        foreach (var setting in settings)
        {
            if (!first)
            {
                Sql.Append(", ");
            }
            first = false;

            Sql.Append(setting.Key)
                .Append(" = ")
                .Append(FormatSettingValue(setting.Value));
        }
    }

    /// <summary>
    /// Formats a setting value for the SETTINGS clause.
    /// </summary>
    private static string FormatSettingValue(object value)
    {
        return value switch
        {
            bool b => b ? "1" : "0",
            string s => $"'{s.Replace("'", "\\'")}'",
            int or long or short or byte or uint or ulong or ushort or sbyte => value.ToString()!,
            float f => f.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString()!
        };
    }

    /// <summary>
    /// Handles ClickHouse-specific expression types.
    /// </summary>
    protected override Expression VisitExtension(Expression extensionExpression)
    {
        return extensionExpression switch
        {
            ClickHouseWindowFunctionExpression windowExpression => VisitWindowFunction(windowExpression),
            ClickHouseDictionaryTableExpression dictionaryExpression => VisitDictionaryTable(dictionaryExpression),
            ClickHouseExternalTableFunctionExpression externalExpression => VisitExternalTableFunction(externalExpression),
            ClickHouseTableModifierExpression modifierExpression => VisitTableModifier(modifierExpression),
            ClickHouseFinalExpression finalExpression => VisitFinal(finalExpression),
            ClickHouseSampleExpression sampleExpression => VisitSample(sampleExpression),
            ClickHouseJsonPathExpression jsonPathExpression => VisitJsonPath(jsonPathExpression),
            _ => base.VisitExtension(extensionExpression)
        };
    }

    /// <summary>
    /// Generates dictionary table function call.
    /// E.g., dictionary('country_lookup') AS "c"
    /// </summary>
    private Expression VisitDictionaryTable(ClickHouseDictionaryTableExpression expression)
    {
        Sql.Append(expression.FunctionCall);
        Sql.Append(" AS ");
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(expression.Alias!));
        return expression;
    }

    /// <summary>
    /// Generates external table function call for remote data sources.
    /// E.g., postgresql('host:port', 'db', 'table', 'user', 'pass', 'schema') AS "alias"
    /// </summary>
    private Expression VisitExternalTableFunction(ClickHouseExternalTableFunctionExpression expression)
    {
        Sql.Append(expression.FunctionCall);
        Sql.Append(" AS ");
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(expression.Alias!));
        return expression;
    }

    /// <summary>
    /// Generates table reference with FINAL and SAMPLE modifiers.
    /// Called by the postprocessor-injected ClickHouseTableModifierExpression.
    /// </summary>
    private Expression VisitTableModifier(ClickHouseTableModifierExpression expression)
    {
        // Visit the underlying table (generates: "schema"."table" AS "t")
        Visit(expression.Table);

        // Add FINAL if requested
        if (expression.UseFinal)
        {
            Sql.Append(" FINAL");
        }

        // Add SAMPLE if requested
        if (expression.SampleFraction.HasValue)
        {
            Sql.Append(" SAMPLE ");
            Sql.Append(expression.SampleFraction.Value.ToString("G", CultureInfo.InvariantCulture));

            if (expression.SampleOffset.HasValue)
            {
                Sql.Append(" OFFSET ");
                Sql.Append(expression.SampleOffset.Value.ToString("G", CultureInfo.InvariantCulture));
            }
        }

        return expression;
    }

    /// <summary>
    /// Generates FINAL modifier for ReplacingMergeTree tables.
    /// </summary>
    private Expression VisitFinal(ClickHouseFinalExpression expression)
    {
        Visit(expression.Table);
        Sql.Append(" FINAL");
        return expression;
    }

    /// <summary>
    /// Generates SAMPLE clause for probabilistic sampling.
    /// </summary>
    private Expression VisitSample(ClickHouseSampleExpression expression)
    {
        Sql.Append(" SAMPLE ");
        Sql.Append(expression.Fraction.ToString("G", CultureInfo.InvariantCulture));

        if (expression.Offset.HasValue)
        {
            Sql.Append(" OFFSET ");
            Sql.Append(expression.Offset.Value.ToString("G", CultureInfo.InvariantCulture));
        }

        return expression;
    }

    /// <summary>
    /// Generates SQL for a JSON path expression using ClickHouse subcolumn syntax.
    /// E.g., "column"."path"."subpath" or "column"."array"[1]
    /// </summary>
    private Expression VisitJsonPath(ClickHouseJsonPathExpression expression)
    {
        // Visit the column (generates: "column" or "t"."column")
        Visit(expression.Column);

        // Append each path segment with proper quoting
        for (var i = 0; i < expression.PathSegments.Count; i++)
        {
            Sql.Append(".");
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(expression.PathSegments[i]));

            // Add array index if present (ClickHouse uses 1-based indexing)
            if (expression.ArrayIndices[i].HasValue)
            {
                // Add 1 to convert from 0-based C# index to 1-based ClickHouse index
                Sql.Append("[");
                Sql.Append((expression.ArrayIndices[i]!.Value + 1).ToString(CultureInfo.InvariantCulture));
                Sql.Append("]");
            }
        }

        return expression;
    }

    /// <summary>
    /// Generates SQL for a window function expression.
    /// E.g., row_number() OVER (PARTITION BY "Region" ORDER BY "Date" ASC)
    /// </summary>
    private Expression VisitWindowFunction(ClickHouseWindowFunctionExpression expression)
    {
        // Function name
        Sql.Append(expression.FunctionName);
        Sql.Append("(");

        // Function arguments (e.g., value and offset for lagInFrame)
        for (var i = 0; i < expression.Arguments.Count; i++)
        {
            if (i > 0) Sql.Append(", ");
            Visit(expression.Arguments[i]);
        }

        Sql.Append(") OVER (");

        var needsSpace = false;

        // PARTITION BY clause
        if (expression.PartitionBy.Count > 0)
        {
            Sql.Append("PARTITION BY ");
            for (var i = 0; i < expression.PartitionBy.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(expression.PartitionBy[i]);
            }
            needsSpace = true;
        }

        // ORDER BY clause
        if (expression.OrderBy.Count > 0)
        {
            if (needsSpace) Sql.Append(" ");
            Sql.Append("ORDER BY ");
            for (var i = 0; i < expression.OrderBy.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(expression.OrderBy[i].Expression);
                Sql.Append(expression.OrderBy[i].IsAscending ? " ASC" : " DESC");
            }
            needsSpace = true;
        }

        // Frame clause
        if (expression.Frame != null)
        {
            if (needsSpace) Sql.Append(" ");
            GenerateFrameClause(expression.Frame);
        }

        Sql.Append(")");

        return expression;
    }

    /// <summary>
    /// Generates the window frame clause.
    /// E.g., ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// </summary>
    private void GenerateFrameClause(WindowFrame frame)
    {
        Sql.Append(frame.Type == WindowFrameType.Rows ? "ROWS" : "RANGE");
        Sql.Append(" BETWEEN ");
        GenerateFrameBound(frame.StartBound, frame.StartOffset);
        Sql.Append(" AND ");
        GenerateFrameBound(frame.EndBound, frame.EndOffset);
    }

    /// <summary>
    /// Generates a frame boundary specification.
    /// </summary>
    private void GenerateFrameBound(WindowFrameBound bound, int? offset)
    {
        var text = bound switch
        {
            WindowFrameBound.UnboundedPreceding => "UNBOUNDED PRECEDING",
            WindowFrameBound.Preceding => $"{offset} PRECEDING",
            WindowFrameBound.CurrentRow => "CURRENT ROW",
            WindowFrameBound.Following => $"{offset} FOLLOWING",
            WindowFrameBound.UnboundedFollowing => "UNBOUNDED FOLLOWING",
            _ => throw new InvalidOperationException($"Unknown frame bound: {bound}")
        };
        Sql.Append(text);
    }

    /// <summary>
    /// Override to ensure proper SQL generation for ClickHouse-specific constructs.
    /// </summary>
    protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
    {
        // Handle ClickHouse-specific function naming
        return base.VisitSqlFunction(sqlFunctionExpression);
    }

    /// <summary>
    /// Generates SQL for an ordering expression, including WITH FILL if configured.
    /// </summary>
    protected override Expression VisitOrdering(OrderingExpression orderingExpression)
    {
        // Generate the base ordering (column ASC/DESC)
        var result = base.VisitOrdering(orderingExpression);

        // Check if this column has a WITH FILL spec
        var options = _currentWithFillOptions;
        if (options?.HasWithFill == true)
        {
            var columnName = GetOrderingColumnName(orderingExpression);
            if (columnName != null && options.WithFillSpecs.TryGetValue(columnName, out var fillSpec))
            {
                GenerateWithFillClause(fillSpec);
            }
        }

        return result;
    }

    /// <summary>
    /// Extracts the column name from an ordering expression for matching against WITH FILL specs.
    /// </summary>
    private static string? GetOrderingColumnName(OrderingExpression ordering)
    {
        var expression = ordering.Expression;

        // Handle column expressions
        if (expression is ColumnExpression column)
        {
            return column.Name;
        }

        // Handle projections/aliases
        if (expression is SqlUnaryExpression unary)
        {
            expression = unary.Operand;
            if (expression is ColumnExpression innerColumn)
            {
                return innerColumn.Name;
            }
        }

        return null;
    }

    /// <summary>
    /// Generates the WITH FILL clause for an ORDER BY column.
    /// </summary>
    private void GenerateWithFillClause(WithFillColumnSpec spec)
    {
        Sql.Append(" WITH FILL");

        if (spec.From != null)
        {
            Sql.Append(" FROM ");
            GenerateFillValue(spec.From);
        }

        if (spec.To != null)
        {
            Sql.Append(" TO ");
            GenerateFillValue(spec.To);
        }

        if (spec.Step != null)
        {
            Sql.Append(" STEP ");
            GenerateStepValue(spec.Step);
        }

        if (spec.Staleness != null)
        {
            Sql.Append(" STALENESS ");
            GenerateStepValue(spec.Staleness);
        }
    }

    /// <summary>
    /// Generates a FROM/TO value for WITH FILL.
    /// </summary>
    private void GenerateFillValue(object value)
    {
        switch (value)
        {
            case DateTime dt:
                Sql.Append($"toDateTime64('{dt:yyyy-MM-dd HH:mm:ss.fff}', 3)");
                break;
            case DateOnly d:
                Sql.Append($"toDate('{d:yyyy-MM-dd}')");
                break;
            case DateTimeOffset dto:
                Sql.Append($"toDateTime64('{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss.fff}', 3)");
                break;
            case int i:
                Sql.Append(i.ToString(CultureInfo.InvariantCulture));
                break;
            case long l:
                Sql.Append(l.ToString(CultureInfo.InvariantCulture));
                break;
            case double d:
                Sql.Append(d.ToString(CultureInfo.InvariantCulture));
                break;
            case decimal dec:
                Sql.Append(dec.ToString(CultureInfo.InvariantCulture));
                break;
            case float f:
                Sql.Append(f.ToString(CultureInfo.InvariantCulture));
                break;
            default:
                Sql.Append(value.ToString() ?? string.Empty);
                break;
        }
    }

    /// <summary>
    /// Generates a STEP or STALENESS value for WITH FILL.
    /// </summary>
    private void GenerateStepValue(object step)
    {
        switch (step)
        {
            case TimeSpan ts:
                // Convert TimeSpan to appropriate INTERVAL
                if (ts.TotalDays >= 1 && ts.TotalDays % 1 == 0)
                    Sql.Append($"INTERVAL {(int)ts.TotalDays} DAY");
                else if (ts.TotalHours >= 1 && ts.TotalHours % 1 == 0)
                    Sql.Append($"INTERVAL {(int)ts.TotalHours} HOUR");
                else if (ts.TotalMinutes >= 1 && ts.TotalMinutes % 1 == 0)
                    Sql.Append($"INTERVAL {(int)ts.TotalMinutes} MINUTE");
                else if (ts.TotalSeconds >= 1 && ts.TotalSeconds % 1 == 0)
                    Sql.Append($"INTERVAL {(int)ts.TotalSeconds} SECOND");
                else
                    Sql.Append($"INTERVAL {(long)ts.TotalMilliseconds} MILLISECOND");
                break;

            case ClickHouseInterval interval:
                Sql.Append(interval.ToSql());
                break;

            case int i:
                Sql.Append(i.ToString(CultureInfo.InvariantCulture));
                break;

            case long l:
                Sql.Append(l.ToString(CultureInfo.InvariantCulture));
                break;

            case double d:
                Sql.Append(d.ToString(CultureInfo.InvariantCulture));
                break;

            default:
                Sql.Append(step.ToString() ?? string.Empty);
                break;
        }
    }

    /// <summary>
    /// Generates the INTERPOLATE clause after ORDER BY.
    /// </summary>
    private void GenerateInterpolateClause()
    {
        var options = Interlocked.Exchange(ref _currentWithFillOptions, null);

        if (options?.HasInterpolate != true)
        {
            return;
        }

        Sql.AppendLine();
        Sql.Append("INTERPOLATE (");

        var first = true;
        foreach (var spec in options.InterpolateSpecs)
        {
            if (!first) Sql.Append(", ");
            first = false;

            var quotedName = _sqlGenerationHelper.DelimitIdentifier(spec.ColumnName);
            Sql.Append(quotedName);

            if (spec.Mode == InterpolateMode.Prev)
            {
                // Forward-fill: column AS column
                Sql.Append(" AS ");
                Sql.Append(quotedName);
            }
            else if (spec.IsConstant && spec.ConstantValue != null)
            {
                // Constant value
                Sql.Append(" AS ");
                GenerateFillValue(spec.ConstantValue);
            }
            // InterpolateMode.Default = no AS clause needed
        }

        Sql.Append(")");
    }

    /// <summary>
    /// Generates DELETE statement for ClickHouse.
    /// ClickHouse does not support table aliases in DELETE statements.
    /// </summary>
    protected override Expression VisitDelete(DeleteExpression deleteExpression)
    {
        var selectExpression = deleteExpression.SelectExpression;
        var table = deleteExpression.Table;

        // ClickHouse DELETE syntax: DELETE FROM "table" WHERE ...
        // Note: No table alias support
        Sql.Append("DELETE FROM ");
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(table.Name, table.Schema));

        if (selectExpression.Predicate != null)
        {
            Sql.AppendLine().Append("WHERE ");

            // Set flag to suppress table qualifiers in column references
            _inDeleteContext = true;
            try
            {
                Visit(selectExpression.Predicate);
            }
            finally
            {
                _inDeleteContext = false;
            }
        }

        return deleteExpression;
    }

    /// <summary>
    /// Generates column reference.
    /// In DELETE context, omits table alias since ClickHouse DELETE doesn't support aliases.
    /// </summary>
    protected override Expression VisitColumn(ColumnExpression columnExpression)
    {
        if (_inDeleteContext)
        {
            // In DELETE context, use unqualified column name
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
            return columnExpression;
        }

        return base.VisitColumn(columnExpression);
    }

    /// <summary>
    /// Generates SELECT statement with optional PREWHERE clause.
    /// PREWHERE is injected between FROM and WHERE.
    /// </summary>
    protected override Expression VisitSelect(SelectExpression selectExpression)
    {
        // Capture and clear PREWHERE expression atomically
        var preWhereExpr = Interlocked.Exchange(ref _currentPreWhereExpression, null);

        if (preWhereExpr == null)
        {
            // No PREWHERE - use base implementation
            return base.VisitSelect(selectExpression);
        }

        // Generate SELECT with PREWHERE injection
        IDisposable? subQueryIndent = null;

        if (selectExpression.Alias != null)
        {
            Sql.AppendLine("(");
            subQueryIndent = Sql.Indent();
        }

        Sql.Append("SELECT ");

        if (selectExpression.IsDistinct)
        {
            Sql.Append("DISTINCT ");
        }

        GenerateTop(selectExpression);

        // Generate projection
        if (selectExpression.Projection.Count > 0)
        {
            GenerateList(selectExpression.Projection, e => Visit(e));
        }
        else
        {
            Sql.Append("1");
        }

        // Generate FROM clause
        if (selectExpression.Tables.Count > 0)
        {
            Sql.AppendLine().Append("FROM ");
            GenerateList(selectExpression.Tables, e => Visit(e), sql => sql.AppendLine());
        }

        // Generate PREWHERE (before WHERE)
        Sql.AppendLine().Append("PREWHERE ");
        Visit(preWhereExpr);

        // Generate WHERE
        if (selectExpression.Predicate != null)
        {
            Sql.AppendLine().Append("WHERE ");
            Visit(selectExpression.Predicate);
        }

        // Generate GROUP BY
        if (selectExpression.GroupBy.Count > 0)
        {
            Sql.AppendLine().Append("GROUP BY ");
            for (var i = 0; i < selectExpression.GroupBy.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(selectExpression.GroupBy[i]);
            }
        }

        // Generate HAVING
        if (selectExpression.Having != null)
        {
            Sql.AppendLine().Append("HAVING ");
            Visit(selectExpression.Having);
        }

        // Generate ORDER BY (base method handles WITH FILL via VisitOrdering override)
        GenerateOrderings(selectExpression);

        // Generate LIMIT/OFFSET (includes INTERPOLATE and SETTINGS)
        GenerateLimitOffset(selectExpression);

        subQueryIndent?.Dispose();

        if (selectExpression.Alias != null)
        {
            Sql.AppendLine().Append(")");
            Sql.Append(AliasSeparator);
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(selectExpression.Alias));
        }

        return selectExpression;
    }

    /// <summary>
    /// Helper to generate comma-separated list of items.
    /// </summary>
    private void GenerateList<T>(
        IReadOnlyList<T> items,
        Action<T> generatorAction,
        Action<IRelationalCommandBuilder>? joinAction = null)
    {
        joinAction ??= (isb => isb.Append(", "));

        for (var i = 0; i < items.Count; i++)
        {
            if (i > 0) joinAction(Sql);
            generatorAction(items[i]);
        }
    }
}

/// <summary>
/// Factory for creating ClickHouse query SQL generators.
/// </summary>
public class ClickHouseQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseQuerySqlGeneratorFactory(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
    }

    public QuerySqlGenerator Create()
        => new ClickHouseQuerySqlGenerator(_dependencies, _typeMappingSource);
}
