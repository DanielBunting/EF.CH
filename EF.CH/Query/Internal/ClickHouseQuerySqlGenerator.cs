using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using EF.CH.Extensions;
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
    private bool _inUpdateContext;

    // Thread-local storage for query settings (set during translation, read during SQL generation)
    [ThreadStatic]
    private static Dictionary<string, object>? _currentQuerySettings;

    // Thread-local storage for WITH FILL / INTERPOLATE options
    [ThreadStatic]
    private static ClickHouseQueryCompilationContextOptions? _currentWithFillOptions;

    // Thread-local storage for PREWHERE expression
    [ThreadStatic]
    private static SqlExpression? _currentPreWhereExpression;

    // Thread-local storage for LIMIT BY options
    // Values may be int (direct) or DeferredParameter (resolved via ResolveDeferredParameters)
    [ThreadStatic]
    private static object? _currentLimitByLimit;

    [ThreadStatic]
    private static object? _currentLimitByOffset;

    [ThreadStatic]
    private static List<SqlExpression>? _currentLimitByExpressions;

    // Thread-local storage for GROUP BY modifier (stored as int for Interlocked.Exchange compatibility)
    [ThreadStatic]
    private static int _currentGroupByModifierValue;

    // Thread-local storage for raw SQL filter
    [ThreadStatic]
    private static string? _currentRawFilter;

    // Thread-local storage for deferred raw SQL filter
    [ThreadStatic]
    private static DeferredParameter? _currentDeferredRawFilter;

    // Thread-local storage for CTE definitions
    [ThreadStatic]
    private static List<CteDefinition>? _currentCteDefinitions;

    // Thread-local storage for ARRAY JOIN specifications
    [ThreadStatic]
    private static List<ArrayJoinSpec>? _currentArrayJoinSpecs;

    // Thread-local storage for ASOF JOIN metadata
    [ThreadStatic]
    private static AsofJoinInfo? _currentAsofJoin;

    // Thread-local storage for resolved CTE names (used when deferred CTE names are resolved)
    [ThreadStatic]
    private static Dictionary<string, string>? _resolvedCteNames;

    // Thread-local storage for parameter values, used for inline deferred resolution
    // during SQL generation when ResolveDeferredParameters may not have caught everything
    [ThreadStatic]
    private static IReadOnlyDictionary<string, object?>? _currentParameterValues;

    public ClickHouseQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        _sqlGenerationHelper = dependencies.SqlGenerationHelper;
        _typeMappingSource = typeMappingSource;
    }

    // Thread-local storage for deferred settings (from WithSettings with parameterized dictionary)
    [ThreadStatic]
    private static DeferredParameter? _currentDeferredSettings;

    // Thread-local storage for deferred setting pairs (from WithSetting with parameterized name/value)
    [ThreadStatic]
    private static List<(object Name, object Value)>? _currentDeferredSettingPairs;

    // Thread-local storage for sample options (may contain DeferredParameter)
    [ThreadStatic]
    private static object? _currentSampleFraction;

    [ThreadStatic]
    private static object? _currentSampleOffset;

    /// <summary>
    /// Sets query settings to be appended as SETTINGS clause.
    /// This is called during query translation and read during SQL generation.
    /// Settings are automatically cleared after being consumed by GenerateSettings().
    /// </summary>
    internal static void SetQuerySettings(
        Dictionary<string, object> settings,
        DeferredParameter? deferredSettings = null,
        List<(object Name, object Value)>? deferredSettingPairs = null)
    {
        _currentQuerySettings = settings.Count > 0 ? new Dictionary<string, object>(settings) : null;
        _currentDeferredSettings = deferredSettings;
        _currentDeferredSettingPairs = deferredSettingPairs is { Count: > 0 } ? new List<(object, object)>(deferredSettingPairs) : null;
    }

    /// <summary>
    /// Sets sample options for deferred resolution.
    /// </summary>
    internal static void SetSampleOptions(object? fraction, object? offset)
    {
        _currentSampleFraction = fraction;
        _currentSampleOffset = offset;
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
    /// Sets LIMIT BY options for SQL generation.
    /// LIMIT BY generates: LIMIT [offset,] limit BY column1[, column2, ...]
    /// </summary>
    internal static void SetLimitBy(object limit, object? offset, List<SqlExpression> expressions)
    {
        _currentLimitByLimit = limit;
        _currentLimitByOffset = offset;
        _currentLimitByExpressions = expressions;
    }

    /// <summary>
    /// Resolves any deferred parameter values in the thread-local state.
    /// Called by ClickHouseParameterBasedSqlProcessor when parameter values become available.
    /// </summary>
    internal static void ResolveDeferredParameters(IReadOnlyDictionary<string, object?>? parameterValues)
    {
        // Always store parameter values for inline resolution, even if empty
        if (parameterValues != null)
            _currentParameterValues = parameterValues;

        if (parameterValues == null || parameterValues.Count == 0)
            return;

        // Resolve LIMIT BY deferred parameters
        if (_currentLimitByLimit is DeferredParameter limitParam)
            _currentLimitByLimit = DeferredParameter.ResolveValue<int>(limitParam, parameterValues);

        if (_currentLimitByOffset is DeferredParameter offsetParam)
            _currentLimitByOffset = DeferredParameter.ResolveValue<int>(offsetParam, parameterValues);

        // Resolve SAMPLE deferred parameters
        if (_currentSampleFraction is DeferredParameter fractionParam)
            _currentSampleFraction = DeferredParameter.ResolveValue<double>(fractionParam, parameterValues);

        if (_currentSampleOffset is DeferredParameter sampleOffsetParam)
            _currentSampleOffset = DeferredParameter.ResolveValue<double>(sampleOffsetParam, parameterValues);

        // Resolve deferred settings dictionary (from WithSettings)
        if (_currentDeferredSettings is DeferredParameter deferredSettingsParam)
        {
            var dict = deferredSettingsParam.Resolve<IDictionary<string, object>>(parameterValues);
            _currentQuerySettings ??= new Dictionary<string, object>();
            foreach (var kvp in dict)
            {
                _currentQuerySettings[kvp.Key] = kvp.Value;
            }
            _currentDeferredSettings = null;
        }

        // Resolve deferred setting pairs (from WithSetting with parameterized name/value)
        if (_currentDeferredSettingPairs is { Count: > 0 })
        {
            _currentQuerySettings ??= new Dictionary<string, object>();
            foreach (var (nameVal, valueVal) in _currentDeferredSettingPairs)
            {
                var resolvedName = nameVal is DeferredParameter np
                    ? np.Resolve<string>(parameterValues)
                    : (string)nameVal;
                var resolvedValue = valueVal is DeferredParameter vp
                    ? vp.Resolve<object>(parameterValues)
                    : valueVal;
                _currentQuerySettings[resolvedName] = resolvedValue;
            }
            _currentDeferredSettingPairs = null;
        }

        // Resolve deferred values in existing settings dictionary
        if (_currentQuerySettings != null)
        {
            var keysToResolve = new List<string>();
            foreach (var kvp in _currentQuerySettings)
            {
                if (kvp.Value is DeferredParameter)
                    keysToResolve.Add(kvp.Key);
            }
            foreach (var key in keysToResolve)
            {
                var dp = (DeferredParameter)_currentQuerySettings[key];
                _currentQuerySettings[key] = dp.Resolve<object>(parameterValues);
            }
        }

        // Resolve deferred raw filter
        if (_currentDeferredRawFilter is DeferredParameter rawFilterParam)
        {
            _currentRawFilter = rawFilterParam.Resolve<string>(parameterValues);
            _currentDeferredRawFilter = null;
        }

        // Resolve deferred CTE names
        if (_currentCteDefinitions != null)
        {
            foreach (var cte in _currentCteDefinitions)
            {
                if (cte.NameValue is DeferredParameter cteNameParam)
                {
                    var resolved = cteNameParam.Resolve<string>(parameterValues);
                    cte.ResolveName(resolved);
                    // Store mapping so VisitCteReference can find the resolved name
                    // even after _currentCteDefinitions is consumed
                    _resolvedCteNames ??= new Dictionary<string, string>();
                    _resolvedCteNames["__deferred_cte__"] = resolved;
                }
            }
        }

        // Resolve deferred WITH FILL values
        if (_currentWithFillOptions != null)
        {
            foreach (var spec in _currentWithFillOptions.WithFillSpecs.Values)
            {
                if (spec.Step is DeferredParameter stepParam)
                    spec.Step = stepParam.Resolve<object>(parameterValues);
                if (spec.From is DeferredParameter fromParam)
                    spec.From = fromParam.Resolve<object>(parameterValues);
                if (spec.To is DeferredParameter toParam)
                    spec.To = toParam.Resolve<object>(parameterValues);
                if (spec.Staleness is DeferredParameter stalenessParam)
                    spec.Staleness = stalenessParam.Resolve<object>(parameterValues);
            }

            foreach (var spec in _currentWithFillOptions.InterpolateSpecs)
            {
                if (spec.ModeValue is DeferredParameter modeParam)
                {
                    var resolved = modeParam.Resolve<object>(parameterValues);
                    spec.ModeValue = resolved;
                    if (resolved is InterpolateMode m)
                        spec.Mode = m;
                }
                if (spec.ConstantValue is DeferredParameter constParam)
                    spec.ConstantValue = constParam.Resolve<object>(parameterValues);
            }

            // Resolve deferred interpolate builder
            if (_currentWithFillOptions.DeferredInterpolateBuilder is DeferredParameter builderParam)
            {
                var builderVal = builderParam.Resolve<object>(parameterValues);
                _currentWithFillOptions.DeferredInterpolateBuilder = null;

                if (builderVal != null)
                {
                    // Extract columns from the builder using reflection (same as TranslateInterpolate)
                    var builderType = builderVal.GetType();
                    var columnsProperty = builderType.GetProperty("Columns",
                        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

                    if (columnsProperty?.GetValue(builderVal) is System.Collections.IEnumerable columns)
                    {
                        foreach (var col in columns)
                        {
                            var colType = col.GetType();
                            var columnProp = colType.GetProperty("Column");
                            var modeProp = colType.GetProperty("Mode");
                            var constantProp = colType.GetProperty("Constant");
                            var isConstantProp = colType.GetProperty("IsConstant");

                            var columnExpr = columnProp?.GetValue(col) as System.Linq.Expressions.LambdaExpression;
                            var mode = modeProp?.GetValue(col);
                            var constant = constantProp?.GetValue(col);
                            var isConstant = (bool)(isConstantProp?.GetValue(col) ?? false);

                            if (columnExpr != null)
                            {
                                var body = columnExpr.Body;
                                if (body is System.Linq.Expressions.UnaryExpression unary
                                    && unary.NodeType == System.Linq.Expressions.ExpressionType.Convert)
                                    body = unary.Operand;

                                var columnName = body is System.Linq.Expressions.MemberExpression member
                                    ? member.Member.Name
                                    : body.ToString();

                                _currentWithFillOptions.InterpolateSpecs.Add(new InterpolateColumnSpec
                                {
                                    ColumnName = columnName,
                                    Mode = isConstant
                                        ? InterpolateMode.Default
                                        : (InterpolateMode)(mode ?? InterpolateMode.Default),
                                    IsConstant = isConstant,
                                    ConstantValue = constant
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// Sets GROUP BY modifier (ROLLUP, CUBE, or TOTALS) for SQL generation.
    /// </summary>
    internal static void SetGroupByModifier(GroupByModifier modifier)
    {
        _currentGroupByModifierValue = (int)modifier;
    }

    /// <summary>
    /// Sets a raw SQL filter to be AND-ed into the WHERE clause.
    /// Value may be a string (direct) or DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    internal static void SetRawFilter(object rawFilter)
    {
        _currentRawFilter = rawFilter is string s ? s : null;
        _currentDeferredRawFilter = rawFilter is DeferredParameter dp ? dp : null;
    }

    /// <summary>
    /// Sets CTE definitions to be prepended as a WITH clause.
    /// </summary>
    internal static void SetCteDefinitions(List<CteDefinition> definitions)
    {
        _currentCteDefinitions = definitions.Count > 0 ? definitions : null;
    }

    /// <summary>
    /// Sets ARRAY JOIN specifications for SQL generation.
    /// </summary>
    internal static void SetArrayJoinSpecs(List<ArrayJoinSpec> specs)
    {
        _currentArrayJoinSpecs = specs.Count > 0 ? new List<ArrayJoinSpec>(specs) : null;
    }

    /// <summary>
    /// Sets ASOF JOIN metadata for SQL generation.
    /// </summary>
    internal static void SetAsofJoin(AsofJoinInfo info)
    {
        _currentAsofJoin = info;
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
    /// Also generates INTERPOLATE, LIMIT BY, and SETTINGS clauses if present.
    /// Clause order: INTERPOLATE -> LIMIT BY -> LIMIT -> SETTINGS
    /// </summary>
    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        ArgumentNullException.ThrowIfNull(selectExpression);

        // Generate INTERPOLATE clause after ORDER BY but before LIMIT BY
        // (INTERPOLATE is consumed and cleared here to prevent state leakage)
        GenerateInterpolateClause();

        // Generate LIMIT BY clause (for top-N per group queries)
        GenerateLimitBy();

        // Generate global LIMIT clause
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
        // Check for ASOF JOIN override on InnerJoinExpression
        if (extensionExpression is InnerJoinExpression innerJoin && _currentAsofJoin != null)
        {
            return VisitAsofJoin(innerJoin);
        }

        return extensionExpression switch
        {
            ClickHouseWindowFunctionExpression windowExpression => VisitWindowFunction(windowExpression),
            ClickHouseDictionaryTableExpression dictionaryExpression => VisitDictionaryTable(dictionaryExpression),
            ClickHouseExternalTableFunctionExpression externalExpression => VisitExternalTableFunction(externalExpression),
            ClickHouseTableModifierExpression modifierExpression => VisitTableModifier(modifierExpression),
            ClickHouseFinalExpression finalExpression => VisitFinal(finalExpression),
            ClickHouseSampleExpression sampleExpression => VisitSample(sampleExpression),
            ClickHouseJsonPathExpression jsonPathExpression => VisitJsonPath(jsonPathExpression),
            ClickHouseCteReferenceExpression cteRef => VisitCteReference(cteRef),
            ClickHouseRawSqlExpression rawSql => VisitRawSql(rawSql),
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
        // Use the resolved thread-local values (which may have been deferred parameters)
        var fraction = _currentSampleFraction ?? expression.SampleFraction;
        var offset = _currentSampleOffset ?? expression.SampleOffset;

        if (fraction != null)
        {
            var fractionDouble = fraction is double d ? d : Convert.ToDouble(fraction);
            Sql.Append(" SAMPLE ");
            Sql.Append(fractionDouble.ToString("G", CultureInfo.InvariantCulture));

            if (offset != null)
            {
                var offsetDouble = offset is double od ? od : Convert.ToDouble(offset);
                Sql.Append(" OFFSET ");
                Sql.Append(offsetDouble.ToString("G", CultureInfo.InvariantCulture));
            }

            // Clear after consumption to prevent leakage
            _currentSampleFraction = null;
            _currentSampleOffset = null;
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
    /// Generates a CTE reference in the FROM clause.
    /// E.g., "cte_name" AS "alias"
    /// </summary>
    private Expression VisitCteReference(ClickHouseCteReferenceExpression expression)
    {
        // For deferred CTE names, look up the resolved name
        var cteName = expression.CteName;
        if (cteName == "__deferred_cte__")
        {
            // First try the resolved names map (persists after CTE definitions are consumed)
            if (_resolvedCteNames?.TryGetValue(cteName, out var resolvedName) == true)
            {
                cteName = resolvedName;
            }
            // Fall back to CTE definitions if still available
            else if (_currentCteDefinitions != null)
            {
                var resolved = _currentCteDefinitions.FirstOrDefault(c => c.Name != "__deferred_cte__");
                if (resolved != null)
                    cteName = resolved.Name;
            }
        }

        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(cteName));
        Sql.Append(" AS ");
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(expression.Alias!));

        // Clean up resolved names after use
        _resolvedCteNames = null;
        return expression;
    }

    private Expression VisitRawSql(ClickHouseRawSqlExpression expression)
    {
        Sql.Append(expression.Sql);
        return expression;
    }

    /// <summary>
    /// Generates the WITH clause for CTE definitions.
    /// </summary>
    private void GenerateWithClause(List<CteDefinition> definitions)
    {
        for (var i = 0; i < definitions.Count; i++)
        {
            if (i == 0)
            {
                Sql.Append("WITH ");
            }
            else
            {
                Sql.Append(", ");
            }

            var cte = definitions[i];
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(cte.Name));
            Sql.Append(" AS (");
            Sql.AppendLine();

            using (Sql.Indent())
            {
                if (cte.Body != null)
                {
                    Visit(cte.Body);
                }
                else if (cte.SourceTable != null)
                {
                    // Direct table reference — generate SELECT * FROM "table"
                    Sql.Append("SELECT * FROM ");
                    Visit(cte.SourceTable);
                }
            }

            Sql.AppendLine();
            Sql.Append(")");
            Sql.AppendLine();
        }
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
    /// Generates set operations with explicit ALL/DISTINCT qualifiers.
    /// ClickHouse requires UNION ALL or UNION DISTINCT — bare UNION is not accepted.
    /// Same applies to INTERSECT and EXCEPT.
    /// </summary>
    protected override void GenerateSetOperation(SetOperationBase setOperation)
    {
        GenerateSetOperationOperand(setOperation, setOperation.Source1);

        Sql.AppendLine();

        var operationKeyword = setOperation switch
        {
            UnionExpression => "UNION",
            IntersectExpression => "INTERSECT",
            ExceptExpression => "EXCEPT",
            _ => throw new InvalidOperationException($"Unknown set operation type: {setOperation.GetType().Name}")
        };

        Sql.Append(operationKeyword);
        Sql.Append(setOperation.IsDistinct ? " DISTINCT" : " ALL");
        Sql.AppendLine();

        GenerateSetOperationOperand(setOperation, setOperation.Source2);
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
        // Resolve any remaining DeferredParameter inline using stored parameter values
        if (value is DeferredParameter dp)
        {
            if (_currentParameterValues != null)
            {
                value = dp.Resolve<object>(_currentParameterValues);
                // Fall through to switch below with resolved value
            }
            else
            {
                Sql.Append(value.ToString() ?? "0");
                return;
            }
        }

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
        // Resolve any remaining DeferredParameter inline
        if (step is DeferredParameter dp)
        {
            if (_currentParameterValues != null)
                step = dp.Resolve<object>(_currentParameterValues);
            else
            {
                    Sql.Append(step.ToString() ?? "0");
                return;
            }
        }


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

            // Determine effective mode - ModeValue takes precedence (may have been deferred)
            var effectiveMode = spec.ModeValue is InterpolateMode mv ? mv : spec.Mode;

            if (effectiveMode == InterpolateMode.Prev)
            {
                // Forward-fill: column AS column
                Sql.Append(" AS ");
                Sql.Append(quotedName);
            }
            else if (spec.IsConstant && spec.ConstantValue != null)
            {
                // Constant value (DeferredParameter should already be resolved)
                Sql.Append(" AS ");
                GenerateFillValue(spec.ConstantValue);
            }
            // InterpolateMode.Default = no AS clause needed
        }

        Sql.Append(")");
    }

    /// <summary>
    /// Generates the GROUP BY modifier (WITH ROLLUP/CUBE/TOTALS) if set.
    /// </summary>
    private void GenerateGroupByModifier()
    {
        // Capture and clear the modifier (atomic exchange for int, then cast)
        var modifierValue = Interlocked.Exchange(ref _currentGroupByModifierValue, 0);
        var modifier = (GroupByModifier)modifierValue;

        switch (modifier)
        {
            case GroupByModifier.Rollup:
                Sql.Append(" WITH ROLLUP");
                break;
            case GroupByModifier.Cube:
                Sql.Append(" WITH CUBE");
                break;
            case GroupByModifier.Totals:
                Sql.Append(" WITH TOTALS");
                break;
        }
    }

    /// <summary>
    /// Generates the LIMIT BY clause for top-N per group queries.
    /// ClickHouse syntax: LIMIT [offset,] limit BY column1[, column2, ...]
    /// </summary>
    private void GenerateLimitBy()
    {
        // Capture and clear atomically to prevent leakage
        // Note: Interlocked.Exchange doesn't support nullable value types, so we capture and clear manually
        var limit = _currentLimitByLimit;
        var offset = _currentLimitByOffset;
        var expressions = Interlocked.Exchange(ref _currentLimitByExpressions, null);

        // Clear the value types after capture
        _currentLimitByLimit = null;
        _currentLimitByOffset = null;

        if (limit == null || expressions == null || expressions.Count == 0)
        {
            return;
        }

        Sql.AppendLine();
        Sql.Append("LIMIT ");

        if (offset is int offsetInt && offsetInt > 0)
        {
            Sql.Append(offsetInt.ToString(CultureInfo.InvariantCulture));
            Sql.Append(", ");
        }

        var limitInt = limit is int li ? li : Convert.ToInt32(limit);
        Sql.Append(limitInt.ToString(CultureInfo.InvariantCulture));
        Sql.Append(" BY ");

        for (var i = 0; i < expressions.Count; i++)
        {
            if (i > 0)
            {
                Sql.Append(", ");
            }
            Visit(expressions[i]);
        }
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
    /// Generates UPDATE statement for ClickHouse using ALTER TABLE ... UPDATE mutation syntax.
    /// ClickHouse does not support standard UPDATE; instead uses ALTER TABLE mutations.
    /// </summary>
    protected override Expression VisitUpdate(UpdateExpression updateExpression)
    {
        var selectExpression = updateExpression.SelectExpression;
        var table = updateExpression.Table;

        // ClickHouse mutations don't support joins
        if (selectExpression.Tables.Count > 1)
        {
            throw new InvalidOperationException(
                "ClickHouse does not support UPDATE with joins. " +
                "Simplify the query to update a single table without joins.");
        }

        // ALTER TABLE "table" UPDATE col1 = val1, col2 = val2 WHERE predicate
        Sql.Append("ALTER TABLE ");
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(table.Name, table.Schema));
        Sql.Append(" UPDATE ");

        _inUpdateContext = true;
        try
        {
            // Generate SET clause: col1 = val1, col2 = val2
            for (var i = 0; i < updateExpression.ColumnValueSetters.Count; i++)
            {
                if (i > 0)
                {
                    Sql.Append(", ");
                }

                var setter = updateExpression.ColumnValueSetters[i];
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(setter.Column.Name));
                Sql.Append(" = ");
                Visit(setter.Value);
            }

            // Generate WHERE clause (ClickHouse requires WHERE for mutations)
            if (selectExpression.Predicate != null)
            {
                Sql.AppendLine().Append("WHERE ");
                Visit(selectExpression.Predicate);
            }
            else
            {
                Sql.AppendLine().Append("WHERE 1");
            }
        }
        finally
        {
            _inUpdateContext = false;
        }

        return updateExpression;
    }

    /// <summary>
    /// Generates column reference.
    /// In DELETE/UPDATE context, omits table alias since ClickHouse mutations don't support aliases.
    /// </summary>
    protected override Expression VisitColumn(ColumnExpression columnExpression)
    {
        if (_inDeleteContext || _inUpdateContext)
        {
            // In DELETE/UPDATE context, use unqualified column name
            // ClickHouse mutations don't support table aliases
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
            return columnExpression;
        }

        return base.VisitColumn(columnExpression);
    }

    /// <summary>
    /// Generates SELECT statement with optional PREWHERE clause and GROUP BY modifiers.
    /// PREWHERE is injected between FROM and WHERE.
    /// GROUP BY modifiers (ROLLUP, CUBE, TOTALS) are appended after GROUP BY.
    /// </summary>
    protected override Expression VisitSelect(SelectExpression selectExpression)
    {
        // Capture and clear CTE definitions (only at outermost SELECT)
        var cteDefinitions = Interlocked.Exchange(ref _currentCteDefinitions, null);
        if (cteDefinitions?.Count > 0)
        {
            GenerateWithClause(cteDefinitions);
        }

        // Capture and clear PREWHERE expression atomically
        var preWhereExpr = Interlocked.Exchange(ref _currentPreWhereExpression, null);

        // Capture and clear raw filter atomically
        var rawFilter = Interlocked.Exchange(ref _currentRawFilter, null);

        // Check if we have a GROUP BY modifier - we need to handle this specially
        var hasGroupByModifier = _currentGroupByModifierValue != 0;
        var hasArrayJoin = _currentArrayJoinSpecs is { Count: > 0 };

        if (preWhereExpr == null && rawFilter == null && !hasGroupByModifier && !hasArrayJoin)
        {
            // No PREWHERE, no raw filter, no GROUP BY modifier, no ARRAY JOIN - use base implementation
            return base.VisitSelect(selectExpression);
        }

        if (preWhereExpr == null && rawFilter == null && hasGroupByModifier)
        {
            // No PREWHERE, no raw filter, but has GROUP BY modifier - need custom implementation
            return VisitSelectWithGroupByModifier(selectExpression);
        }

        // Generate SELECT with PREWHERE and/or raw filter injection
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

        // Generate projection (inlined for EF Core 8 compatibility)
        if (selectExpression.Projection.Count > 0)
        {
            for (var i = 0; i < selectExpression.Projection.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(selectExpression.Projection[i]);
            }
        }
        else
        {
            Sql.Append("1");
        }

        // Generate FROM clause (inlined for EF Core 8 compatibility)
        if (selectExpression.Tables.Count > 0)
        {
            Sql.AppendLine().Append("FROM ");
            for (var i = 0; i < selectExpression.Tables.Count; i++)
            {
                if (i > 0) Sql.AppendLine();
                Visit(selectExpression.Tables[i]);
            }
        }

        // Generate ARRAY JOIN (between FROM and PREWHERE)
        GenerateArrayJoin();

        // Generate PREWHERE (before WHERE)
        if (preWhereExpr != null)
        {
            Sql.AppendLine().Append("PREWHERE ");
            Visit(preWhereExpr);
        }

        // Generate WHERE (potentially with raw filter AND-ed)
        if (selectExpression.Predicate != null && rawFilter != null)
        {
            Sql.AppendLine().Append("WHERE (");
            Visit(selectExpression.Predicate);
            Sql.Append(") AND (");
            Sql.Append(rawFilter);
            Sql.Append(")");
        }
        else if (selectExpression.Predicate != null)
        {
            Sql.AppendLine().Append("WHERE ");
            Visit(selectExpression.Predicate);
        }
        else if (rawFilter != null)
        {
            Sql.AppendLine().Append("WHERE ");
            Sql.Append(rawFilter);
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

            // Append GROUP BY modifier if set
            GenerateGroupByModifier();
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
    /// Generates SELECT statement with GROUP BY modifier but no PREWHERE.
    /// This is used when we need to add ROLLUP, CUBE, or TOTALS to GROUP BY.
    /// </summary>
    private Expression VisitSelectWithGroupByModifier(SelectExpression selectExpression)
    {
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
            for (var i = 0; i < selectExpression.Projection.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(selectExpression.Projection[i]);
            }
        }
        else
        {
            Sql.Append("1");
        }

        // Generate FROM clause
        if (selectExpression.Tables.Count > 0)
        {
            Sql.AppendLine().Append("FROM ");
            for (var i = 0; i < selectExpression.Tables.Count; i++)
            {
                if (i > 0) Sql.AppendLine();
                Visit(selectExpression.Tables[i]);
            }
        }

        // Generate ARRAY JOIN (between FROM and WHERE)
        GenerateArrayJoin();

        // Generate WHERE
        if (selectExpression.Predicate != null)
        {
            Sql.AppendLine().Append("WHERE ");
            Visit(selectExpression.Predicate);
        }

        // Generate GROUP BY with modifier
        if (selectExpression.GroupBy.Count > 0)
        {
            Sql.AppendLine().Append("GROUP BY ");
            for (var i = 0; i < selectExpression.GroupBy.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(selectExpression.GroupBy[i]);
            }

            // Append GROUP BY modifier
            GenerateGroupByModifier();
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
    /// Generates ARRAY JOIN clause between FROM and PREWHERE/WHERE.
    /// </summary>
    private void GenerateArrayJoin()
    {
        var specs = Interlocked.Exchange(ref _currentArrayJoinSpecs, null);
        if (specs is not { Count: > 0 }) return;

        foreach (var group in specs.GroupBy(s => s.IsLeft))
        {
            Sql.AppendLine();
            Sql.Append(group.Key ? "LEFT ARRAY JOIN " : "ARRAY JOIN ");

            var first = true;
            foreach (var spec in group)
            {
                if (!first) Sql.Append(", ");
                first = false;
                Sql.Append(_sqlGenerationHelper.DelimitIdentifier(spec.ColumnName));
                if (spec.Alias != spec.ColumnName)
                {
                    Sql.Append(" AS ");
                    Sql.Append(_sqlGenerationHelper.DelimitIdentifier(spec.Alias));
                }
            }
        }
    }

    /// <summary>
    /// Generates ASOF JOIN SQL in place of a standard INNER JOIN.
    /// </summary>
    private Expression VisitAsofJoin(InnerJoinExpression innerJoinExpression)
    {
        var asofJoin = Interlocked.Exchange(ref _currentAsofJoin, null);

        Sql.AppendLine();
        Sql.Append(asofJoin!.IsLeft ? "ASOF LEFT JOIN " : "ASOF JOIN ");
        Visit(innerJoinExpression.Table);
        Sql.Append(" ON ");
        Visit(innerJoinExpression.JoinPredicate);

        // Append ASOF inequality condition (must be last in ON clause)
        Sql.Append(" AND ");

        var (leftAlias, rightAlias) = ExtractJoinAliases(
            innerJoinExpression.JoinPredicate, innerJoinExpression.Table);

        if (leftAlias != null)
        {
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(leftAlias));
            Sql.Append(".");
        }
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(asofJoin.LeftColumnName));
        Sql.Append(" ");
        Sql.Append(asofJoin.Operator);
        Sql.Append(" ");
        if (rightAlias != null)
        {
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(rightAlias));
            Sql.Append(".");
        }
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(asofJoin.RightColumnName));

        return innerJoinExpression;
    }

    /// <summary>
    /// Extracts table aliases from a join predicate by finding ColumnExpression nodes.
    /// Returns (leftAlias, rightAlias) where rightAlias matches the joined table.
    /// </summary>
    private static (string? leftAlias, string? rightAlias) ExtractJoinAliases(
        SqlExpression predicate, TableExpressionBase rightTable)
    {
        var rightAlias = rightTable.Alias;
        var aliases = new HashSet<string>();
        CollectAliases(predicate, aliases);
        if (rightAlias != null) aliases.Remove(rightAlias);
        var leftAlias = aliases.FirstOrDefault();
        return (leftAlias, rightAlias);
    }

    /// <summary>
    /// Recursively collects table aliases from ColumnExpression nodes in a SQL expression tree.
    /// </summary>
    private static void CollectAliases(SqlExpression expr, HashSet<string> aliases)
    {
        switch (expr)
        {
            case SqlBinaryExpression binary:
                CollectAliases(binary.Left, aliases);
                CollectAliases(binary.Right, aliases);
                break;
            case ColumnExpression col when col.TableAlias != null:
                aliases.Add(col.TableAlias);
                break;
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
