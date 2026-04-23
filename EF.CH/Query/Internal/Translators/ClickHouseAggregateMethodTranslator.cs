using System.Reflection;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates LINQ aggregate methods to ClickHouse SQL aggregate functions.
/// Uses *OrNull variants per design decision for null-safe behavior.
/// </summary>
public class ClickHouseAggregateMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    // Model reference for defaultForNull lookups during translation
    private IModel? _currentModel;

    public ClickHouseAggregateMethodCallTranslator(
        ClickHouseSqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        IModel model,
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Store model for defaultForNull lookups
        _currentModel = model;

        // Handle ClickHouseAggregates extension methods
        if (method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseAggregates")
        {
            return TranslateClickHouseAggregate(method, source, arguments);
        }

        // Only handle Queryable and Enumerable extension methods
        if (method.DeclaringType != typeof(Queryable) &&
            method.DeclaringType != typeof(Enumerable))
        {
            return null;
        }

        var methodName = method.Name;

        // Get the selector expression if present - for scalar aggregates,
        // the selector is passed in arguments[0]
        var selector = arguments.Count > 0 ? arguments[0] : null;

        return methodName switch
        {
            nameof(Queryable.Average) => TranslateAverage(source, selector, method.ReturnType),
            nameof(Queryable.Sum) => TranslateSum(source, selector, method.ReturnType),
            nameof(Queryable.Min) => TranslateMin(source, selector, method.ReturnType),
            nameof(Queryable.Max) => TranslateMax(source, selector, method.ReturnType),
            nameof(Queryable.Count) => TranslateCount(source, selector),
            nameof(Queryable.LongCount) => TranslateLongCount(source, selector),
            _ => null
        };
    }

    /// <summary>
    /// Translates Average to avgOrNull for null-safe behavior.
    /// For defaultForNull columns, uses avgOrNullIf to exclude defaultForNull values.
    /// </summary>
    private SqlExpression? TranslateAverage(
        EnumerableExpression source,
        SqlExpression? selector,
        Type returnType)
    {
        var argument = GetAggregateArgument(source, selector);
        if (argument is null)
        {
            return null;
        }

        // For integer types, convert to Float64 first to avoid integer division
        var convertedArgument = argument;
        if (argument.Type == typeof(int) || argument.Type == typeof(long) ||
            argument.Type == typeof(short) || argument.Type == typeof(byte))
        {
            convertedArgument = _sqlExpressionFactory.Convert(argument, typeof(double));
        }

        // Check if this column has a defaultForNull default
        var defaultForNullCondition = TryCreateSentinelExclusionCondition(argument);
        if (defaultForNullCondition != null)
        {
            // Use avgOrNullIf(value, condition) to exclude defaultForNull values
            return _sqlExpressionFactory.Function(
                "avgOrNullIf",
                [convertedArgument, defaultForNullCondition],
                nullable: true,
                argumentsPropagateNullability: [false, false],
                returnType);
        }

        // Use avgOrNull for null-safe behavior (returns NULL for empty set)
        return _sqlExpressionFactory.Function(
            "avgOrNull",
            [convertedArgument],
            nullable: true,
            argumentsPropagateNullability: [false],
            returnType);
    }

    /// <summary>
    /// Translates Sum to sumOrNull for null-safe behavior.
    /// For defaultForNull columns, uses sumOrNullIf to exclude defaultForNull values.
    /// ClickHouse sum returns Int64, so we wrap in toInt32/toInt64 for type compatibility.
    /// </summary>
    private SqlExpression? TranslateSum(
        EnumerableExpression source,
        SqlExpression? selector,
        Type returnType)
    {
        var argument = GetAggregateArgument(source, selector);
        if (argument is null)
        {
            return null;
        }

        // ClickHouse sumOrNull returns Int64/UInt64, but we need to match the expected return type
        // The actual return type from ClickHouse for sum
        var clickHouseReturnType = typeof(long?);

        SqlExpression sumExpr;

        // Check if this column has a defaultForNull default
        var defaultForNullCondition = TryCreateSentinelExclusionCondition(argument);
        if (defaultForNullCondition != null)
        {
            // Use sumOrNullIf(value, condition) to exclude defaultForNull values
            sumExpr = _sqlExpressionFactory.Function(
                "sumOrNullIf",
                [argument, defaultForNullCondition],
                nullable: true,
                argumentsPropagateNullability: [false, false],
                clickHouseReturnType);
        }
        else
        {
            // Use sumOrNull for null-safe behavior (returns NULL for empty set)
            sumExpr = _sqlExpressionFactory.Function(
                "sumOrNull",
                [argument],
                nullable: true,
                argumentsPropagateNullability: [false],
                clickHouseReturnType);
        }

        // If return type differs from Int64, wrap in conversion
        // Use CAST for type conversion (toInt32OrNull is for string parsing)
        var underlyingReturnType = Nullable.GetUnderlyingType(returnType) ?? returnType;
        if (underlyingReturnType == typeof(int))
        {
            // Cast Int64 to Int32
            return _sqlExpressionFactory.Convert(sumExpr, returnType);
        }

        // For long/Int64, just ensure type mapping is correct
        return _sqlExpressionFactory.Convert(sumExpr, returnType);
    }

    /// <summary>
    /// Translates Min to minOrNull for null-safe behavior.
    /// For defaultForNull columns, uses minOrNullIf to exclude defaultForNull values.
    /// </summary>
    private SqlExpression? TranslateMin(
        EnumerableExpression source,
        SqlExpression? selector,
        Type returnType)
    {
        var argument = GetAggregateArgument(source, selector);
        if (argument is null)
        {
            return null;
        }

        // Check if this column has a defaultForNull default
        var defaultForNullCondition = TryCreateSentinelExclusionCondition(argument);
        if (defaultForNullCondition != null)
        {
            // Use minOrNullIf(value, condition) to exclude defaultForNull values
            return _sqlExpressionFactory.Function(
                "minOrNullIf",
                [argument, defaultForNullCondition],
                nullable: true,
                argumentsPropagateNullability: [false, false],
                returnType);
        }

        return _sqlExpressionFactory.Function(
            "minOrNull",
            [argument],
            nullable: true,
            argumentsPropagateNullability: [false],
            returnType);
    }

    /// <summary>
    /// Translates Max to maxOrNull for null-safe behavior.
    /// For defaultForNull columns, uses maxOrNullIf to exclude defaultForNull values.
    /// </summary>
    private SqlExpression? TranslateMax(
        EnumerableExpression source,
        SqlExpression? selector,
        Type returnType)
    {
        var argument = GetAggregateArgument(source, selector);
        if (argument is null)
        {
            return null;
        }

        // Check if this column has a defaultForNull default
        var defaultForNullCondition = TryCreateSentinelExclusionCondition(argument);
        if (defaultForNullCondition != null)
        {
            // Use maxOrNullIf(value, condition) to exclude defaultForNull values
            return _sqlExpressionFactory.Function(
                "maxOrNullIf",
                [argument, defaultForNullCondition],
                nullable: true,
                argumentsPropagateNullability: [false, false],
                returnType);
        }

        return _sqlExpressionFactory.Function(
            "maxOrNull",
            [argument],
            nullable: true,
            argumentsPropagateNullability: [false],
            returnType);
    }

    /// <summary>
    /// Translates Count. Count always returns 0 for empty sets, not NULL.
    /// ClickHouse returns UInt64, we wrap in toInt32() for .NET compatibility.
    /// </summary>
    private SqlExpression? TranslateCount(
        EnumerableExpression source,
        SqlExpression? predicate)
    {
        SqlExpression countExpr;

        // Simple count without predicate: count()
        if (predicate is null)
        {
            countExpr = _sqlExpressionFactory.Function(
                "count",
                Array.Empty<SqlExpression>(),
                nullable: false,
                argumentsPropagateNullability: Array.Empty<bool>(),
                typeof(ulong));
        }
        else
        {
            // Count with predicate: countIf(predicate)
            countExpr = _sqlExpressionFactory.Function(
                "countIf",
                [predicate],
                nullable: false,
                argumentsPropagateNullability: [false],
                typeof(ulong));
        }

        // Wrap in toInt32() for .NET Int32 compatibility
        return _sqlExpressionFactory.Function(
            "toInt32",
            [countExpr],
            nullable: false,
            argumentsPropagateNullability: [false],
            typeof(int));
    }

    /// <summary>
    /// Translates LongCount. Always returns 0 for empty sets.
    /// ClickHouse returns UInt64, we wrap in toInt64() for .NET compatibility.
    /// </summary>
    private SqlExpression? TranslateLongCount(
        EnumerableExpression source,
        SqlExpression? predicate)
    {
        SqlExpression countExpr;

        // Simple count without predicate
        if (predicate is null)
        {
            countExpr = _sqlExpressionFactory.Function(
                "count",
                Array.Empty<SqlExpression>(),
                nullable: false,
                argumentsPropagateNullability: Array.Empty<bool>(),
                typeof(ulong));
        }
        else
        {
            // Count with predicate
            countExpr = _sqlExpressionFactory.Function(
                "countIf",
                [predicate],
                nullable: false,
                argumentsPropagateNullability: [false],
                typeof(ulong));
        }

        // Wrap in toInt64() for .NET Int64 compatibility
        return _sqlExpressionFactory.Function(
            "toInt64",
            [countExpr],
            nullable: false,
            argumentsPropagateNullability: [false],
            typeof(long));
    }

    /// <summary>
    /// Gets the argument expression for an aggregate function.
    /// In EF Core 10+, the translated selector is in EnumerableExpression.Selector,
    /// not in the arguments list.
    /// </summary>
    private SqlExpression? GetAggregateArgument(EnumerableExpression source, SqlExpression? selector)
    {
        // First check if selector was passed directly (some code paths)
        if (selector is SqlExpression sqlSelector)
        {
            return sqlSelector;
        }

        // In EF Core 10+, the selector is stored in EnumerableExpression.Selector
        // after being translated
        if (source.Selector is SqlExpression sourceSelector)
        {
            return sourceSelector;
        }

        // For simple aggregates like Count(), we might not have a selector
        return null;
    }

    /// <summary>
    /// Creates a condition to exclude defaultForNull values from aggregation.
    /// Returns column != defaultForNull for columns with defaultForNull defaults.
    /// </summary>
    /// <remarks>
    /// Looks up defaultForNull annotations directly from the model to automatically exclude
    /// defaultForNull values (which represent NULL in .NET) from aggregate calculations.
    /// </remarks>
    private SqlExpression? TryCreateSentinelExclusionCondition(SqlExpression argument)
    {
        // Only handle column expressions
        if (argument is not ColumnExpression column || _currentModel == null)
        {
            return null;
        }

        // Look up the defaultForNull value for this column from the model
        var defaultForNull = FindDefaultForNullValue(column);
        if (defaultForNull == null)
        {
            return null;
        }

        // Create: column != defaultForNull
        var typeMapping = _typeMappingSource.FindMapping(defaultForNull.GetType());
        var defaultForNullConstant = _sqlExpressionFactory.Constant(defaultForNull, typeMapping);
        return _sqlExpressionFactory.NotEqual(column, defaultForNullConstant);
    }

    /// <summary>
    /// Finds the defaultForNull value for a column by looking up the property in the model.
    /// </summary>
    private object? FindDefaultForNullValue(ColumnExpression column)
    {
        if (_currentModel == null)
        {
            return null;
        }

        // Search all entity types for a property matching this column
        foreach (var entityType in _currentModel.GetEntityTypes())
        {
            var tableName = entityType.GetTableName();
            if (tableName == null)
            {
                continue;
            }

            // Check if this column belongs to this table
            // ColumnExpression.TableAlias might differ, so we match by column name
            foreach (var property in entityType.GetProperties())
            {
                var columnName = property.GetColumnName() ?? property.Name;
                if (string.Equals(columnName, column.Name, StringComparison.OrdinalIgnoreCase))
                {
                    // Found matching property, check for defaultForNull annotation
                    var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull);
                    if (annotation?.Value != null)
                    {
                        return annotation.Value;
                    }
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Translates ClickHouseAggregates extension methods to ClickHouse SQL aggregate functions.
    /// </summary>
    private SqlExpression? TranslateClickHouseAggregate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments)
    {
        var methodName = method.Name;
        var returnType = method.ReturnType;

        // Get the aggregate argument (the selector expression result)
        var argument = GetAggregateArgument(source, arguments.Count > 0 ? arguments[0] : null);

        return methodName switch
        {
            // Phase 1 - Simple single-selector aggregates
            "Uniq" => TranslateSimpleAggregate("uniq", argument, typeof(ulong)),
            "UniqExact" => TranslateSimpleAggregate("uniqExact", argument, typeof(ulong)),
            "AnyValue" => TranslateSimpleAggregate("any", argument, returnType),
            "AnyLastValue" => TranslateSimpleAggregate("anyLast", argument, returnType),

            // Phase 1 - Two-selector aggregates
            "ArgMax" => TranslateTwoArgAggregate("argMax", arguments, returnType),
            "ArgMin" => TranslateTwoArgAggregate("argMin", arguments, returnType),

            // Phase 2 - Statistical aggregates
            "Median" => TranslateSimpleAggregate("median", argument, typeof(double)),
            "StddevPop" => TranslateSimpleAggregate("stddevPop", argument, typeof(double)),
            "StddevSamp" => TranslateSimpleAggregate("stddevSamp", argument, typeof(double)),
            "VarPop" => TranslateSimpleAggregate("varPop", argument, typeof(double)),
            "VarSamp" => TranslateSimpleAggregate("varSamp", argument, typeof(double)),

            // Phase 2 - Parameterized aggregates
            "Quantile" => TranslateQuantile(arguments),

            // Approximate count distinct
            "UniqCombined" => TranslateSimpleAggregate("uniqCombined", argument, typeof(ulong)),
            "UniqCombined64" => TranslateSimpleAggregate("uniqCombined64", argument, typeof(ulong)),
            "UniqHLL12" => TranslateSimpleAggregate("uniqHLL12", argument, typeof(ulong)),
            "UniqTheta" => TranslateSimpleAggregate("uniqTheta", argument, typeof(ulong)),

            // Quantile variants
            "QuantileTDigest" => TranslateParametricQuantile("quantileTDigest", arguments),
            "QuantileDD" => TranslateQuantileDD(arguments),
            "QuantileExact" => TranslateParametricQuantile("quantileExact", arguments),
            "QuantileTiming" => TranslateParametricQuantile("quantileTiming", arguments),

            // Multi-quantile
            "Quantiles" => TranslateMultiQuantile("quantiles", arguments),
            "QuantilesTDigest" => TranslateMultiQuantile("quantilesTDigest", arguments),

            // Phase 3 - Array aggregates
            "GroupArray" => TranslateGroupArray(arguments, returnType),
            "GroupUniqArray" => TranslateSimpleAggregate("groupUniqArray", argument, returnType),
            "TopK" => TranslateTopK(arguments, returnType),
            "TopKWeighted" => TranslateTopKWeighted(arguments, returnType),

            // State combinators - return byte[] for AggregatingMergeTree storage
            "CountState" => TranslateNoArgAggregate("countState", typeof(byte[])),
            "SumState" => TranslateSimpleAggregate("sumState", argument, typeof(byte[])),
            "AvgState" => TranslateSimpleAggregate("avgState", argument, typeof(byte[])),
            "MinState" => TranslateSimpleAggregate("minState", argument, typeof(byte[])),
            "MaxState" => TranslateSimpleAggregate("maxState", argument, typeof(byte[])),
            "UniqState" => TranslateSimpleAggregate("uniqState", argument, typeof(byte[])),
            "UniqExactState" => TranslateSimpleAggregate("uniqExactState", argument, typeof(byte[])),
            "QuantileState" => TranslateQuantileState(arguments),
            "AnyState" => TranslateSimpleAggregate("anyState", argument, typeof(byte[])),
            "AnyLastState" => TranslateSimpleAggregate("anyLastState", argument, typeof(byte[])),

            // Merge combinators - read from AggregatingMergeTree state columns
            "CountMerge" => TranslateSimpleAggregate("countMerge", argument, typeof(long)),
            "SumMerge" => TranslateSimpleAggregate("sumMerge", argument, returnType),
            "AvgMerge" => TranslateSimpleAggregate("avgMerge", argument, typeof(double)),
            "MinMerge" => TranslateSimpleAggregate("minMerge", argument, returnType),
            "MaxMerge" => TranslateSimpleAggregate("maxMerge", argument, returnType),
            "UniqMerge" => TranslateSimpleAggregate("uniqMerge", argument, typeof(ulong)),
            "UniqExactMerge" => TranslateSimpleAggregate("uniqExactMerge", argument, typeof(ulong)),
            "QuantileMerge" => TranslateQuantileMerge(arguments),
            "AnyMerge" => TranslateSimpleAggregate("anyMerge", argument, returnType),
            "AnyLastMerge" => TranslateSimpleAggregate("anyLastMerge", argument, returnType),

            // If combinators - conditional aggregation
            "CountIf" => TranslateCountIfCombinator(argument),
            "SumIf" => TranslateSumIfCombinator(arguments, returnType),
            "AvgIf" => TranslateIfCombinator("avgIf", arguments, typeof(double)),
            "MinIf" => TranslateIfCombinator("minIf", arguments, returnType),
            "MaxIf" => TranslateIfCombinator("maxIf", arguments, returnType),
            "UniqIf" => TranslateIfCombinator("uniqIf", arguments, typeof(ulong)),
            "UniqExactIf" => TranslateIfCombinator("uniqExactIf", arguments, typeof(ulong)),
            "AnyIf" => TranslateIfCombinator("anyIf", arguments, returnType),
            "AnyLastIf" => TranslateIfCombinator("anyLastIf", arguments, returnType),
            "QuantileIf" => TranslateQuantileIf(arguments),
            "ArgMaxIf" => TranslateTwoArgIfCombinator("argMaxIf", arguments, returnType),
            "ArgMinIf" => TranslateTwoArgIfCombinator("argMinIf", arguments, returnType),
            "TopKIf" => TranslateTopKIf(arguments, returnType),
            "TopKWeightedIf" => TranslateTopKWeightedIf(arguments, returnType),
            "GroupArrayIf" => TranslateGroupArrayIf(arguments, returnType),
            "GroupUniqArrayIf" => TranslateGroupUniqArrayIf(arguments, returnType),
            "MedianIf" => TranslateIfCombinator("medianIf", arguments, typeof(double)),
            "StddevPopIf" => TranslateIfCombinator("stddevPopIf", arguments, typeof(double)),
            "StddevSampIf" => TranslateIfCombinator("stddevSampIf", arguments, typeof(double)),
            "VarPopIf" => TranslateIfCombinator("varPopIf", arguments, typeof(double)),
            "VarSampIf" => TranslateIfCombinator("varSampIf", arguments, typeof(double)),
            "UniqCombinedIf" => TranslateIfCombinator("uniqCombinedIf", arguments, typeof(ulong)),
            "UniqCombined64If" => TranslateIfCombinator("uniqCombined64If", arguments, typeof(ulong)),
            "UniqHLL12If" => TranslateIfCombinator("uniqHLL12If", arguments, typeof(ulong)),
            "UniqThetaIf" => TranslateIfCombinator("uniqThetaIf", arguments, typeof(ulong)),
            "QuantileTDigestIf" => TranslateParametricIfCombinator("quantileTDigestIf", arguments),
            "QuantileExactIf" => TranslateParametricIfCombinator("quantileExactIf", arguments),
            "QuantileTimingIf" => TranslateParametricIfCombinator("quantileTimingIf", arguments),
            "QuantileDDIf" => TranslateQuantileDDIf(arguments),
            "QuantilesIf" => TranslateMultiQuantileIf("quantilesIf", arguments),
            "QuantilesTDigestIf" => TranslateMultiQuantileIf("quantilesTDigestIf", arguments),

            _ => null
        };
    }

    private SqlExpression? TranslateSimpleAggregate(string functionName, SqlExpression? argument, Type returnType)
    {
        if (argument == null)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            functionName,
            [argument],
            nullable: true,
            argumentsPropagateNullability: [false],
            returnType);
    }

    private SqlExpression? TranslateTwoArgAggregate(string functionName, IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        if (arguments.Count < 2)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            functionName,
            [arguments[0], arguments[1]],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            returnType);
    }

    private SqlExpression? TranslateQuantile(IReadOnlyList<SqlExpression> arguments)
    {
        // Quantile(source, level, selector) - level is arguments[0], selector result is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var levelArg = arguments[0];
        var selectorArg = arguments[1];

        // Extract the level constant
        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        // Create quantile(level)(column) as a nested function call
        // ClickHouse parametric aggregate: quantile(0.95)(column)
        var innerFunction = _sqlExpressionFactory.Function(
            $"quantile({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})",
            [selectorArg],
            nullable: true,
            argumentsPropagateNullability: [false],
            typeof(double));

        return innerFunction;
    }

    private SqlExpression? TranslateGroupArray(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        if (arguments.Count == 1)
        {
            // Simple groupArray(column)
            return _sqlExpressionFactory.Function(
                "groupArray",
                [arguments[0]],
                nullable: true,
                argumentsPropagateNullability: [false],
                returnType);
        }
        else if (arguments.Count >= 2)
        {
            // groupArray(maxSize)(column) - maxSize is arguments[0], selector is arguments[1]
            var maxSizeArg = arguments[0];
            var selectorArg = arguments[1];

            if (maxSizeArg is SqlConstantExpression maxSizeConstant && maxSizeConstant.Value is int maxSize)
            {
                return _sqlExpressionFactory.Function(
                    $"groupArray({maxSize})",
                    [selectorArg],
                    nullable: true,
                    argumentsPropagateNullability: [false],
                    returnType);
            }
        }

        return null;
    }

    private SqlExpression? TranslateTopK(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        // TopK(source, k, selector) - k is arguments[0], selector result is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var kArg = arguments[0];
        var selectorArg = arguments[1];

        if (kArg is SqlConstantExpression kConstant && kConstant.Value is int k)
        {
            return _sqlExpressionFactory.Function(
                $"topK({k})",
                [selectorArg],
                nullable: true,
                argumentsPropagateNullability: [false],
                returnType);
        }

        return null;
    }

    /// <summary>
    /// Translates no-argument aggregate functions like countState().
    /// </summary>
    private SqlExpression? TranslateNoArgAggregate(string functionName, Type returnType)
    {
        return _sqlExpressionFactory.Function(
            functionName,
            Array.Empty<SqlExpression>(),
            nullable: true,
            argumentsPropagateNullability: Array.Empty<bool>(),
            returnType);
    }

    /// <summary>
    /// Translates quantileState(level)(column) - parametric aggregate state function.
    /// </summary>
    private SqlExpression? TranslateQuantileState(IReadOnlyList<SqlExpression> arguments)
    {
        // QuantileState(source, level, selector) - level is arguments[0], selector result is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var levelArg = arguments[0];
        var selectorArg = arguments[1];

        // Extract the level constant
        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        // Create quantileState(level)(column) as a nested function call
        return _sqlExpressionFactory.Function(
            $"quantileState({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})",
            [selectorArg],
            nullable: true,
            argumentsPropagateNullability: [false],
            typeof(byte[]));
    }

    /// <summary>
    /// Translates quantileMerge(level)(stateColumn) - merges quantile states.
    /// </summary>
    private SqlExpression? TranslateQuantileMerge(IReadOnlyList<SqlExpression> arguments)
    {
        // QuantileMerge(source, level, stateSelector) - level is arguments[0], selector result is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var levelArg = arguments[0];
        var selectorArg = arguments[1];

        // Extract the level constant
        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        // Create quantileMerge(level)(column) as a nested function call
        return _sqlExpressionFactory.Function(
            $"quantileMerge({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})",
            [selectorArg],
            nullable: true,
            argumentsPropagateNullability: [false],
            typeof(double));
    }

    /// <summary>
    /// Translates countIf(condition) - the CountIf combinator with just a predicate.
    /// </summary>
    private SqlExpression? TranslateCountIfCombinator(SqlExpression? predicate)
    {
        if (predicate == null)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            "countIf",
            [predicate],
            nullable: false,
            argumentsPropagateNullability: [false],
            typeof(long));
    }

    /// <summary>
    /// Translates If combinators like sumIf(column, condition), avgIf(column, condition), etc.
    /// When the selector is a defaultForNull column, the sentinel-exclusion is ANDed into the
    /// predicate; for sum/avg/min/max the function is also upgraded to its *OrNullIf variant so
    /// all-sentinel slices return NULL instead of 0 (matching the defaultForNull contract).
    /// </summary>
    private SqlExpression? TranslateIfCombinator(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        Type returnType)
    {
        // Pattern: SumIf(source, selector, predicate) - selector is arguments[0], predicate is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var selectorArg = arguments[0];
        var predicateArg = arguments[1];

        var sentinelCondition = TryCreateSentinelExclusionCondition(selectorArg);
        if (sentinelCondition != null)
        {
            predicateArg = _sqlExpressionFactory.AndAlso(predicateArg, sentinelCondition);
            functionName = UpgradeToOrNullIf(functionName);
        }

        return _sqlExpressionFactory.Function(
            functionName,
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            returnType);
    }

    /// <summary>
    /// Translates SumIf with defaultForNull sentinel composition and Int64 return-type coercion.
    /// Mirrors the Int64-wrap-in-Convert pattern from TranslateSum so SumIf&lt;T, decimal&gt;
    /// yields the user's declared return type rather than the raw ClickHouse Int64.
    /// </summary>
    private SqlExpression? TranslateSumIfCombinator(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        if (arguments.Count < 2)
        {
            return null;
        }

        var selectorArg = arguments[0];
        var predicateArg = arguments[1];

        var functionName = "sumIf";
        var sentinelCondition = TryCreateSentinelExclusionCondition(selectorArg);
        if (sentinelCondition != null)
        {
            predicateArg = _sqlExpressionFactory.AndAlso(predicateArg, sentinelCondition);
            functionName = "sumOrNullIf";
        }

        // ClickHouse sum returns Int64; wrap in Convert to match the .NET return type.
        var clickHouseReturnType = typeof(long?);
        var sumExpr = _sqlExpressionFactory.Function(
            functionName,
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            clickHouseReturnType);

        return _sqlExpressionFactory.Convert(sumExpr, returnType);
    }

    /// <summary>
    /// Upgrades an If combinator function name to its OrNullIf variant for aggregates that have one.
    /// Only sum/avg/min/max support OrNull; uniq/any/etc. keep their base name.
    /// </summary>
    private static string UpgradeToOrNullIf(string functionName) => functionName switch
    {
        "sumIf" => "sumOrNullIf",
        "avgIf" => "avgOrNullIf",
        "minIf" => "minOrNullIf",
        "maxIf" => "maxOrNullIf",
        _ => functionName
    };

    /// <summary>
    /// Translates two-selector If combinators like argMaxIf(arg, val, condition).
    /// </summary>
    private SqlExpression? TranslateTwoArgIfCombinator(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        Type returnType)
    {
        // Pattern: ArgMaxIf(source, argSelector, valSelector, predicate) -> [arg, val, pred]
        if (arguments.Count < 3)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            functionName,
            [arguments[0], arguments[1], arguments[2]],
            nullable: true,
            argumentsPropagateNullability: [false, false, false],
            returnType);
    }

    /// <summary>
    /// Translates parametric-level If combinators like quantileTDigestIf(level)(column, condition).
    /// </summary>
    private SqlExpression? TranslateParametricIfCombinator(
        string functionName,
        IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: QuantileXxxIf(source, level, selector, predicate) -> [selector, predicate] with level in name
        if (arguments.Count < 3)
        {
            return null;
        }

        var levelArg = arguments[0];
        var selectorArg = arguments[1];
        var predicateArg = arguments[2];

        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            $"{functionName}({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})",
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            typeof(double));
    }

    /// <summary>
    /// Translates quantileDDIf(relative_accuracy, level)(column, condition).
    /// </summary>
    private SqlExpression? TranslateQuantileDDIf(IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: QuantileDDIf(source, relativeAccuracy, level, selector, predicate)
        if (arguments.Count < 4)
        {
            return null;
        }

        var accuracyArg = arguments[0];
        var levelArg = arguments[1];
        var selectorArg = arguments[2];
        var predicateArg = arguments[3];

        if (accuracyArg is not SqlConstantExpression accuracyConstant || accuracyConstant.Value is not double accuracy)
        {
            return null;
        }

        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);

        return _sqlExpressionFactory.Function(
            $"quantileDDIf({accuracyStr}, {levelStr})",
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            typeof(double));
    }

    /// <summary>
    /// Translates multi-quantile If combinators like quantilesIf(l1, l2, ...)(column, condition).
    /// </summary>
    private SqlExpression? TranslateMultiQuantileIf(string functionName, IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: QuantilesIf(source, levels[], selector, predicate)
        if (arguments.Count < 3)
        {
            return null;
        }

        var levelsArg = arguments[0];
        var selectorArg = arguments[1];
        var predicateArg = arguments[2];

        if (levelsArg is not SqlConstantExpression levelsConstant || levelsConstant.Value is not double[] levels)
        {
            return null;
        }

        var levelsStr = string.Join(", ", levels.Select(l => l.ToString(System.Globalization.CultureInfo.InvariantCulture)));

        return _sqlExpressionFactory.Function(
            $"{functionName}({levelsStr})",
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            typeof(double[]));
    }

    /// <summary>
    /// Translates topKIf(k)(column, condition).
    /// </summary>
    private SqlExpression? TranslateTopKIf(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        // Pattern: TopKIf(source, k, selector, predicate)
        if (arguments.Count < 3)
        {
            return null;
        }

        var kArg = arguments[0];
        var selectorArg = arguments[1];
        var predicateArg = arguments[2];

        if (kArg is not SqlConstantExpression kConstant || kConstant.Value is not int k)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            $"topKIf({k})",
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            returnType);
    }

    /// <summary>
    /// Translates topKWeightedIf(k)(column, weight, condition).
    /// </summary>
    private SqlExpression? TranslateTopKWeightedIf(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        // Pattern: TopKWeightedIf(source, k, selector, weightSelector, predicate)
        if (arguments.Count < 4)
        {
            return null;
        }

        var kArg = arguments[0];
        var selectorArg = arguments[1];
        var weightArg = arguments[2];
        var predicateArg = arguments[3];

        if (kArg is not SqlConstantExpression kConstant || kConstant.Value is not int k)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            $"topKWeightedIf({k})",
            [selectorArg, weightArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false, false],
            returnType);
    }

    /// <summary>
    /// Translates groupArrayIf(column, condition) or groupArrayIf(maxSize)(column, condition).
    /// </summary>
    private SqlExpression? TranslateGroupArrayIf(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        // Overload 1: GroupArrayIf(source, selector, predicate) -> [selector, predicate]
        if (arguments.Count == 2)
        {
            return _sqlExpressionFactory.Function(
                "groupArrayIf",
                [arguments[0], arguments[1]],
                nullable: true,
                argumentsPropagateNullability: [false, false],
                returnType);
        }

        // Overload 2: GroupArrayIf(source, maxSize, selector, predicate)
        if (arguments.Count >= 3)
        {
            var maxSizeArg = arguments[0];
            var selectorArg = arguments[1];
            var predicateArg = arguments[2];

            if (maxSizeArg is SqlConstantExpression maxSizeConstant && maxSizeConstant.Value is int maxSize)
            {
                return _sqlExpressionFactory.Function(
                    $"groupArrayIf({maxSize})",
                    [selectorArg, predicateArg],
                    nullable: true,
                    argumentsPropagateNullability: [false, false],
                    returnType);
            }
        }

        return null;
    }

    /// <summary>
    /// Translates groupUniqArrayIf(column, condition) or groupUniqArrayIf(maxSize)(column, condition).
    /// </summary>
    private SqlExpression? TranslateGroupUniqArrayIf(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        if (arguments.Count == 2)
        {
            return _sqlExpressionFactory.Function(
                "groupUniqArrayIf",
                [arguments[0], arguments[1]],
                nullable: true,
                argumentsPropagateNullability: [false, false],
                returnType);
        }

        if (arguments.Count >= 3)
        {
            var maxSizeArg = arguments[0];
            var selectorArg = arguments[1];
            var predicateArg = arguments[2];

            if (maxSizeArg is SqlConstantExpression maxSizeConstant && maxSizeConstant.Value is int maxSize)
            {
                return _sqlExpressionFactory.Function(
                    $"groupUniqArrayIf({maxSize})",
                    [selectorArg, predicateArg],
                    nullable: true,
                    argumentsPropagateNullability: [false, false],
                    returnType);
            }
        }

        return null;
    }

    /// <summary>
    /// Translates quantileDD(relative_accuracy, level)(column) - two parametric args.
    /// </summary>
    private SqlExpression? TranslateQuantileDD(IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: QuantileDD(source, relativeAccuracy, level, selector)
        // - relativeAccuracy is arguments[0], level is arguments[1], selector result is arguments[2]
        if (arguments.Count < 3)
        {
            return null;
        }

        var accuracyArg = arguments[0];
        var levelArg = arguments[1];
        var selectorArg = arguments[2];

        if (accuracyArg is not SqlConstantExpression accuracyConstant || accuracyConstant.Value is not double accuracy)
        {
            return null;
        }

        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        var accuracyStr = accuracy.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var levelStr = level.ToString(System.Globalization.CultureInfo.InvariantCulture);

        return _sqlExpressionFactory.Function(
            $"quantileDD({accuracyStr}, {levelStr})",
            [selectorArg],
            nullable: true,
            argumentsPropagateNullability: [false],
            typeof(double));
    }

    /// <summary>
    /// Translates parametric quantile variants like quantileTDigest(level)(column).
    /// </summary>
    private SqlExpression? TranslateParametricQuantile(string functionName, IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: QuantileXxx(source, level, selector) - level is arguments[0], selector result is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var levelArg = arguments[0];
        var selectorArg = arguments[1];

        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            $"{functionName}({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})",
            [selectorArg],
            nullable: true,
            argumentsPropagateNullability: [false],
            typeof(double));
    }

    /// <summary>
    /// Translates multi-quantile functions like quantiles(0.5, 0.9, 0.99)(column).
    /// </summary>
    private SqlExpression? TranslateMultiQuantile(string functionName, IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: Quantiles(source, levels[], selector) - levels is arguments[0], selector result is arguments[1]
        if (arguments.Count < 2)
        {
            return null;
        }

        var levelsArg = arguments[0];
        var selectorArg = arguments[1];

        if (levelsArg is not SqlConstantExpression levelsConstant || levelsConstant.Value is not double[] levels)
        {
            return null;
        }

        var levelsStr = string.Join(", ", levels.Select(l => l.ToString(System.Globalization.CultureInfo.InvariantCulture)));

        return _sqlExpressionFactory.Function(
            $"{functionName}({levelsStr})",
            [selectorArg],
            nullable: true,
            argumentsPropagateNullability: [false],
            typeof(double[]));
    }

    /// <summary>
    /// Translates topKWeighted(k)(column, weight) - parametric with two arguments.
    /// </summary>
    private SqlExpression? TranslateTopKWeighted(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        // Pattern: TopKWeighted(source, k, selector, weightSelector) - k is arguments[0], selector is arguments[1], weight is arguments[2]
        if (arguments.Count < 3)
        {
            return null;
        }

        var kArg = arguments[0];
        var selectorArg = arguments[1];
        var weightArg = arguments[2];

        if (kArg is not SqlConstantExpression kConstant || kConstant.Value is not int k)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            $"topKWeighted({k})",
            [selectorArg, weightArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            returnType);
    }

    /// <summary>
    /// Translates quantileIf(level)(column, condition) - parametric quantile with If combinator.
    /// </summary>
    private SqlExpression? TranslateQuantileIf(IReadOnlyList<SqlExpression> arguments)
    {
        // Pattern: QuantileIf(source, level, selector, predicate) - level is arguments[0], selector is arguments[1], predicate is arguments[2]
        if (arguments.Count < 3)
        {
            return null;
        }

        var levelArg = arguments[0];
        var selectorArg = arguments[1];
        var predicateArg = arguments[2];

        if (levelArg is not SqlConstantExpression levelConstant || levelConstant.Value is not double level)
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            $"quantileIf({level.ToString(System.Globalization.CultureInfo.InvariantCulture)})",
            [selectorArg, predicateArg],
            nullable: true,
            argumentsPropagateNullability: [false, false],
            typeof(double));
    }
}

/// <summary>
/// Provides aggregate method call translator for ClickHouse.
/// </summary>
public class ClickHouseAggregateMethodCallTranslatorProvider : IAggregateMethodCallTranslatorProvider
{
    private readonly ClickHouseAggregateMethodCallTranslator _translator;

    public ClickHouseAggregateMethodCallTranslatorProvider(
        ISqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _translator = new ClickHouseAggregateMethodCallTranslator(
            (ClickHouseSqlExpressionFactory)sqlExpressionFactory,
            typeMappingSource);
    }

    public SqlExpression? Translate(
        IModel model,
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        return _translator.Translate(model, method, source, arguments, logger);
    }
}
