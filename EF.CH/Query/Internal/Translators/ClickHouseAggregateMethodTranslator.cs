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
                new[] { convertedArgument, defaultForNullCondition },
                nullable: true,
                argumentsPropagateNullability: new[] { false, false },
                returnType);
        }

        // Use avgOrNull for null-safe behavior (returns NULL for empty set)
        return _sqlExpressionFactory.Function(
            "avgOrNull",
            new[] { convertedArgument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
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
                new[] { argument, defaultForNullCondition },
                nullable: true,
                argumentsPropagateNullability: new[] { false, false },
                clickHouseReturnType);
        }
        else
        {
            // Use sumOrNull for null-safe behavior (returns NULL for empty set)
            sumExpr = _sqlExpressionFactory.Function(
                "sumOrNull",
                new[] { argument },
                nullable: true,
                argumentsPropagateNullability: new[] { false },
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
                new[] { argument, defaultForNullCondition },
                nullable: true,
                argumentsPropagateNullability: new[] { false, false },
                returnType);
        }

        return _sqlExpressionFactory.Function(
            "minOrNull",
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
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
                new[] { argument, defaultForNullCondition },
                nullable: true,
                argumentsPropagateNullability: new[] { false, false },
                returnType);
        }

        return _sqlExpressionFactory.Function(
            "maxOrNull",
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
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
                new[] { predicate },
                nullable: false,
                argumentsPropagateNullability: new[] { false },
                typeof(ulong));
        }

        // Wrap in toInt32() for .NET Int32 compatibility
        return _sqlExpressionFactory.Function(
            "toInt32",
            new[] { countExpr },
            nullable: false,
            argumentsPropagateNullability: new[] { false },
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
                new[] { predicate },
                nullable: false,
                argumentsPropagateNullability: new[] { false },
                typeof(ulong));
        }

        // Wrap in toInt64() for .NET Int64 compatibility
        return _sqlExpressionFactory.Function(
            "toInt64",
            new[] { countExpr },
            nullable: false,
            argumentsPropagateNullability: new[] { false },
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

            // Phase 3 - Array aggregates
            "GroupArray" => TranslateGroupArray(arguments, returnType),
            "GroupUniqArray" => TranslateSimpleAggregate("groupUniqArray", argument, returnType),
            "TopK" => TranslateTopK(arguments, returnType),

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
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
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
            new[] { arguments[0], arguments[1] },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false },
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
            new[] { selectorArg },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
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
                new[] { arguments[0] },
                nullable: true,
                argumentsPropagateNullability: new[] { false },
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
                    new[] { selectorArg },
                    nullable: true,
                    argumentsPropagateNullability: new[] { false },
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
                new[] { selectorArg },
                nullable: true,
                argumentsPropagateNullability: new[] { false },
                returnType);
        }

        return null;
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
