using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates LINQ aggregate methods to ClickHouse SQL aggregate functions.
/// Uses *OrNull variants per design decision for null-safe behavior.
/// </summary>
public class ClickHouseAggregateMethodCallTranslator : IAggregateMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseAggregateMethodCallTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Only handle Queryable and Enumerable extension methods
        if (method.DeclaringType != typeof(Queryable) &&
            method.DeclaringType != typeof(Enumerable))
        {
            return null;
        }

        var methodName = method.Name;

        // Get the selector expression if present
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
        if (argument.Type == typeof(int) || argument.Type == typeof(long) ||
            argument.Type == typeof(short) || argument.Type == typeof(byte))
        {
            argument = _sqlExpressionFactory.Convert(argument, typeof(double));
        }

        // Use avgOrNull for null-safe behavior (returns NULL for empty set)
        return _sqlExpressionFactory.Function(
            "avgOrNull",
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
            returnType);
    }

    /// <summary>
    /// Translates Sum to sumOrNull for null-safe behavior.
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

        // Use sumOrNull for null-safe behavior (returns NULL for empty set)
        return _sqlExpressionFactory.Function(
            "sumOrNull",
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
            returnType);
    }

    /// <summary>
    /// Translates Min to minOrNull for null-safe behavior.
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

        return _sqlExpressionFactory.Function(
            "minOrNull",
            new[] { argument },
            nullable: true,
            argumentsPropagateNullability: new[] { false },
            returnType);
    }

    /// <summary>
    /// Translates Max to maxOrNull for null-safe behavior.
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
    /// </summary>
    private SqlExpression? GetAggregateArgument(EnumerableExpression source, SqlExpression? selector)
    {
        // If there's a selector, use it; otherwise use the source
        if (selector is not null)
        {
            return selector;
        }

        // For simple aggregates like Count(), we might not have a selector
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
        ISqlExpressionFactory sqlExpressionFactory)
    {
        _translator = new ClickHouseAggregateMethodCallTranslator(
            (ClickHouseSqlExpressionFactory)sqlExpressionFactory);
    }

    public SqlExpression? Translate(
        IModel model,
        MethodInfo method,
        EnumerableExpression source,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        return _translator.Translate(method, source, arguments, logger);
    }
}
