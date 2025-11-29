using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Factory for creating ClickHouse queryable method translating expression visitors.
/// </summary>
public class ClickHouseQueryableMethodTranslatingExpressionVisitorFactory
    : IQueryableMethodTranslatingExpressionVisitorFactory
{
    private readonly QueryableMethodTranslatingExpressionVisitorDependencies _dependencies;
    private readonly RelationalQueryableMethodTranslatingExpressionVisitorDependencies _relationalDependencies;

    public ClickHouseQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new ClickHouseQueryableMethodTranslatingExpressionVisitor(
            _dependencies,
            _relationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext);
}

/// <summary>
/// Translates LINQ queryable methods to ClickHouse SQL expressions.
/// </summary>
public class ClickHouseQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly ClickHouseQueryCompilationContextOptions _options;

    public ClickHouseQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _options = queryCompilationContext.QueryCompilationContextOptions();
    }

    /// <summary>
    /// Handles ClickHouse-specific extension methods like Final() and Sample().
    /// </summary>
    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
    {
        var method = methodCallExpression.Method;

        // Handle Final() extension method
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == ClickHouseQueryableExtensions.FinalMethodInfo)
        {
            return TranslateFinal(methodCallExpression);
        }

        // Handle Sample() extension methods
        if (method.IsGenericMethod)
        {
            var genericDef = method.GetGenericMethodDefinition();
            if (genericDef == ClickHouseQueryableExtensions.SampleMethodInfo)
            {
                return TranslateSample(methodCallExpression, hasOffset: false);
            }
            if (genericDef == ClickHouseQueryableExtensions.SampleWithOffsetMethodInfo)
            {
                return TranslateSample(methodCallExpression, hasOffset: true);
            }
        }

        return base.VisitMethodCall(methodCallExpression);
    }

    /// <summary>
    /// Translates the Final() extension method.
    /// Stores FINAL flag in the query options to be applied during SQL generation.
    /// </summary>
    private Expression TranslateFinal(MethodCallExpression methodCallExpression)
    {
        // Visit the source to get the translated query
        var source = Visit(methodCallExpression.Arguments[0]);

        // Mark that FINAL should be applied - this will be read by the SQL generator
        _options.UseFinal = true;

        return source;
    }

    /// <summary>
    /// Translates the Sample() extension method.
    /// Stores SAMPLE parameters in the query options to be applied during SQL generation.
    /// </summary>
    private Expression TranslateSample(MethodCallExpression methodCallExpression, bool hasOffset)
    {
        // Visit the source to get the translated query
        var source = Visit(methodCallExpression.Arguments[0]);

        // Get the fraction value
        var fractionArg = methodCallExpression.Arguments[1];
        if (fractionArg is not ConstantExpression fractionConst || fractionConst.Value is not double fraction)
        {
            throw new InvalidOperationException("Sample fraction must be a constant value.");
        }

        _options.SampleFraction = fraction;

        if (hasOffset)
        {
            var offsetArg = methodCallExpression.Arguments[2];
            if (offsetArg is not ConstantExpression offsetConst || offsetConst.Value is not double offsetValue)
            {
                throw new InvalidOperationException("Sample offset must be a constant value.");
            }
            _options.SampleOffset = offsetValue;
        }

        return source;
    }
}

/// <summary>
/// Stores ClickHouse-specific query options that are set during translation
/// and read during SQL generation.
/// </summary>
public class ClickHouseQueryCompilationContextOptions
{
    /// <summary>
    /// Whether to add FINAL modifier to the query.
    /// </summary>
    public bool UseFinal { get; set; }

    /// <summary>
    /// The SAMPLE fraction (0.0 to 1.0), or null if no sampling.
    /// </summary>
    public double? SampleFraction { get; set; }

    /// <summary>
    /// The SAMPLE offset for reproducible sampling.
    /// </summary>
    public double? SampleOffset { get; set; }
}

/// <summary>
/// Extension methods for getting ClickHouse options from query compilation context.
/// </summary>
public static class QueryCompilationContextExtensions
{
    private static readonly object _optionsKey = new();

    /// <summary>
    /// Gets or creates the ClickHouse query options for this compilation context.
    /// </summary>
    public static ClickHouseQueryCompilationContextOptions QueryCompilationContextOptions(
        this RelationalQueryCompilationContext context)
    {
        // Store options in the context's data dictionary if available
        // Otherwise use a static dictionary keyed by context
        if (!OptionsStore.TryGetValue(context, out var options))
        {
            options = new ClickHouseQueryCompilationContextOptions();
            OptionsStore[context] = options;
        }
        return options;
    }

    // Simple store - in production would want WeakReference or proper lifecycle management
    private static readonly Dictionary<RelationalQueryCompilationContext, ClickHouseQueryCompilationContextOptions>
        OptionsStore = new();
}
