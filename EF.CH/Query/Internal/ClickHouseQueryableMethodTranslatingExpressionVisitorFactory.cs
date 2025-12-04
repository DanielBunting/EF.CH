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
    /// Constructor for subquery visitors. Shares state with the parent visitor.
    /// </summary>
    protected ClickHouseQueryableMethodTranslatingExpressionVisitor(
        ClickHouseQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor)
    {
        _options = parentVisitor._options;
    }

    /// <summary>
    /// Creates a visitor for translating subqueries. Returns our custom type to ensure
    /// EnumerableExpression handling works correctly in subqueries.
    /// </summary>
    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new ClickHouseQueryableMethodTranslatingExpressionVisitor(this);

    /// <summary>
    /// Handles extension expressions, including EnumerableExpression which cannot have VisitChildren called on it in EF Core 10+.
    /// </summary>
    protected override Expression VisitExtension(Expression extensionExpression)
    {
        // EnumerableExpression.VisitChildren throws "VisitIsNotAllowed" in EF Core 10+
        // Return unchanged - the aggregate translator will handle it properly
        if (extensionExpression is EnumerableExpression)
        {
            return extensionExpression;
        }

        return base.VisitExtension(extensionExpression);
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
            if (genericDef == ClickHouseQueryableExtensions.WithSettingsMethodInfo)
            {
                return TranslateWithSettings(methodCallExpression);
            }
            if (genericDef == ClickHouseQueryableExtensions.WithSettingMethodInfo)
            {
                return TranslateWithSetting(methodCallExpression);
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

        // Get the fraction value - it may be wrapped in EF.Constant() to prevent parameterization
        var fractionArg = methodCallExpression.Arguments[1];
        if (!TryGetConstantValue<double>(fractionArg, out var fraction))
        {
            throw new InvalidOperationException(
                $"Sample fraction must be a constant value. Got expression type: {fractionArg.GetType().Name}, NodeType: {fractionArg.NodeType}");
        }

        _options.SampleFraction = fraction;

        if (hasOffset)
        {
            var offsetArg = methodCallExpression.Arguments[2];
            if (!TryGetConstantValue<double>(offsetArg, out var offsetValue))
            {
                throw new InvalidOperationException(
                    $"Sample offset must be a constant value. Got expression type: {offsetArg.GetType().Name}");
            }
            _options.SampleOffset = offsetValue;
        }

        return source;
    }

    /// <summary>
    /// Translates the WithSettings() extension method.
    /// Stores settings in the query options to be applied during SQL generation.
    /// </summary>
    private Expression TranslateWithSettings(MethodCallExpression methodCallExpression)
    {
        var source = Visit(methodCallExpression.Arguments[0]);

        var settingsArg = methodCallExpression.Arguments[1];
        if (!TryGetConstantValue<IDictionary<string, object>>(settingsArg, out var settings))
        {
            throw new InvalidOperationException("WithSettings argument must be a constant dictionary.");
        }

        foreach (var kvp in settings)
        {
            _options.QuerySettings[kvp.Key] = kvp.Value;
        }

        return source;
    }

    /// <summary>
    /// Translates the WithSetting() extension method.
    /// Stores the setting in the query options to be applied during SQL generation.
    /// </summary>
    private Expression TranslateWithSetting(MethodCallExpression methodCallExpression)
    {
        var source = Visit(methodCallExpression.Arguments[0]);

        var nameArg = methodCallExpression.Arguments[1];
        var valueArg = methodCallExpression.Arguments[2];

        if (!TryGetConstantValue<string>(nameArg, out var name))
        {
            throw new InvalidOperationException(
                $"WithSetting name must be a constant string. Got expression type: {nameArg.GetType().Name}, NodeType: {nameArg.NodeType}");
        }

        if (!TryGetConstantValue<object>(valueArg, out var value))
        {
            throw new InvalidOperationException(
                $"WithSetting value must be a constant. Got expression type: {valueArg.GetType().Name}, NodeType: {valueArg.NodeType}");
        }

        _options.QuerySettings[name] = value;

        return source;
    }

    /// <summary>
    /// Attempts to extract a constant value from an expression by evaluating it.
    /// Handles various expression tree structures that EF Core may create.
    /// </summary>
    private static bool TryGetConstantValue<T>(Expression expression, out T value)
    {
        value = default!;

        // Unwrap Quote expressions (used for lambda expressions)
        while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
        {
            expression = unary.Operand;
        }

        // Direct constant
        if (expression is ConstantExpression constant)
        {
            return TryConvertValue(constant.Value, out value);
        }

        // Convert expression wrapping a constant
        if (expression is UnaryExpression convertExpr &&
            (convertExpr.NodeType == ExpressionType.Convert || convertExpr.NodeType == ExpressionType.ConvertChecked))
        {
            return TryGetConstantValue(convertExpr.Operand, out value);
        }

        // Handle EF.Constant() wrapper - extract the inner value
        if (expression is MethodCallExpression methodCall &&
            methodCall.Method.DeclaringType?.FullName == "Microsoft.EntityFrameworkCore.EF" &&
            methodCall.Method.Name == "Constant" &&
            methodCall.Arguments.Count == 1)
        {
            return TryGetConstantValue(methodCall.Arguments[0], out value);
        }

        // Member access on a constant (closure capture)
        if (expression is MemberExpression memberExpr)
        {
            return TryEvaluateMemberExpression(memberExpr, out value);
        }

        // Try to compile and execute the expression
        return TryCompileAndExecute(expression, out value);
    }

    private static bool TryConvertValue<T>(object? obj, out T value)
    {
        value = default!;
        if (obj == null) return typeof(T).IsClass || Nullable.GetUnderlyingType(typeof(T)) != null;

        if (obj is T typed)
        {
            value = typed;
            return true;
        }

        try
        {
            value = (T)Convert.ChangeType(obj, typeof(T));
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryEvaluateMemberExpression<T>(MemberExpression memberExpr, out T value)
    {
        value = default!;

        // Walk up to find the root constant
        object? target = null;
        if (memberExpr.Expression is ConstantExpression constantExpr)
        {
            target = constantExpr.Value;
        }
        else if (memberExpr.Expression is MemberExpression nestedMember)
        {
            if (!TryEvaluateMemberExpression(nestedMember, out object? nestedTarget))
                return false;
            target = nestedTarget;
        }
        else if (memberExpr.Expression == null)
        {
            // Static member
            target = null;
        }
        else
        {
            return TryCompileAndExecute(memberExpr, out value);
        }

        try
        {
            object? result = memberExpr.Member switch
            {
                System.Reflection.FieldInfo field => field.GetValue(target),
                System.Reflection.PropertyInfo prop => prop.GetValue(target),
                _ => throw new InvalidOperationException()
            };

            return TryConvertValue(result, out value);
        }
        catch
        {
            return false;
        }
    }

    private static bool TryCompileAndExecute<T>(Expression expression, out T value)
    {
        value = default!;
        try
        {
            var lambda = Expression.Lambda<Func<object>>(
                Expression.Convert(expression, typeof(object)));
            var compiled = lambda.Compile();
            var result = compiled();
            return TryConvertValue(result, out value);
        }
        catch
        {
            return false;
        }
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

    /// <summary>
    /// ClickHouse query settings to append as SETTINGS clause.
    /// </summary>
    public Dictionary<string, object> QuerySettings { get; } = new();
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
