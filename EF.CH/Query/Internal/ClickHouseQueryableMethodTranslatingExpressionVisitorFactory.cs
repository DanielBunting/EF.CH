using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Query.Internal.WithFill;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// GROUP BY modifier types for ClickHouse.
/// </summary>
public enum GroupByModifier
{
    None = 0,
    Rollup,
    Cube,
    Totals
}

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

            // Handle unified Interpolate extension methods
            if (InterpolateExtensions.AllMethodInfos.Contains(genericDef))
            {
                return TranslateInterpolate(methodCallExpression);
            }

            // Handle PreWhere extension method
            if (genericDef == ClickHouseQueryableExtensions.PreWhereMethodInfo)
            {
                return TranslatePreWhere(methodCallExpression);
            }

            // Handle LimitBy extension methods
            if (genericDef == ClickHouseQueryableExtensions.LimitByMethodInfo)
            {
                return TranslateLimitBy(methodCallExpression, hasOffset: false);
            }
            if (genericDef == ClickHouseQueryableExtensions.LimitByWithOffsetMethodInfo)
            {
                return TranslateLimitBy(methodCallExpression, hasOffset: true);
            }

            // Handle GROUP BY modifiers
            if (genericDef == ClickHouseQueryableExtensions.WithRollupMethodInfo)
            {
                return TranslateGroupByModifier(methodCallExpression, GroupByModifier.Rollup);
            }
            if (genericDef == ClickHouseQueryableExtensions.WithCubeMethodInfo)
            {
                return TranslateGroupByModifier(methodCallExpression, GroupByModifier.Cube);
            }
            if (genericDef == ClickHouseQueryableExtensions.WithTotalsMethodInfo)
            {
                return TranslateGroupByModifier(methodCallExpression, GroupByModifier.Totals);
            }

            // Handle AsCte extension method
            if (genericDef == ClickHouseQueryableExtensions.AsCteMethodInfo)
            {
                return TranslateAsCte(methodCallExpression);
            }

            // Handle WithRawFilter extension method
            if (genericDef == ClickHouseQueryableExtensions.WithRawFilterMethodInfo)
            {
                return TranslateWithRawFilter(methodCallExpression);
            }

            // Note: ArrayJoin and AsofJoin are rewritten by ClickHouseQueryTranslationPreprocessor
            // before they reach this visitor, so they don't need handling here.
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

        // Get the fraction value - may be a constant or deferred parameter (EF Core 9+)
        var fractionVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[1])
            ?? throw new InvalidOperationException(
                $"Sample fraction must be a constant value. Got expression type: {methodCallExpression.Arguments[1].GetType().Name}, NodeType: {methodCallExpression.Arguments[1].NodeType}");

        _options.SampleFraction = fractionVal;

        if (hasOffset)
        {
            var offsetVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[2])
                ?? throw new InvalidOperationException(
                    $"Sample offset must be a constant value. Got expression type: {methodCallExpression.Arguments[2].GetType().Name}");
            _options.SampleOffset = offsetVal;
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

        var settingsVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[1]);
        if (settingsVal is IDictionary<string, object> settings)
        {
            foreach (var kvp in settings)
            {
                _options.QuerySettings[kvp.Key] = kvp.Value;
            }
        }
        else if (settingsVal is DeferredParameter deferred)
        {
            // Store the deferred parameter - will be resolved when parameter values are available
            _options.DeferredSettings = deferred;
        }
        else
        {
            throw new InvalidOperationException("WithSettings argument must be a constant dictionary.");
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

        var nameVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[1])
            ?? throw new InvalidOperationException(
                $"WithSetting name must be a constant string. Got expression type: {methodCallExpression.Arguments[1].GetType().Name}, NodeType: {methodCallExpression.Arguments[1].NodeType}");

        var valueVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[2])
            ?? throw new InvalidOperationException(
                $"WithSetting value must be a constant. Got expression type: {methodCallExpression.Arguments[2].GetType().Name}, NodeType: {methodCallExpression.Arguments[2].NodeType}");

        if (nameVal is string name)
        {
            // Direct constant name - store value (which may itself be deferred)
            _options.QuerySettings[name] = valueVal;
        }
        else if (nameVal is DeferredParameter)
        {
            // Both name and value are deferred - store as a pending setting pair
            _options.DeferredSettingPairs.Add((nameVal, valueVal));
        }
        else
        {
            throw new InvalidOperationException(
                $"WithSetting name must be a constant string. Got: {nameVal.GetType().Name}");
        }

        return source;
    }

    /// <summary>
    /// Translates the unified Interpolate() extension method.
    /// Extracts fill column, step, optional from/to, and column interpolations.
    /// </summary>
    private Expression TranslateInterpolate(MethodCallExpression methodCallExpression)
    {
        var source = Visit(methodCallExpression.Arguments[0]);

        // Extract fill column selector (argument 1)
        var fillArg = methodCallExpression.Arguments[1];
        var fillSelector = UnwrapLambda(fillArg);
        var fillColumnName = ExtractMemberName(fillSelector);

        // Extract step (argument 2) - may be constant or deferred parameter
        var step = GetConstantOrDeferredValue(methodCallExpression.Arguments[2])
            ?? throw new InvalidOperationException("Interpolate step must be a constant value.");

        object? from = null;
        object? to = null;

        var genericDef = methodCallExpression.Method.GetGenericMethodDefinition();
        var paramCount = methodCallExpression.Arguments.Count;
        var genericArgCount = methodCallExpression.Method.GetGenericArguments().Length;

        // Determine which overload we're dealing with
        // Basic: 3 args (source, fill, step)
        // FromTo: 5 args with 2 generic args (source, fill, step, from, to)
        // ColumnMode: 5 args with 3 generic args, last is InterpolateMode
        // ColumnConstant: 5 args with 3 generic args, last is TValue
        // Builder: 4 args with 2 generic args, last is Action<InterpolateBuilder<T>>

        var isFromTo = paramCount == 5 && genericArgCount == 2;
        // For ColumnMode detection, check both ConstantExpression and ParameterExpression cases
        var isColumnMode = paramCount == 5 && genericArgCount == 3 &&
            (methodCallExpression.Arguments[4] is ConstantExpression ce && ce.Type == typeof(InterpolateMode)
             || IsInterpolateModeArgument(methodCallExpression.Arguments[4]));
        var isColumnConstant = paramCount == 5 && genericArgCount == 3 && !isColumnMode;
        var isBuilder = paramCount == 4 && genericArgCount == 2;

        if (isFromTo)
        {
            from = GetConstantOrDeferredValue(methodCallExpression.Arguments[3]);
            to = GetConstantOrDeferredValue(methodCallExpression.Arguments[4]);
        }

        // Store the fill spec
        _options.WithFillSpecs[fillColumnName] = new WithFillColumnSpec
        {
            ColumnName = fillColumnName,
            Step = step,
            From = from,
            To = to
        };

        // Handle column interpolation if present
        if (isColumnMode)
        {
            var columnArg = methodCallExpression.Arguments[3];
            var columnSelector = UnwrapLambda(columnArg);
            var columnName = ExtractMemberName(columnSelector);

            var modeVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[4])
                ?? throw new InvalidOperationException("Interpolate mode must be a constant value.");

            _options.InterpolateSpecs.Add(new InterpolateColumnSpec
            {
                ColumnName = columnName,
                Mode = modeVal is InterpolateMode m ? m : InterpolateMode.Default,
                ModeValue = modeVal
            });
        }
        else if (isColumnConstant)
        {
            var columnArg = methodCallExpression.Arguments[3];
            var columnSelector = UnwrapLambda(columnArg);
            var columnName = ExtractMemberName(columnSelector);

            var constantValue = GetConstantOrDeferredValue(methodCallExpression.Arguments[4]);

            _options.InterpolateSpecs.Add(new InterpolateColumnSpec
            {
                ColumnName = columnName,
                IsConstant = true,
                ConstantValue = constantValue
            });
        }
        else if (isBuilder)
        {
            // Extract the builder - try GetConstantOrDeferredValue first.
            // If EF Core 9+ auto-parameterized it, defer builder processing to ResolveDeferredParameters.
            var builderVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[3]);
            if (builderVal is DeferredParameter deferredBuilder)
            {
                // Store for deferred resolution when parameter values become available
                _options.DeferredInterpolateBuilder = deferredBuilder;
                return source;
            }

            if (builderVal == null)
            {
                throw new InvalidOperationException("Interpolate builder must be a constant value.");
            }

            // Use reflection to get the Columns property from InterpolateBuilder<T>
            var builderType = builderVal.GetType();
            var columnsProperty = builderType.GetProperty("Columns",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

            if (columnsProperty?.GetValue(builderVal) is System.Collections.IEnumerable columns)
            {
                foreach (var col in columns)
                {
                    // col is InterpolateBuilder<T>.InterpolateColumn record
                    var colType = col.GetType();
                    var columnProp = colType.GetProperty("Column");
                    var modeProp = colType.GetProperty("Mode");
                    var constantProp = colType.GetProperty("Constant");
                    var isConstantProp = colType.GetProperty("IsConstant");

                    var columnExpr = columnProp?.GetValue(col) as LambdaExpression;
                    var mode = modeProp?.GetValue(col);
                    var constant = constantProp?.GetValue(col);
                    var isConstant = (bool)(isConstantProp?.GetValue(col) ?? false);

                    if (columnExpr != null)
                    {
                        var columnName = ExtractMemberName(columnExpr);
                        _options.InterpolateSpecs.Add(new InterpolateColumnSpec
                        {
                            ColumnName = columnName,
                            Mode = isConstant ? InterpolateMode.Default : (InterpolateMode)(mode ?? InterpolateMode.Default),
                            IsConstant = isConstant,
                            ConstantValue = constant
                        });
                    }
                }
            }
        }

        return source;
    }

    /// <summary>
    /// Translates the PreWhere() extension method.
    /// Uses the same predicate translation as Where() but stores separately for PREWHERE clause generation.
    /// </summary>
    private Expression TranslatePreWhere(MethodCallExpression methodCallExpression)
    {
        // Visit the source to get the translated query
        var source = Visit(methodCallExpression.Arguments[0]);

        if (source is not ShapedQueryExpression shapedQuery)
        {
            throw new InvalidOperationException("PreWhere can only be applied to a query source.");
        }

        // Get the predicate lambda
        var predicateLambda = UnwrapLambda(methodCallExpression.Arguments[1]);

        // Translate the predicate using EF Core's infrastructure
        var translatedPredicate = TranslateLambdaExpression(shapedQuery, predicateLambda);

        if (translatedPredicate == null)
        {
            throw new InvalidOperationException(
                "Could not translate PREWHERE predicate to SQL. " +
                "Ensure all expressions are server-evaluable.");
        }

        // Store for SQL generation - only one PreWhere call per query
        if (_options.PreWhereExpression != null)
        {
            throw new InvalidOperationException(
                "PreWhere can only be called once per query. Combine conditions in a single PreWhere call.");
        }

        _options.PreWhereExpression = translatedPredicate;

        return source;
    }

    /// <summary>
    /// Translates the AsCte() extension method.
    /// Stores the CTE name in options; actual extraction happens in postprocessor.
    /// </summary>
    private Expression TranslateAsCte(MethodCallExpression methodCallExpression)
    {
        var source = Visit(methodCallExpression.Arguments[0]);

        var nameVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[1])
            ?? throw new InvalidOperationException("AsCte name must be a constant string.");

        if (nameVal is string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("CTE name cannot be empty.", nameof(name));
            }
        }

        if (_options.PendingCteName != null)
        {
            throw new InvalidOperationException(
                $"Cannot define multiple CTEs in a single query. Already defined CTE '{_options.PendingCteName}'.");
        }

        _options.PendingCteName = nameVal;

        return source;
    }

    /// <summary>
    /// Translates the WithRawFilter() extension method.
    /// Stores the raw SQL string in options for injection into the WHERE clause.
    /// </summary>
    private Expression TranslateWithRawFilter(MethodCallExpression methodCallExpression)
    {
        var source = Visit(methodCallExpression.Arguments[0]);

        var sqlVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[1])
            ?? throw new InvalidOperationException("WithRawFilter argument must be a constant string.");

        if (sqlVal is string rawSql)
        {
            if (string.IsNullOrWhiteSpace(rawSql))
            {
                throw new ArgumentException("WithRawFilter SQL condition cannot be empty.");
            }
        }

        _options.RawFilterSql = sqlVal;

        return source;
    }

    /// <summary>
    /// Translates GROUP BY modifier extension methods (WithRollup, WithCube, WithTotals).
    /// </summary>
    private Expression TranslateGroupByModifier(
        MethodCallExpression methodCallExpression,
        GroupByModifier modifier)
    {
        var source = Visit(methodCallExpression.Arguments[0]);

        if (_options.GroupByModifier != GroupByModifier.None)
        {
            throw new InvalidOperationException(
                $"Cannot combine multiple GROUP BY modifiers. Already using {_options.GroupByModifier}.");
        }

        _options.GroupByModifier = modifier;

        return source;
    }

    /// <summary>
    /// Translates the LimitBy() extension method.
    /// Extracts limit, optional offset, and key selector columns for LIMIT BY clause.
    /// </summary>
    private Expression TranslateLimitBy(MethodCallExpression methodCallExpression, bool hasOffset)
    {
        // Visit the source to get the translated query
        var source = Visit(methodCallExpression.Arguments[0]);

        if (source is not ShapedQueryExpression shapedQuery)
        {
            throw new InvalidOperationException("LimitBy can only be applied to a query source.");
        }

        int argIndex = 1;

        // Extract offset if present
        if (hasOffset)
        {
            var offsetVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[argIndex])
                ?? throw new InvalidOperationException("LimitBy offset must be a constant value.");
            _options.LimitByOffset = offsetVal;
            argIndex++;
        }

        // Extract limit
        var limitVal = GetConstantOrDeferredValue(methodCallExpression.Arguments[argIndex])
            ?? throw new InvalidOperationException("LimitBy limit must be a constant value.");
        _options.LimitByLimit = limitVal;
        argIndex++;

        // Extract key selector lambda
        var keySelectorLambda = UnwrapLambda(methodCallExpression.Arguments[argIndex]);

        // Translate the key selector to SQL expressions
        var keyExpressions = TranslateLimitByKeySelector(shapedQuery, keySelectorLambda);

        if (keyExpressions == null || keyExpressions.Count == 0)
        {
            throw new InvalidOperationException(
                "Could not translate LIMIT BY key selector to SQL. " +
                "Ensure the expression references entity properties.");
        }

        _options.LimitByExpressions = keyExpressions;

        return source;
    }

    /// <summary>
    /// Translates the key selector lambda for LIMIT BY clause.
    /// Handles single column selectors and anonymous type selectors (compound keys).
    /// </summary>
    private List<SqlExpression>? TranslateLimitByKeySelector(
        ShapedQueryExpression shapedQuery,
        LambdaExpression keySelector)
    {
        var body = keySelector.Body;

        // Handle anonymous type (compound key): e => new { e.A, e.B }
        if (body is NewExpression newExpr)
        {
            var expressions = new List<SqlExpression>();
            foreach (var arg in newExpr.Arguments)
            {
                // Create a lambda for each member
                var memberLambda = Expression.Lambda(arg, keySelector.Parameters);
                var translated = TranslateLambdaExpression(shapedQuery, memberLambda);
                if (translated == null)
                {
                    return null;
                }
                expressions.Add(translated);
            }
            return expressions;
        }

        // Handle single column selector: e => e.Category
        var singleTranslated = TranslateLambdaExpression(shapedQuery, keySelector);
        if (singleTranslated == null)
        {
            return null;
        }
        return new List<SqlExpression> { singleTranslated };
    }

    /// <summary>
    /// Checks if an argument expression represents an InterpolateMode value
    /// (handles ParameterExpression from EF Core 9+ auto-parameterization).
    /// </summary>
    private static bool IsInterpolateModeArgument(Expression expression)
    {
        // Unwrap Convert if present
        if (expression is UnaryExpression unary &&
            (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
        {
            expression = unary.Operand;
        }

        // ParameterExpression whose type is InterpolateMode (auto-parameterized by EF Core 9+)
        if (expression is ParameterExpression paramExpr && paramExpr.Type == typeof(InterpolateMode))
            return true;

        return false;
    }

    /// <summary>
    /// Unwraps a lambda expression from Quote wrapping.
    /// </summary>
    private static LambdaExpression UnwrapLambda(Expression expression)
    {
        while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
        {
            expression = unary.Operand;
        }

        return expression as LambdaExpression
            ?? throw new InvalidOperationException($"Expected lambda expression, got {expression.GetType().Name}");
    }

    /// <summary>
    /// Extracts the member name from a lambda expression body.
    /// </summary>
    private static string ExtractMemberName(LambdaExpression lambda)
    {
        var body = lambda.Body;

        // Handle Convert wrapper
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            body = unary.Operand;
        }

        if (body is MemberExpression member)
        {
            return member.Member.Name;
        }

        // For complex expressions, use the expression string as key
        return body.ToString();
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

        // EF Core 9+: The funcletizer auto-parameterizes constants into ParameterExpressions.
        // Return false — callers should use TryGetConstantOrParameterValue instead.
        if (expression is ParameterExpression)
        {
            return false;
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

    /// <summary>
    /// Extracts a constant value OR a deferred parameter name from an expression.
    /// In EF Core 9+, constants are auto-parameterized, so this returns the parameter name
    /// as a string (deferred value) when the actual value isn't available at compile time.
    /// The returned object is either T (direct value) or string (parameter name for deferred resolution).
    /// </summary>
    private static object? GetConstantOrDeferredValue(Expression expression)
    {
        // Unwrap Quote expressions
        while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
            expression = unary.Operand;

        // Handle EF.Constant() wrapper
        if (expression is MethodCallExpression mc
            && mc.Method.DeclaringType?.FullName == "Microsoft.EntityFrameworkCore.EF"
            && mc.Method.Name == "Constant"
            && mc.Arguments.Count == 1)
        {
            return GetConstantOrDeferredValue(mc.Arguments[0]);
        }

        // Direct constant
        if (expression is ConstantExpression constant)
            return constant.Value;

        // ParameterExpression — deferred value (EF Core 9+ auto-parameterization)
        if (expression is ParameterExpression paramExpr)
            return new DeferredParameter(paramExpr.Name!);

        // Convert wrapping a constant
        if (expression is UnaryExpression convertExpr &&
            (convertExpr.NodeType == ExpressionType.Convert || convertExpr.NodeType == ExpressionType.ConvertChecked))
            return GetConstantOrDeferredValue(convertExpr.Operand);

        // Member access on closure
        if (expression is MemberExpression memberExpr)
        {
            if (TryGetConstantValue<object>(expression, out var val))
                return val;
        }

        // Try compile-and-execute
        if (TryCompileAndExecute<object>(expression, out var result))
            return result;

        return null;
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
    /// Value is double (direct) or DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    public object? SampleFraction { get; set; }

    /// <summary>
    /// The SAMPLE offset for reproducible sampling.
    /// Value is double (direct) or DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    public object? SampleOffset { get; set; }

    /// <summary>
    /// ClickHouse query settings to append as SETTINGS clause.
    /// Values may be direct objects or DeferredParameter instances.
    /// </summary>
    public Dictionary<string, object> QuerySettings { get; } = new();

    /// <summary>
    /// Deferred settings dictionary (from WithSettings with a parameterized dictionary).
    /// </summary>
    internal DeferredParameter? DeferredSettings { get; set; }

    /// <summary>
    /// Deferred setting pairs (from WithSetting with parameterized name/value).
    /// Each tuple contains (name, value) where either may be a DeferredParameter.
    /// </summary>
    internal List<(object Name, object Value)> DeferredSettingPairs { get; } = new();

    /// <summary>
    /// WITH FILL specifications for ORDER BY columns.
    /// Key: column name
    /// </summary>
    internal Dictionary<string, WithFillColumnSpec> WithFillSpecs { get; } = new();

    /// <summary>
    /// INTERPOLATE specifications for non-ORDER BY columns.
    /// </summary>
    internal List<InterpolateColumnSpec> InterpolateSpecs { get; } = new();

    /// <summary>
    /// Whether any WITH FILL specifications have been added.
    /// </summary>
    public bool HasWithFill => WithFillSpecs.Count > 0;

    /// <summary>
    /// Whether any INTERPOLATE specifications have been added.
    /// </summary>
    public bool HasInterpolate => InterpolateSpecs.Count > 0 || DeferredInterpolateBuilder != null;

    /// <summary>
    /// Deferred interpolate builder (from builder overload with parameterized lambda in EF Core 9+).
    /// Resolved in ResolveDeferredParameters when parameter values are available.
    /// </summary>
    internal DeferredParameter? DeferredInterpolateBuilder { get; set; }

    /// <summary>
    /// The translated PREWHERE predicate expression.
    /// </summary>
    public SqlExpression? PreWhereExpression { get; set; }

    /// <summary>
    /// The LIMIT BY limit (rows per group), or null if no LIMIT BY.
    /// Value is int (direct) or string (deferred parameter name for EF Core 9+ resolution).
    /// </summary>
    public object? LimitByLimit { get; set; }

    /// <summary>
    /// The LIMIT BY offset (rows to skip per group), or null if no offset.
    /// Value is int (direct) or string (deferred parameter name for EF Core 9+ resolution).
    /// </summary>
    public object? LimitByOffset { get; set; }

    /// <summary>
    /// The LIMIT BY column expressions for grouping.
    /// </summary>
    public List<SqlExpression>? LimitByExpressions { get; set; }

    /// <summary>
    /// Whether any LIMIT BY clause has been specified.
    /// </summary>
    public bool HasLimitBy => LimitByLimit != null && LimitByExpressions?.Count > 0;

    /// <summary>
    /// GROUP BY modifier (ROLLUP, CUBE, or TOTALS).
    /// </summary>
    public GroupByModifier GroupByModifier { get; set; } = GroupByModifier.None;

    /// <summary>
    /// The pending CTE name (set during translation, consumed during postprocessing).
    /// Value is string (direct) or DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    internal object? PendingCteName { get; set; }

    /// <summary>
    /// CTE definitions extracted during postprocessing.
    /// </summary>
    internal List<CteDefinition> CteDefinitions { get; } = new();

    /// <summary>
    /// Whether any CTEs have been defined.
    /// </summary>
    public bool HasCtes => CteDefinitions.Count > 0;

    /// <summary>
    /// Raw SQL condition to inject into the WHERE clause.
    /// Value is string (direct) or DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    public object? RawFilterSql { get; set; }

    /// <summary>
    /// ARRAY JOIN specifications for unnesting array columns.
    /// </summary>
    internal List<ArrayJoinSpec> ArrayJoinSpecs { get; } = new();

    /// <summary>
    /// Whether any ARRAY JOIN has been specified.
    /// </summary>
    public bool HasArrayJoin => ArrayJoinSpecs.Count > 0;

    /// <summary>
    /// ASOF JOIN metadata for closest-match joins.
    /// </summary>
    internal AsofJoinInfo? AsofJoin { get; set; }

    /// <summary>
    /// Pre-extracted constant values from method arguments, populated by the preprocessor
    /// before EF Core 9's funcletizer auto-parameterizes them. Keyed by parameter name.
    /// </summary>
    internal Dictionary<string, object> PreExtractedConstants { get; } = new();
}

/// <summary>
/// Specification for a single ARRAY JOIN column.
/// </summary>
internal class ArrayJoinSpec
{
    public required string ColumnName { get; init; }
    public required string Alias { get; init; }
    public bool IsLeft { get; init; }
}

/// <summary>
/// Metadata for an ASOF JOIN inequality condition.
/// </summary>
internal class AsofJoinInfo
{
    public required string LeftColumnName { get; init; }
    public required string RightColumnName { get; init; }
    public required string Operator { get; init; }
    public bool IsLeft { get; init; }
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
        return OptionsStore.GetOrAdd(context, _ => new ClickHouseQueryCompilationContextOptions());
    }

    // Thread-safe store for concurrent query compilation
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<RelationalQueryCompilationContext, ClickHouseQueryCompilationContextOptions>
        OptionsStore = new();
}

/// <summary>
/// Represents a deferred parameter value that will be resolved when parameter values
/// are available (in the RelationalParameterBasedSqlProcessor). Used in EF Core 9+
/// where the funcletizer auto-parameterizes constants before the method translator runs.
/// </summary>
internal sealed class DeferredParameter
{
    public string ParameterName { get; }

    public DeferredParameter(string parameterName)
    {
        ParameterName = parameterName;
    }

    /// <summary>
    /// Resolves this deferred parameter to its actual value from a parameter values dictionary.
    /// </summary>
    public T Resolve<T>(IReadOnlyDictionary<string, object?> parameterValues)
    {
        if (parameterValues.TryGetValue(ParameterName, out var value) && value is T typed)
            return typed;

        if (value != null)
            return (T)Convert.ChangeType(value, typeof(T));

        throw new InvalidOperationException(
            $"Cannot resolve deferred parameter '{ParameterName}' to type {typeof(T).Name}.");
    }

    /// <summary>
    /// Resolves an option value that may be either a direct value or a deferred parameter.
    /// </summary>
    public static T ResolveValue<T>(object? value, IReadOnlyDictionary<string, object?> parameterValues)
    {
        return value switch
        {
            T direct => direct,
            DeferredParameter deferred => deferred.Resolve<T>(parameterValues),
            _ => throw new InvalidOperationException($"Cannot resolve value of type {value?.GetType().Name} to {typeof(T).Name}.")
        };
    }
}
