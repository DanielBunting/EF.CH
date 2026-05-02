using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Preprocesses queries to rewrite ClickHouse-specific extension methods (ArrayJoin, AsofJoin)
/// into standard LINQ methods before the NavigationExpandingExpressionVisitor runs.
/// This is necessary because type-changing custom methods are rejected by the navigation expander.
/// </summary>
public class ClickHouseQueryTranslationPreprocessor : RelationalQueryTranslationPreprocessor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;

    public ClickHouseQueryTranslationPreprocessor(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = (RelationalQueryCompilationContext)queryCompilationContext;
    }

    public override Expression Process(Expression query)
    {
        // Rewrite custom methods BEFORE the navigation expander runs
        query = new ClickHouseMethodRewritingVisitor(_queryCompilationContext).Visit(query);
        return base.Process(query);
    }
}

/// <summary>
/// Factory for creating ClickHouse query translation preprocessors.
/// </summary>
public class ClickHouseQueryTranslationPreprocessorFactory : IQueryTranslationPreprocessorFactory
{
    private readonly QueryTranslationPreprocessorDependencies _dependencies;
    private readonly RelationalQueryTranslationPreprocessorDependencies _relationalDependencies;

    public ClickHouseQueryTranslationPreprocessorFactory(
        QueryTranslationPreprocessorDependencies dependencies,
        RelationalQueryTranslationPreprocessorDependencies relationalDependencies)
    {
        _dependencies = dependencies;
        _relationalDependencies = relationalDependencies;
    }

    public QueryTranslationPreprocessor Create(QueryCompilationContext queryCompilationContext)
        => new ClickHouseQueryTranslationPreprocessor(
            _dependencies, _relationalDependencies, queryCompilationContext);
}

/// <summary>
/// Rewrites ArrayJoin and AsofJoin method calls into standard LINQ methods
/// so the NavigationExpandingExpressionVisitor can process them.
/// Stores ClickHouse-specific metadata in the query compilation context options.
/// </summary>
internal class ClickHouseMethodRewritingVisitor : ExpressionVisitor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly ClickHouseQueryCompilationContextOptions _options;

    public ClickHouseMethodRewritingVisitor(RelationalQueryCompilationContext queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
        _options = queryCompilationContext.QueryCompilationContextOptions();
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Visit arguments first (handles nested calls like .Final().ArrayJoin(...))
        var visited = (MethodCallExpression)base.VisitMethodCall(node);

        // Force ClickHouseIntervalUnit args to be ConstantExpressions before EF
        // parameterizes them. The translator needs the actual enum value to
        // pick the right toIntervalX function — a SqlParameterExpression at
        // translation time is too late, since the parameter value isn't bound
        // until execution.
        visited = ConstFoldIntervalUnitArgs(visited);

        if (!visited.Method.IsGenericMethod)
            return visited;

        var genericDef = visited.Method.GetGenericMethodDefinition();

        if (genericDef == ClickHouseQueryableExtensions.ArrayJoinMethodInfo)
            return RewriteArrayJoin(visited, isLeft: false);

        if (genericDef == ClickHouseQueryableExtensions.LeftArrayJoinMethodInfo)
            return RewriteArrayJoin(visited, isLeft: true);

        if (genericDef == ClickHouseQueryableExtensions.ArrayJoin2MethodInfo)
            return RewriteArrayJoin2(visited);

        if (genericDef == ClickHouseArrayJoinExtensions.ArrayJoin3MethodInfo)
            return RewriteArrayJoinN(visited, arrayCount: 3);

        if (genericDef == ClickHouseArrayJoinExtensions.ArrayJoin4MethodInfo)
            return RewriteArrayJoinN(visited, arrayCount: 4);

        if (genericDef == ClickHouseArrayJoinExtensions.ArrayJoin5MethodInfo)
            return RewriteArrayJoinN(visited, arrayCount: 5);

        if (genericDef == ClickHouseQueryableExtensions.AsofJoinMethodInfo)
            return RewriteAsofJoin(visited, isLeft: false);

        if (genericDef == ClickHouseQueryableExtensions.AsofLeftJoinMethodInfo)
            return RewriteAsofJoin(visited, isLeft: true);

        if (genericDef == ClickHouseQueryableExtensions.PreWhereMethodInfo)
            return RewritePreWhere(visited);

        // .LimitBy(...).Skip(n) / .Take(n) — EF's NavigationExpandingExpressionVisitor
        // doesn't recognise our custom LimitBy method, so chaining standard
        // Skip/Take after it would otherwise fail with "could not be translated".
        // Push the Skip/Take *under* the LimitBy in the call chain so the
        // expander processes them on a plain IQueryable; ClickHouse evaluates
        // LIMIT BY before global LIMIT/OFFSET, so the rewritten SQL preserves
        // the intended global-skip / global-take semantics
        // (LIMIT n BY key  →  LIMIT [skip,] take).
        if (genericDef == QueryableSkipMethodInfo || genericDef == QueryableTakeMethodInfo)
        {
            if (TryUnwrapLimitBy(visited.Arguments[0], out var inner, out var limitByCall))
            {
                var pushed = visited.Update(visited.Object, new[] { inner!, visited.Arguments[1] });
                return limitByCall!.Update(
                    limitByCall.Object,
                    new[] { (Expression)pushed }.Concat(limitByCall.Arguments.Skip(1)).ToArray());
            }
        }

        // ClickHouseAggregates.XxxMerge on a grouping — rewrite into Enumerable.Sum/Count/…
        // with a MergeSentinel selector so EF's navigation expander accepts it.
        if (visited.Method.DeclaringType == typeof(ClickHouseAggregates)
            && MergeRewrites.TryGetValue(visited.Method.Name, out var rewrite))
        {
            var rewritten = rewrite(visited);
            if (rewritten != null) return rewritten;
        }

        return visited;
    }

    private static readonly MethodInfo QueryableSkipMethodInfo =
        typeof(Queryable).GetMethods()
            .First(m => m.Name == nameof(Queryable.Skip) && m.GetParameters().Length == 2);

    private static readonly MethodInfo QueryableTakeMethodInfo =
        typeof(Queryable).GetMethods()
            .First(m => m.Name == nameof(Queryable.Take)
                && m.GetParameters().Length == 2
                && m.GetParameters()[1].ParameterType == typeof(int));

    /// <summary>
    /// If <paramref name="expr"/> is a <c>LimitBy(...)</c> or <c>LimitBy(..., offset)</c>
    /// call, returns the inner source and the original call so the caller can
    /// rebuild it around a transformed source. Returns <c>false</c> for any
    /// other shape.
    /// </summary>
    private static bool TryUnwrapLimitBy(
        Expression expr,
        out Expression? inner,
        out MethodCallExpression? limitByCall)
    {
        inner = null;
        limitByCall = null;
        if (expr is not MethodCallExpression mc) return false;
        if (!mc.Method.IsGenericMethod) return false;
        var def = mc.Method.GetGenericMethodDefinition();
        if (def != ClickHouseQueryableExtensions.LimitByMethodInfo
            && def != ClickHouseQueryableExtensions.LimitByWithOffsetMethodInfo)
            return false;
        inner = mc.Arguments[0];
        limitByCall = mc;
        return true;
    }

    private static bool TryEvaluateConstant(Expression expr, out object? value)
    {
        value = null;
        if (expr is ConstantExpression c) { value = c.Value; return true; }
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.Quote } u)
            return TryEvaluateConstant(u.Operand, out value);
        if (expr is MemberExpression m && TryEvaluateConstant(m.Expression!, out var holder))
        {
            try
            {
                value = m.Member switch
                {
                    System.Reflection.FieldInfo f => f.GetValue(holder),
                    System.Reflection.PropertyInfo p => p.GetValue(holder),
                    _ => null,
                };
                return true;
            }
            catch { return false; }
        }
        // Last resort: try compiling. Works for pure expressions (e.g. enum literals
        // wrapped in Convert) but fails for queryable shaper / parameter extensions.
        try
        {
            value = Expression.Lambda(expr).Compile().DynamicInvoke();
            return true;
        }
        catch { return false; }
    }

    private static MethodCallExpression ConstFoldIntervalUnitArgs(MethodCallExpression node)
    {
        if (node.Method.DeclaringType != typeof(EF.CH.Extensions.ClickHouseDateTruncDbFunctionsExtensions))
            return node;

        Expression[]? newArgs = null;
        var prms = node.Method.GetParameters();
        for (var i = 0; i < node.Arguments.Count; i++)
        {
            if (i >= prms.Length) break;
            var pType = prms[i].ParameterType;
            var underlying = Nullable.GetUnderlyingType(pType) ?? pType;
            if (underlying != typeof(EF.CH.ClickHouseIntervalUnit))
                continue;
            var arg = node.Arguments[i];
            if (arg is ConstantExpression) continue;
            // Compile the closure-captured / convert-wrapped expression to fold it.
            // Try to extract a constant by walking through Convert/quote layers
            // (the captured-variable expression EF Core has materialised into
            // a closure-member-access chain by this point in the pipeline).
            object? folded = null;
            if (TryEvaluateConstant(arg, out folded))
            {
                newArgs ??= node.Arguments.ToArray();
                newArgs[i] = Expression.Constant(folded, pType);
            }
        }
        return newArgs is null ? node : node.Update(node.Object, newArgs);
    }

    private static readonly Dictionary<string, Func<MethodCallExpression, Expression?>> MergeRewrites = new()
    {
        // -Merge (read AggregatingMergeTree state back into scalars)
        ["CountMerge"] = e => RewriteSimpleMerge(e, "countMerge", "Sum"),
        ["SumMerge"] = e => RewriteSimpleMerge(e, "sumMerge", "Sum"),
        ["AvgMerge"] = e => RewriteSimpleMerge(e, "avgMerge", "Sum"),
        ["MinMerge"] = e => RewriteSimpleMerge(e, "minMerge", "Sum"),
        ["MaxMerge"] = e => RewriteSimpleMerge(e, "maxMerge", "Sum"),
        ["UniqMerge"] = e => RewriteSimpleMerge(e, "uniqMerge", "Sum"),
        ["UniqExactMerge"] = e => RewriteSimpleMerge(e, "uniqExactMerge", "Sum"),
        ["AnyMerge"] = e => RewriteSimpleMerge(e, "anyMerge", "Sum"),
        ["AnyLastMerge"] = e => RewriteSimpleMerge(e, "anyLastMerge", "Sum"),
        ["QuantileMerge"] = e => RewriteParametricMerge(e, "quantileMerge"),

        // Non-merge aggregates on runtime groupings. Each rewrites
        // `ClickHouseAggregates.X(g, ...)` into `Enumerable.Max(g, s => AggregateSentinel...(s.col, "x"))`
        // or similar. The aggregate translator intercepts the sentinel in
        // source.Selector and emits the real function (see
        // ClickHouseAggregateMethodTranslator.TranslateMergeSentinel).
        ["Uniq"] = e => RewriteSimpleAggregate(e, "uniq"),
        ["UniqExact"] = e => RewriteSimpleAggregate(e, "uniqExact"),
        ["UniqCombined"] = e => RewriteSimpleAggregate(e, "uniqCombined"),
        ["UniqCombined64"] = e => RewriteSimpleAggregate(e, "uniqCombined64"),
        ["UniqHLL12"] = e => RewriteSimpleAggregate(e, "uniqHLL12"),
        ["UniqTheta"] = e => RewriteSimpleAggregate(e, "uniqTheta"),
        ["AnyValue"] = e => RewriteSimpleAggregate(e, "any"),
        ["AnyLastValue"] = e => RewriteSimpleAggregate(e, "anyLast"),
        ["Median"] = e => RewriteSimpleAggregate(e, "median"),
        ["StddevPop"] = e => RewriteSimpleAggregate(e, "stddevPop"),
        ["StddevSamp"] = e => RewriteSimpleAggregate(e, "stddevSamp"),
        ["VarPop"] = e => RewriteSimpleAggregate(e, "varPop"),
        ["VarSamp"] = e => RewriteSimpleAggregate(e, "varSamp"),
        ["GroupArray"] = e => RewriteGroupArray(e, "groupArray"),
        ["GroupUniqArray"] = e => RewriteGroupArray(e, "groupUniqArray"),
        ["Quantile"] = e => RewriteParametricAggregate(e, "quantile"),
        ["QuantileTDigest"] = e => RewriteParametricAggregate(e, "quantileTDigest"),
        ["QuantileExact"] = e => RewriteParametricAggregate(e, "quantileExact"),
        ["QuantileTiming"] = e => RewriteParametricAggregate(e, "quantileTiming"),
        ["Quantiles"] = e => RewriteMultiQuantileAggregate(e, "quantiles"),
        ["QuantilesTDigest"] = e => RewriteMultiQuantileAggregate(e, "quantilesTDigest"),
        ["TopK"] = e => RewriteTopK(e, "topK"),
        ["TopKWeighted"] = e => RewriteTopKWeighted(e, "topKWeighted"),
        ["ArgMax"] = e => RewriteTwoSelectorAggregate(e, "argMax"),
        ["ArgMin"] = e => RewriteTwoSelectorAggregate(e, "argMin"),
        ["QuantileDD"] = e => RewriteTwoParamQuantile(e, "quantileDD"),

        // -If combinators on runtime groupings. Each rewrites
        // `g.SumIf(sel, p)` into `g.Sum(s => AggregateSentinelTwoArg(sel(s), p(s), "sumIf"))`
        // (or no-arg sentinel for CountIf which has only a predicate). The aggregate
        // translator detects the sentinel selector and emits the real -If aggregate.
        ["CountIf"] = e => RewritePredicateOnlyAggregate(e, "countIf"),
        ["SumIf"] = e => RewriteSelectorPredicateAggregate(e, "sumIf"),
        ["AvgIf"] = e => RewriteSelectorPredicateAggregate(e, "avgIf"),
        ["MinIf"] = e => RewriteSelectorPredicateAggregate(e, "minIf"),
        ["MaxIf"] = e => RewriteSelectorPredicateAggregate(e, "maxIf"),
        ["UniqIf"] = e => RewriteSelectorPredicateAggregate(e, "uniqIf"),
        ["UniqExactIf"] = e => RewriteSelectorPredicateAggregate(e, "uniqExactIf"),
        ["UniqCombinedIf"] = e => RewriteSelectorPredicateAggregate(e, "uniqCombinedIf"),
        ["UniqCombined64If"] = e => RewriteSelectorPredicateAggregate(e, "uniqCombined64If"),
        ["UniqHLL12If"] = e => RewriteSelectorPredicateAggregate(e, "uniqHLL12If"),
        ["UniqThetaIf"] = e => RewriteSelectorPredicateAggregate(e, "uniqThetaIf"),
        ["AnyIf"] = e => RewriteSelectorPredicateAggregate(e, "anyIf"),
        ["AnyLastIf"] = e => RewriteSelectorPredicateAggregate(e, "anyLastIf"),
        ["MedianIf"] = e => RewriteSelectorPredicateAggregate(e, "medianIf"),
        ["StddevPopIf"] = e => RewriteSelectorPredicateAggregate(e, "stddevPopIf"),
        ["StddevSampIf"] = e => RewriteSelectorPredicateAggregate(e, "stddevSampIf"),
        ["VarPopIf"] = e => RewriteSelectorPredicateAggregate(e, "varPopIf"),
        ["VarSampIf"] = e => RewriteSelectorPredicateAggregate(e, "varSampIf"),
        ["ArgMaxIf"] = e => RewriteTwoSelectorPredicateAggregate(e, "argMaxIf"),
        ["ArgMinIf"] = e => RewriteTwoSelectorPredicateAggregate(e, "argMinIf"),
        ["GroupArrayIf"] = e => RewriteGroupArrayIf(e, "groupArrayIf"),
        ["GroupUniqArrayIf"] = e => RewriteGroupArrayIf(e, "groupUniqArrayIf"),
        ["TopKIf"] = e => RewriteTopKIf(e, "topKIf"),
        ["TopKWeightedIf"] = e => RewriteTopKWeightedIf(e, "topKWeightedIf"),
        ["QuantileIf"] = e => RewriteParametricSelectorPredicateAggregate(e, "quantileIf"),
        ["QuantileTDigestIf"] = e => RewriteParametricSelectorPredicateAggregate(e, "quantileTDigestIf"),
        ["QuantileExactIf"] = e => RewriteParametricSelectorPredicateAggregate(e, "quantileExactIf"),
        ["QuantileTimingIf"] = e => RewriteParametricSelectorPredicateAggregate(e, "quantileTimingIf"),
        ["QuantilesIf"] = e => RewriteMultiQuantileSelectorPredicateAggregate(e, "quantilesIf"),
        ["QuantilesTDigestIf"] = e => RewriteMultiQuantileSelectorPredicateAggregate(e, "quantilesTDigestIf"),

        // Typed-return aggregates — emit count() / sum() with the user's declared ulong return.
        ["CountUInt64"] = e => RewriteCountUInt64(e),
        ["SumUInt64"] = e => RewriteSumUInt64(e),
    };

    /// <summary>
    /// Rewrites <c>g.CountIf(predicate)</c> into
    /// <c>g.Sum(s =&gt; AggregateSentinel&lt;bool, long, long&gt;(predicate(s), "countIf"))</c>.
    /// The sentinel carries the predicate body as the aggregate input; the translator emits
    /// <c>countIf(predicate)</c> directly.
    /// </summary>
    private static Expression? RewritePredicateOnlyAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        if (call.Arguments.Count != 2) return null;
        var groupingArg = call.Arguments[0];
        var predicate = UnwrapLambda(call.Arguments[1]);
        var sourceType = call.Method.GetGenericArguments()[1]; // [TKey, TElement]; element at [1]
        var returnType = call.Method.ReturnType;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinel))!
            .MakeGenericMethod(typeof(bool), typeof(long), returnType);
        var newBody = Expression.Call(sentinel, predicate.Body, Expression.Constant(sqlFunctionName));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(long)),
            newBody, predicate.Parameters);

        var sumLong = FindSumWithReturnType(sourceType, typeof(long));
        Expression agg = Expression.Call(null, sumLong, groupingArg, newSelector);
        if (returnType != typeof(long)) agg = Expression.Convert(agg, returnType);
        return agg;
    }

    /// <summary>
    /// Rewrites <c>g.SumIf(selector, predicate)</c> (and other selector+predicate -If
    /// combinators) into <c>g.Sum/Max(s =&gt; AggregateSentinelTwoArg(sel(s), pred(s), "sumIf"))</c>.
    /// The sentinel emits <c>sumIf(selector, predicate)</c> directly.
    /// </summary>
    private static Expression? RewriteSelectorPredicateAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        if (call.Arguments.Count != 3) return null;
        var groupingArg = call.Arguments[0];
        var selector = UnwrapLambda(call.Arguments[1]);
        var predicate = UnwrapLambda(call.Arguments[2]);
        var sourceType = call.Method.GetGenericArguments()[1]; // TElement at index 1
        var selType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        // Unify both lambdas under the selector's parameter so the rebuilt body
        // references a single free parameter.
        var predBody = new ParameterReplacer(predicate.Parameters[0], selector.Parameters[0])
            .Visit(predicate.Body)!;

        var useSumSurrogate = IsSummableType(returnType);
        var surrogateType = useSumSurrogate ? typeof(long) : returnType;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArg))!
            .MakeGenericMethod(selType, typeof(bool), surrogateType, returnType);
        var newBody = Expression.Call(sentinel, selector.Body, predBody, Expression.Constant(sqlFunctionName));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, surrogateType),
            newBody, selector.Parameters);

        Expression agg;
        if (useSumSurrogate)
        {
            var sumLong = FindSumWithReturnType(sourceType, typeof(long));
            agg = Expression.Call(null, sumLong, groupingArg, newSelector);
            if (returnType != typeof(long)) agg = Expression.Convert(agg, returnType);
        }
        else
        {
            var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
            agg = Expression.Call(null, maxMethod, groupingArg, newSelector);
        }
        return agg;
    }

    /// <summary>
    /// Rewrites <c>g.CountUInt64()</c> into
    /// <c>g.Max(s =&gt; AggregateSentinel&lt;int, ulong, ulong&gt;(1, "count"))</c>.
    /// The sentinel emits <c>count(1)</c> wrapped in <c>toUInt64</c> (semantically identical
    /// to <c>count()</c> in ClickHouse, returning UInt64), bypassing the <c>toInt32</c>-wrapping
    /// standard Count path. We use <see cref="Enumerable.Max{TSource, TResult}"/> as the
    /// surrogate carrier so EF's aggregate whitelist accepts the call without the
    /// <c>CAST(NULL, 'UInt64')</c> pitfall that affects Sum on unsigned types.
    /// </summary>
    private static Expression? RewriteCountUInt64(MethodCallExpression call)
    {
        if (call.Arguments.Count != 1) return null;
        var groupingArg = call.Arguments[0];
        var sourceType = call.Method.GetGenericArguments()[1]; // TElement at index 1

        var paramExpr = Expression.Parameter(sourceType, "s");
        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinel))!
            .MakeGenericMethod(typeof(int), typeof(ulong), typeof(ulong));
        // Pass any non-null member access as input — count's actual SQL is count(arg)
        // counting non-nulls; for an entity-property reference all values are non-null.
        // Using a constant 1 would also work but we use the entity to mirror the
        // SumUInt64 / UniqCombined column-reference path for consistency.
        var newBody = Expression.Call(sentinel, Expression.Constant(1), Expression.Constant("count"));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(ulong)),
            newBody, paramExpr);

        var maxMethod = FindMaxWithGenericReturn(sourceType, typeof(ulong));
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.SumUInt64(selector)</c> into <c>(ulong)g.Sum(selector)</c>.
    /// The standard <see cref="Enumerable.Sum{TSource}"/> path with an outer Convert is the
    /// simplest route — at SQL time it emits <c>toUInt64(sumOrNull(selector) ?? 0)</c>
    /// which materialises correctly to <see cref="ulong"/>. The sentinel-based path
    /// (which would emit bare <c>sum(...)</c>) hits an EF Core auto-coercion bug for
    /// <c>sum</c> + non-default numeric return types.
    /// </summary>
    private static Expression? RewriteSumUInt64(MethodCallExpression call)
    {
        if (call.Arguments.Count != 2) return null;
        var groupingArg = call.Arguments[0];
        var selector = UnwrapLambda(call.Arguments[1]);
        var sourceType = call.Method.GetGenericArguments()[1]; // TElement at index 1
        var selType = selector.Body.Type;

        // Only proceed if the selector returns a Sum-friendly numeric type. We promote
        // to a long-typed selector and lean on EF's standard Sum translator.
        if (!IsSummableType(selType)) return null;

        // Build: g.Sum(s => (long)selector(s)) → typed long
        // then Convert the final result to ulong at the LINQ level.
        var castedBody = selType == typeof(long)
            ? selector.Body
            : (Expression)Expression.Convert(selector.Body, typeof(long));
        var castedSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(long)),
            castedBody,
            selector.Parameters);

        var sumLong = FindSumWithReturnType(sourceType, typeof(long));
        Expression agg = Expression.Call(null, sumLong, groupingArg, castedSelector);
        return Expression.Convert(agg, typeof(ulong));
    }

    private static Expression? RewriteTwoParamQuantile(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: X(grouping, double p1, double p2, Func<T, double>) → double
        if (call.Arguments.Count != 4) return null;
        var groupingArg = call.Arguments[0];
        var p1 = call.Arguments[1];
        var p2 = call.Arguments[2];
        var selector = UnwrapLambda(call.Arguments[3]);
        // After the shift to IGrouping<TKey, TElement>, generic args are [TKey, TElement, ...]
        // and the source/element type is at index 1.
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var paramsArr = Expression.NewArrayInit(typeof(double), p1, p2);
        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelMultiParam))!
            .MakeGenericMethod(inputType, typeof(double), returnType);
        var newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName), paramsArr);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(double)),
            newBody, selector.Parameters);

        var sumDouble = FindSumWithReturnType(sourceType, typeof(double));
        Expression agg = Expression.Call(null, sumDouble, groupingArg, newSelector);
        if (returnType != typeof(double)) agg = Expression.Convert(agg, returnType);
        return agg;
    }

    private static Expression? RewriteTopKWeighted(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: X(grouping, int k, Func<T, TValue> valueSelector, Func<T, TWeight> weightSelector)
        // → topKWeighted(k)(value, weight) returning TValue[]
        if (call.Arguments.Count != 4) return null;
        var groupingArg = call.Arguments[0];
        var kArg = call.Arguments[1];
        var valueSel = UnwrapLambda(call.Arguments[2]);
        var weightSel = UnwrapLambda(call.Arguments[3]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var valueType = valueSel.Body.Type;
        var weightType = weightSel.Body.Type;
        var returnType = call.Method.ReturnType;

        // Both selector lambdas have their own ParameterExpression instances —
        // unify them under valueSel's parameter so the resulting body references
        // a single free parameter.
        var weightBody = new ParameterReplacer(weightSel.Parameters[0], valueSel.Parameters[0])
            .Visit(weightSel.Body)!;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArgIntParam))!
            .MakeGenericMethod(valueType, weightType, returnType, returnType);
        var newBody = Expression.Call(sentinel,
            valueSel.Body,
            weightBody,
            Expression.Constant(sqlFunctionName),
            kArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody,
            valueSel.Parameters);

        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }


    /// <summary>
    /// Rewrites <c>ClickHouseAggregates.XxxMerge(grouping, sel)</c> into
    /// <c>Enumerable.Sum(grouping, s =&gt; ClickHouseFunctions.MergeSentinel&lt;long&gt;(sel(s), "xxxMerge"))</c>.
    /// Navigation expander accepts <c>Enumerable.Sum</c>; the aggregate translator later
    /// detects the sentinel in the selector and emits <c>xxxMerge(state)</c> directly with
    /// the user's declared return type. If the user's return type isn't <c>long</c> we wrap
    /// the outer call in a Convert so the LINQ type-checking matches; at SQL-translation
    /// time that Convert is a no-op because the sentinel emits the real function.
    /// </summary>
    private static Expression? RewriteSimpleMerge(
        MethodCallExpression call,
        string sqlFunctionName,
        string _surrogateAggregate)
    {
        if (call.Arguments.Count != 2) return null;

        var groupingArg = call.Arguments[0];
        var selector = UnwrapLambda(call.Arguments[1]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var userReturnType = call.Method.ReturnType;

        // Numerics ride through Enumerable.Sum<TSource> (which is in EF Core's
        // aggregate whitelist). For non-numeric types like string, Sum has no
        // matching overload and Convert(long, string) isn't a valid coercion —
        // use Enumerable.Max<TSource, TResult> instead, which accepts any
        // result type and is also recognised by the aggregate translator.
        var useSumSurrogate = IsSummableType(userReturnType);
        var surrogateType = useSumSurrogate ? typeof(long) : userReturnType;

        var sentinelMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.MergeSentinel))!
            .MakeGenericMethod(surrogateType, userReturnType);

        var newBody = Expression.Call(
            sentinelMethod,
            selector.Body,
            Expression.Constant(sqlFunctionName));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, surrogateType),
            newBody,
            selector.Parameters);

        Expression aggregate;
        if (useSumSurrogate)
        {
            var sumLong = typeof(Enumerable).GetMethods()
                .Where(m => m.Name == "Sum" && m.GetParameters().Length == 2 && m.IsGenericMethodDefinition)
                .First(m => m.GetParameters()[1].ParameterType.GetGenericArguments().Last() == typeof(long))
                .MakeGenericMethod(sourceType);
            aggregate = Expression.Call(null, sumLong, groupingArg, newSelector);
            if (userReturnType != typeof(long))
                aggregate = Expression.Convert(aggregate, userReturnType);
        }
        else
        {
            var maxMethod = FindMaxWithGenericReturn(sourceType, userReturnType);
            aggregate = Expression.Call(null, maxMethod, groupingArg, newSelector);
        }

        return aggregate;
    }

    private static bool IsSummableType(Type t)
    {
        var u = Nullable.GetUnderlyingType(t) ?? t;
        return u == typeof(long) || u == typeof(int) || u == typeof(short)
            || u == typeof(sbyte) || u == typeof(ulong) || u == typeof(uint)
            || u == typeof(ushort) || u == typeof(byte) || u == typeof(double)
            || u == typeof(float) || u == typeof(decimal);
    }

    private static Expression? RewriteParametricMerge(
        MethodCallExpression call,
        string sqlFunctionName)
    {
        // Args: [0]=grouping, [1]=level (double), [2]=stateSelector
        if (call.Arguments.Count != 3) return null;

        var groupingArg = call.Arguments[0];
        var levelArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var userReturnType = call.Method.ReturnType;

        var sentinelMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.MergeSentinelParametric))!
            .MakeGenericMethod(typeof(double), userReturnType);

        var newBody = Expression.Call(
            sentinelMethod,
            selector.Body,
            Expression.Constant(sqlFunctionName),
            levelArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(double)),
            newBody,
            selector.Parameters);

        var sumDouble = typeof(Enumerable).GetMethods()
            .Where(m => m.Name == "Sum" && m.GetParameters().Length == 2 && m.IsGenericMethodDefinition)
            .First(m => m.GetParameters()[1].ParameterType.GetGenericArguments().Last() == typeof(double))
            .MakeGenericMethod(sourceType);

        Expression aggregate = Expression.Call(null, sumDouble, groupingArg, newSelector);
        if (userReturnType != typeof(double))
            aggregate = Expression.Convert(aggregate, userReturnType);
        return aggregate;
    }

    // ─── Non-merge aggregate rewrites ────────────────────────────────────────

    private static Expression? RewriteSimpleAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: ClickHouseAggregates.X(grouping, Func<T, TInput>) → returnType
        if (call.Arguments.Count != 2) return null;
        var groupingArg = call.Arguments[0];
        var selector = UnwrapLambda(call.Arguments[1]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        // Sentinel: AggregateSentinel<TInput, TSurrogate, TReal>(input, fn) → TSurrogate.
        // Surrogate = long so we can hit Enumerable.Sum<TSource, long> — then Convert to TReal.
        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinel))!
            .MakeGenericMethod(inputType, typeof(long), returnType);
        var newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(long)),
            newBody, selector.Parameters);

        var sumLong = FindSumWithReturnType(sourceType, typeof(long));
        Expression agg = Expression.Call(null, sumLong, groupingArg, newSelector);
        if (returnType != typeof(long)) agg = Expression.Convert(agg, returnType);
        return agg;
    }

    private static Expression? RewriteParametricAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: X(grouping, double level, Func<T, double>) → double
        if (call.Arguments.Count != 3) return null;
        var groupingArg = call.Arguments[0];
        var levelArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelParametric))!
            .MakeGenericMethod(inputType, typeof(double), returnType);
        var newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName), levelArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, typeof(double)),
            newBody, selector.Parameters);

        var sumDouble = FindSumWithReturnType(sourceType, typeof(double));
        Expression agg = Expression.Call(null, sumDouble, groupingArg, newSelector);
        if (returnType != typeof(double)) agg = Expression.Convert(agg, returnType);
        return agg;
    }

    private static Expression? RewriteMultiQuantileAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: X(grouping, double[] levels, Func<T, double>) → double[]
        if (call.Arguments.Count != 3) return null;
        var groupingArg = call.Arguments[0];
        var levelsArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType; // double[]

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelMultiParam))!
            .MakeGenericMethod(inputType, returnType, returnType);
        var newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName), levelsArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);

        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    private static Expression? RewriteGroupArray(MethodCallExpression call, string sqlFunctionName)
    {
        // Two overloads:
        //   X(grouping, Func<T, TValue>) → TValue[]
        //   X(grouping, int maxSize, Func<T, TValue>) → TValue[]
        var groupingArg = call.Arguments[0];
        LambdaExpression selector;
        Expression? maxSizeArg = null;
        if (call.Arguments.Count == 2)
        {
            selector = UnwrapLambda(call.Arguments[1]);
        }
        else if (call.Arguments.Count == 3)
        {
            maxSizeArg = call.Arguments[1];
            selector = UnwrapLambda(call.Arguments[2]);
        }
        else return null;

        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        Expression newBody;
        if (maxSizeArg is null)
        {
            var sentinel = typeof(ClickHouseFunctions)
                .GetMethod(nameof(ClickHouseFunctions.AggregateSentinel))!
                .MakeGenericMethod(inputType, returnType, returnType);
            newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName));
        }
        else
        {
            var sentinel = typeof(ClickHouseFunctions)
                .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelIntParametric))!
                .MakeGenericMethod(inputType, returnType, returnType);
            newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName), maxSizeArg);
        }

        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);
        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    private static Expression? RewriteTopK(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: TopK(grouping, int k, Func<T, TValue>) → TValue[]
        if (call.Arguments.Count != 3) return null;
        var groupingArg = call.Arguments[0];
        var kArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelIntParametric))!
            .MakeGenericMethod(inputType, returnType, returnType);
        var newBody = Expression.Call(sentinel, selector.Body, Expression.Constant(sqlFunctionName), kArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);

        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    private static Expression? RewriteTwoSelectorAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: ArgMax(grouping, Func<T, TArg>, Func<T, TVal>) → TArg
        if (call.Arguments.Count != 3) return null;
        var groupingArg = call.Arguments[0];
        var argSelector = UnwrapLambda(call.Arguments[1]);
        var valSelector = UnwrapLambda(call.Arguments[2]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var argType = argSelector.Body.Type;
        var valType = valSelector.Body.Type;
        var returnType = call.Method.ReturnType;

        // Build selector: s => AggregateSentinelTwoArg<TArg, TVal, TReturn, TReturn>(argSel(s), valSel(s), "argMax").
        // We reuse the argSelector's parameter and rebuild the val-body against it.
        var param = argSelector.Parameters[0];
        var valBody = new ParameterReplacer(valSelector.Parameters[0], param).Visit(valSelector.Body);

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArg))!
            .MakeGenericMethod(argType, valType, returnType, returnType);
        var newBody = Expression.Call(sentinel, argSelector.Body, valBody!, Expression.Constant(sqlFunctionName));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, argSelector.Parameters);

        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.ArgMaxIf(argSel, valSel, predicate)</c> into
    /// <c>g.Max(s =&gt; AggregateSentinelThreeArg(argSel(s), valSel(s), pred(s), "argMaxIf"))</c>.
    /// The aggregate translator emits <c>argMaxIf(arg, val, predicate)</c> directly.
    /// </summary>
    private static Expression? RewriteTwoSelectorPredicateAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: ArgMaxIf(grouping, Func<T, TArg>, Func<T, TVal>, Func<T, bool>) → TArg
        if (call.Arguments.Count != 4) return null;
        var groupingArg = call.Arguments[0];
        var argSelector = UnwrapLambda(call.Arguments[1]);
        var valSelector = UnwrapLambda(call.Arguments[2]);
        var predicate = UnwrapLambda(call.Arguments[3]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var argType = argSelector.Body.Type;
        var valType = valSelector.Body.Type;
        var returnType = call.Method.ReturnType;

        // Unify all three lambdas under a single parameter (argSelector's).
        var param = argSelector.Parameters[0];
        var valBody = new ParameterReplacer(valSelector.Parameters[0], param).Visit(valSelector.Body)!;
        var predBody = new ParameterReplacer(predicate.Parameters[0], param).Visit(predicate.Body)!;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelThreeArg))!
            .MakeGenericMethod(argType, valType, typeof(bool), returnType, returnType);
        var newBody = Expression.Call(sentinel, argSelector.Body, valBody, predBody, Expression.Constant(sqlFunctionName));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, argSelector.Parameters);

        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.GroupArrayIf(selector, predicate)</c> and the maxSize overload
    /// into a Max-surrogate aggregate carrying the right -If sentinel. The maxSize
    /// form uses ClickHouse's parametric syntax <c>groupArrayIf(N)(col, predicate)</c>,
    /// rendered via <see cref="ClickHouseMergeSentinelExpression.MergeParameter"/>.
    /// </summary>
    private static Expression? RewriteGroupArrayIf(MethodCallExpression call, string sqlFunctionName)
    {
        var groupingArg = call.Arguments[0];
        Expression? maxSizeArg = null;
        LambdaExpression selector;
        LambdaExpression predicate;
        if (call.Arguments.Count == 3)
        {
            selector = UnwrapLambda(call.Arguments[1]);
            predicate = UnwrapLambda(call.Arguments[2]);
        }
        else if (call.Arguments.Count == 4)
        {
            maxSizeArg = call.Arguments[1];
            selector = UnwrapLambda(call.Arguments[2]);
            predicate = UnwrapLambda(call.Arguments[3]);
        }
        else return null;

        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var predBody = new ParameterReplacer(predicate.Parameters[0], selector.Parameters[0])
            .Visit(predicate.Body)!;

        Expression newBody;
        if (maxSizeArg is null)
        {
            var sentinel = typeof(ClickHouseFunctions)
                .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArg))!
                .MakeGenericMethod(inputType, typeof(bool), returnType, returnType);
            newBody = Expression.Call(sentinel, selector.Body, predBody, Expression.Constant(sqlFunctionName));
        }
        else
        {
            // groupArrayIf(N)(col, predicate) — N is parametric, (col, predicate) are positional.
            var sentinel = typeof(ClickHouseFunctions)
                .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArgIntParam))!
                .MakeGenericMethod(inputType, typeof(bool), returnType, returnType);
            newBody = Expression.Call(sentinel, selector.Body, predBody, Expression.Constant(sqlFunctionName), maxSizeArg);
        }

        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);
        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.TopKIf(k, selector, predicate)</c> into a Max surrogate carrying
    /// a <c>topKIf(k)</c> sentinel. ClickHouse syntax: <c>topKIf(k)(col, predicate)</c>.
    /// </summary>
    private static Expression? RewriteTopKIf(MethodCallExpression call, string sqlFunctionName)
    {
        if (call.Arguments.Count != 4) return null;
        var groupingArg = call.Arguments[0];
        var kArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var predicate = UnwrapLambda(call.Arguments[3]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var predBody = new ParameterReplacer(predicate.Parameters[0], selector.Parameters[0])
            .Visit(predicate.Body)!;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArgIntParam))!
            .MakeGenericMethod(inputType, typeof(bool), returnType, returnType);
        var newBody = Expression.Call(sentinel, selector.Body, predBody, Expression.Constant(sqlFunctionName), kArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);
        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.TopKWeightedIf(k, valueSelector, weightSelector, predicate)</c> into a
    /// Max surrogate carrying a <c>topKWeightedIf(k)</c> sentinel. ClickHouse syntax:
    /// <c>topKWeightedIf(k)(col, weight, predicate)</c>.
    /// </summary>
    private static Expression? RewriteTopKWeightedIf(MethodCallExpression call, string sqlFunctionName)
    {
        if (call.Arguments.Count != 5) return null;
        var groupingArg = call.Arguments[0];
        var kArg = call.Arguments[1];
        var valueSelector = UnwrapLambda(call.Arguments[2]);
        var weightSelector = UnwrapLambda(call.Arguments[3]);
        var predicate = UnwrapLambda(call.Arguments[4]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var valueType = valueSelector.Body.Type;
        var weightType = weightSelector.Body.Type;
        var returnType = call.Method.ReturnType;

        var param = valueSelector.Parameters[0];
        var weightBody = new ParameterReplacer(weightSelector.Parameters[0], param).Visit(weightSelector.Body)!;
        var predBody = new ParameterReplacer(predicate.Parameters[0], param).Visit(predicate.Body)!;

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelThreeArgIntParam))!
            .MakeGenericMethod(valueType, weightType, typeof(bool), returnType, returnType);
        var newBody = Expression.Call(sentinel, valueSelector.Body, weightBody, predBody, Expression.Constant(sqlFunctionName), kArg);
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, valueSelector.Parameters);
        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.QuantileXxxIf(level, selector, predicate)</c> into a Sum surrogate
    /// carrying a <c>quantileXxxIf(level)</c> sentinel. ClickHouse syntax:
    /// <c>quantileXxxIf(level)(col, predicate)</c>.
    /// </summary>
    private static Expression? RewriteParametricSelectorPredicateAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: X(grouping, double level, Func<T, double> selector, Func<T, bool> predicate) → double
        if (call.Arguments.Count != 4) return null;
        var groupingArg = call.Arguments[0];
        var levelArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var predicate = UnwrapLambda(call.Arguments[3]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var predBody = new ParameterReplacer(predicate.Parameters[0], selector.Parameters[0])
            .Visit(predicate.Body)!;

        // Use double-parametric two-arg sentinel: parameter renders as "(level)" prefix.
        // Reuses AggregateSentinelTwoArg + a parameter via secondArg; but we need a parametric
        // double, not int. ClickHouseMergeSentinelExpression.MergeParameter accepts double.
        // Build by piggy-backing on AggregateSentinelTwoArgIntParam isn't suitable (int only),
        // so we add a custom path: pack both lambdas + the level via sentinel and let
        // TranslateMergeSentinel render quantileXxxIf(level)(col, predicate). We use the
        // existing AggregateSentinelTwoArg shape and wedge the level through the function-name
        // string itself (the renderer accepts pre-formatted "(N)" suffixes).
        var levelText = levelArg switch
        {
            ConstantExpression { Value: double d } => d.ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => null,
        };
        if (levelText is null) return null; // levels must be constants for the ClickHouse parametric form.

        var nameWithLevel = $"{sqlFunctionName}({levelText})";
        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArg))!
            .MakeGenericMethod(inputType, typeof(bool), returnType, returnType);
        var newBody = Expression.Call(sentinel, selector.Body, predBody, Expression.Constant(nameWithLevel));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);
        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    /// <summary>
    /// Rewrites <c>g.QuantilesXxxIf(levels[], selector, predicate)</c> into a Max surrogate
    /// carrying a <c>quantilesXxxIf(l1, l2, …)</c> sentinel.
    /// </summary>
    private static Expression? RewriteMultiQuantileSelectorPredicateAggregate(MethodCallExpression call, string sqlFunctionName)
    {
        // Shape: X(grouping, double[] levels, Func<T, double> selector, Func<T, bool> predicate) → double[]
        if (call.Arguments.Count != 4) return null;
        var groupingArg = call.Arguments[0];
        var levelsArg = call.Arguments[1];
        var selector = UnwrapLambda(call.Arguments[2]);
        var predicate = UnwrapLambda(call.Arguments[3]);
        var sourceType = call.Method.GetGenericArguments()[1];
        var inputType = selector.Body.Type;
        var returnType = call.Method.ReturnType;

        var predBody = new ParameterReplacer(predicate.Parameters[0], selector.Parameters[0])
            .Visit(predicate.Body)!;

        double[]? levelArr = levelsArg switch
        {
            ConstantExpression { Value: double[] d } => d,
            NewArrayExpression nae when nae.Expressions.All(x => x is ConstantExpression { Value: double })
                => nae.Expressions.Select(x => (double)((ConstantExpression)x).Value!).ToArray(),
            _ => null,
        };
        if (levelArr is null) return null;

        var inv = System.Globalization.CultureInfo.InvariantCulture;
        var nameWithLevels = $"{sqlFunctionName}({string.Join(", ", levelArr.Select(l => l.ToString(inv)))})";

        var sentinel = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArg))!
            .MakeGenericMethod(inputType, typeof(bool), returnType, returnType);
        var newBody = Expression.Call(sentinel, selector.Body, predBody, Expression.Constant(nameWithLevels));
        var newSelector = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(sourceType, returnType),
            newBody, selector.Parameters);
        var maxMethod = FindMaxWithGenericReturn(sourceType, returnType);
        return Expression.Call(null, maxMethod, groupingArg, newSelector);
    }

    // ─── Surrogate lookups ──────────────────────────────────────────────────

    private static MethodInfo FindSumWithReturnType(Type sourceType, Type returnType)
        => typeof(Enumerable).GetMethods()
            .Where(m => m.Name == "Sum" && m.GetParameters().Length == 2 && m.IsGenericMethodDefinition)
            .First(m => m.GetParameters()[1].ParameterType.GetGenericArguments().Last() == returnType)
            .MakeGenericMethod(sourceType);

    private static MethodInfo FindMaxWithGenericReturn(Type sourceType, Type returnType)
        => typeof(Enumerable).GetMethods()
            .Where(m => m.Name == "Max" && m.GetParameters().Length == 2 && m.IsGenericMethodDefinition)
            .First(m => m.GetGenericArguments().Length == 2)
            .MakeGenericMethod(sourceType, returnType);

    /// <summary>
    /// Rewrites ArrayJoin(source, arraySelector, resultSelector) → source.Select(modifiedResultSelector)
    /// where element parameter references are replaced with RawSql calls.
    /// </summary>
    private Expression RewriteArrayJoin(MethodCallExpression call, bool isLeft)
    {
        // Arguments: [0]=source, [1]=arraySelector (quoted), [2]=resultSelector (quoted)
        var source = call.Arguments[0];
        var arraySelector = UnwrapLambda(call.Arguments[1]);
        var resultSelector = UnwrapLambda(call.Arguments[2]);

        var propertyName = ExtractMemberName(arraySelector);
        var columnName = ResolveColumnName(call.Method.GetGenericArguments()[0], propertyName);
        var alias = resultSelector.Parameters[1].Name ?? columnName;
        var elementType = resultSelector.Parameters[1].Type;

        _options.ArrayJoinSpecs.Add(new ArrayJoinSpec
        {
            ColumnName = columnName,
            Alias = alias,
            IsLeft = isLeft
        });

        // Replace element parameter with RawSql<TElement>("\"alias\"")
        var rawSqlMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.RawSql))!
            .MakeGenericMethod(elementType);
        var quotedAlias = "\"" + alias + "\"";
        var rawSqlCall = Expression.Call(rawSqlMethod, Expression.Constant(quotedAlias));

        var newBody = new ParameterReplacer(resultSelector.Parameters[1], rawSqlCall)
            .Visit(resultSelector.Body);
        var selectLambda = Expression.Lambda(newBody, resultSelector.Parameters[0]);

        // Rewrite as Queryable.Select(source, selectLambda)
        var entityType = resultSelector.Parameters[0].Type;
        var resultType = resultSelector.ReturnType;
        var selectMethod = GetQueryableSelectMethod().MakeGenericMethod(entityType, resultType);

        return Expression.Call(null, selectMethod, source, Expression.Quote(selectLambda));
    }

    /// <summary>
    /// Rewrites ArrayJoin with two arrays.
    /// </summary>
    private Expression RewriteArrayJoin2(MethodCallExpression call)
    {
        // Arguments: [0]=source, [1]=arraySelector1, [2]=arraySelector2, [3]=resultSelector
        var source = call.Arguments[0];
        var arraySelector1 = UnwrapLambda(call.Arguments[1]);
        var arraySelector2 = UnwrapLambda(call.Arguments[2]);
        var resultSelector = UnwrapLambda(call.Arguments[3]);

        var genericArgs = call.Method.GetGenericArguments();
        var entityClrType = genericArgs[0];

        var propName1 = ExtractMemberName(arraySelector1);
        var colName1 = ResolveColumnName(entityClrType, propName1);
        var alias1 = resultSelector.Parameters[1].Name ?? colName1;
        var elemType1 = resultSelector.Parameters[1].Type;

        var propName2 = ExtractMemberName(arraySelector2);
        var colName2 = ResolveColumnName(entityClrType, propName2);
        var alias2 = resultSelector.Parameters[2].Name ?? colName2;
        var elemType2 = resultSelector.Parameters[2].Type;

        _options.ArrayJoinSpecs.Add(new ArrayJoinSpec { ColumnName = colName1, Alias = alias1, IsLeft = false });
        _options.ArrayJoinSpecs.Add(new ArrayJoinSpec { ColumnName = colName2, Alias = alias2, IsLeft = false });

        var rawSqlMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.RawSql))!;

        var rawSql1 = Expression.Call(rawSqlMethod.MakeGenericMethod(elemType1),
            Expression.Constant("\"" + alias1 + "\""));
        var rawSql2 = Expression.Call(rawSqlMethod.MakeGenericMethod(elemType2),
            Expression.Constant("\"" + alias2 + "\""));

        var newBody = new ParameterReplacer(resultSelector.Parameters[1], rawSql1).Visit(resultSelector.Body);
        newBody = new ParameterReplacer(resultSelector.Parameters[2], rawSql2).Visit(newBody);
        var selectLambda = Expression.Lambda(newBody, resultSelector.Parameters[0]);

        var resultType = resultSelector.ReturnType;
        var selectMethod = GetQueryableSelectMethod().MakeGenericMethod(entityClrType, resultType);

        return Expression.Call(null, selectMethod, source, Expression.Quote(selectLambda));
    }

    /// <summary>
    /// Rewrites the N-array (3, 4, 5) ARRAY JOIN forms.
    /// Argument layout: [0]=source, [1..N]=array selectors, [N+1]=resultSelector.
    /// </summary>
    private Expression RewriteArrayJoinN(MethodCallExpression call, int arrayCount)
    {
        var source = call.Arguments[0];
        var arraySelectors = new LambdaExpression[arrayCount];
        for (var i = 0; i < arrayCount; i++)
        {
            arraySelectors[i] = UnwrapLambda(call.Arguments[1 + i]);
        }
        var resultSelector = UnwrapLambda(call.Arguments[1 + arrayCount]);

        var entityClrType = call.Method.GetGenericArguments()[0];
        var rawSqlMethod = typeof(ClickHouseFunctions)
            .GetMethod(nameof(ClickHouseFunctions.RawSql))!;

        var newBody = resultSelector.Body;
        for (var i = 0; i < arrayCount; i++)
        {
            var propName = ExtractMemberName(arraySelectors[i]);
            var colName = ResolveColumnName(entityClrType, propName);
            // Parameters[0] is the entity; element params are 1, 2, ..., arrayCount
            var elementParam = resultSelector.Parameters[1 + i];
            var alias = elementParam.Name ?? colName;
            var elemType = elementParam.Type;

            _options.ArrayJoinSpecs.Add(new ArrayJoinSpec
            {
                ColumnName = colName,
                Alias = alias,
                IsLeft = false
            });

            var rawSqlCall = Expression.Call(
                rawSqlMethod.MakeGenericMethod(elemType),
                Expression.Constant("\"" + alias + "\""));
            newBody = new ParameterReplacer(elementParam, rawSqlCall).Visit(newBody);
        }

        var selectLambda = Expression.Lambda(newBody, resultSelector.Parameters[0]);
        var resultType = resultSelector.ReturnType;
        var selectMethod = GetQueryableSelectMethod().MakeGenericMethod(entityClrType, resultType);

        return Expression.Call(null, selectMethod, source, Expression.Quote(selectLambda));
    }

    /// <summary>
    /// Rewrites <c>source.PreWhere(predicate)</c> into
    /// <c>source.Where(x => ClickHouseFunctions.PreWhereMarker(predicate(x)))</c>.
    /// The Where call is recognised by the navigation expander; the marker is later
    /// detected in the SQL translator, which lifts the inner predicate into
    /// <see cref="ClickHouseQueryCompilationContextOptions.PreWhereExpression"/> and
    /// returns a constant true so the WHERE clause becomes a no-op.
    /// </summary>
    private static Expression RewritePreWhere(MethodCallExpression call)
    {
        var source = call.Arguments[0];
        var predicate = UnwrapLambda(call.Arguments[1]);
        var entityType = call.Method.GetGenericArguments()[0];

        var markerMethod = typeof(ClickHouseFunctions).GetMethod(
            nameof(ClickHouseFunctions.PreWhereMarker),
            BindingFlags.NonPublic | BindingFlags.Static)!;
        var markedBody = Expression.Call(markerMethod, predicate.Body);
        var newPredicate = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(entityType, typeof(bool)),
            markedBody,
            predicate.Parameters);

        var whereMethod = typeof(Queryable).GetMethods()
            .Where(m => m.Name == nameof(Queryable.Where) && m.GetParameters().Length == 2)
            .First(m => m.GetParameters()[1].ParameterType
                .GetGenericArguments()[0]
                .GetGenericArguments().Length == 2)
            .MakeGenericMethod(entityType);

        return Expression.Call(null, whereMethod, source, Expression.Quote(newPredicate));
    }

    /// <summary>
    /// Rewrites AsofJoin(outer, inner, outerKey, innerKey, asofCond, resultSelector)
    /// → Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
    /// </summary>
    private Expression RewriteAsofJoin(MethodCallExpression call, bool isLeft)
    {
        // Arguments: [0]=outer, [1]=inner, [2]=outerKey, [3]=innerKey, [4]=asofCondition, [5]=resultSelector
        var asofCondition = UnwrapLambda(call.Arguments[4]);
        var (leftPropName, rightPropName, op) = ClickHouseAsofConditionParser.Parse(asofCondition);

        var genericArgs = call.Method.GetGenericArguments(); // TOuter, TInner, TKey, TResult
        var leftColName = ResolveColumnName(genericArgs[0], leftPropName);
        var rightColName = ResolveColumnName(genericArgs[1], rightPropName);

        if (_options.AsofJoin != null)
        {
            throw new InvalidOperationException("Only one ASOF JOIN per query is supported.");
        }

        _options.AsofJoin = new AsofJoinInfo
        {
            LeftColumnName = leftColName,
            RightColumnName = rightColName,
            Operator = op,
            IsLeft = isLeft
        };

        // Rewrite as Queryable.Join(outer, inner, outerKey, innerKey, resultSelector)
        var joinMethod = typeof(Queryable).GetMethods()
            .First(m => m.Name == "Join" && m.GetParameters().Length == 5)
            .MakeGenericMethod(genericArgs);

        return Expression.Call(
            null, joinMethod,
            call.Arguments[0],  // outer
            call.Arguments[1],  // inner
            call.Arguments[2],  // outerKeySelector
            call.Arguments[3],  // innerKeySelector
            call.Arguments[5]); // resultSelector (skip asofCondition at [4])
    }

    // ASOF condition parsing lives in ClickHouseAsofConditionParser; both this
    // preprocessor and the design-time MaterializedViewSqlTranslator call it.

    private static string ExtractMemberName(LambdaExpression lambda)
    {
        var body = lambda.Body;
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            body = unary.Operand;

        if (body is MemberExpression member)
            return member.Member.Name;

        return body.ToString();
    }

    private string ResolveColumnName(Type entityType, string propertyName)
    {
        var efEntityType = _queryCompilationContext.Model.FindEntityType(entityType);
        if (efEntityType == null)
            throw new InvalidOperationException($"Entity type {entityType.Name} not found in model.");

        var property = efEntityType.FindProperty(propertyName);
        if (property == null)
            throw new InvalidOperationException($"Property {propertyName} not found on {entityType.Name}.");

        return property.GetColumnName() ?? propertyName;
    }

    private static LambdaExpression UnwrapLambda(Expression expression)
    {
        while (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
            expression = unary.Operand;

        return expression as LambdaExpression
            ?? throw new InvalidOperationException($"Expected lambda expression, got {expression.GetType().Name}");
    }

    private static MethodInfo GetQueryableSelectMethod()
    {
        return typeof(Queryable).GetMethods()
            .Where(m => m.Name == "Select" && m.GetParameters().Length == 2)
            .First(m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0]
                .GetGenericArguments().Length == 2);
    }

    private class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        private readonly Expression _replacement;

        public ParameterReplacer(ParameterExpression target, Expression replacement)
        {
            _target = target;
            _replacement = replacement;
        }

        protected override Expression VisitParameter(ParameterExpression node)
            => node == _target ? _replacement : base.VisitParameter(node);
    }
}
