using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Query.Internal.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates calls to <c>ClickHouseFunctions.MergeSentinel*</c> into a
/// <see cref="ClickHouseMergeSentinelExpression"/> that carries the real
/// merge function name and state column through the SQL translation stage
/// until the aggregate translator unwraps it.
/// </summary>
public class ClickHouseMergeSentinelTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo SentinelMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.MergeSentinel))!;

    private static readonly MethodInfo SentinelParametricMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.MergeSentinelParametric))!;

    private static readonly MethodInfo SentinelMultiParamMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.MergeSentinelMultiParam))!;

    private static readonly MethodInfo AggSentinelMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinel))!;
    private static readonly MethodInfo AggSentinelParametricMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelParametric))!;
    private static readonly MethodInfo AggSentinelIntParametricMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelIntParametric))!;
    private static readonly MethodInfo AggSentinelMultiParamMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelMultiParam))!;
    private static readonly MethodInfo AggSentinelTwoArgMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArg))!;
    private static readonly MethodInfo AggSentinelTwoArgIntParamMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelTwoArgIntParam))!;
    private static readonly MethodInfo AggSentinelThreeArgMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelThreeArg))!;
    private static readonly MethodInfo AggSentinelThreeArgIntParamMethod =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.AggregateSentinelThreeArgIntParam))!;

    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseMergeSentinelTranslator(IRelationalTypeMappingSource typeMappingSource)
    {
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (!method.IsGenericMethod)
            return null;

        var genericDef = method.GetGenericMethodDefinition();
        var returnType = method.ReturnType;
        var typeMapping = _typeMappingSource.FindMapping(returnType);

        // The sentinel's generic arguments are (TSurrogate, TReal). TSurrogate is only there
        // so the outer Sum/Count carrier has a valid signature; TReal is the user's real
        // declared return type — that's what we want the SqlFunctionExpression to carry.
        var genericArgs = method.GetGenericArguments();
        var realReturnType = genericArgs.Length >= 2 ? genericArgs[1] : returnType;
        var realMapping = _typeMappingSource.FindMapping(realReturnType);

        if (genericDef == SentinelMethod)
        {
            var functionName = RequireStringConstant(arguments[1]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, realReturnType, realMapping);
        }

        if (genericDef == SentinelParametricMethod)
        {
            var functionName = RequireStringConstant(arguments[1]);
            var parameter = RequireDoubleConstant(arguments[2]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, realReturnType, realMapping, parameter: parameter);
        }

        if (genericDef == SentinelMultiParamMethod)
        {
            var functionName = RequireStringConstant(arguments[1]);
            var parameters = RequireDoubleArrayConstant(arguments[2]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, realReturnType, realMapping, parameters: parameters);
        }

        // Aggregate sentinels — generic args are (TInput, TSurrogate, TReal).
        // TReal is at index 2. For two-arg variants it's (TArg, TVal, TSurrogate, TReal)
        // so TReal is at index 3.
        if (genericDef == AggSentinelMethod)
        {
            var aggReal = genericArgs[2];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[1]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping);
        }

        if (genericDef == AggSentinelParametricMethod)
        {
            var aggReal = genericArgs[2];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[1]);
            var parameter = RequireDoubleConstant(arguments[2]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping, parameter: parameter);
        }

        if (genericDef == AggSentinelIntParametricMethod)
        {
            var aggReal = genericArgs[2];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[1]);
            var parameter = (double)RequireIntConstant(arguments[2]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping, parameter: parameter);
        }

        if (genericDef == AggSentinelMultiParamMethod)
        {
            var aggReal = genericArgs[2];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[1]);
            var parameters = RequireDoubleArrayConstant(arguments[2]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping, parameters: parameters);
        }

        if (genericDef == AggSentinelTwoArgMethod)
        {
            var aggReal = genericArgs[3];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[2]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping, secondArg: arguments[1]);
        }

        if (genericDef == AggSentinelTwoArgIntParamMethod)
        {
            var aggReal = genericArgs[3];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[2]);
            var parameter = (double)RequireIntConstant(arguments[3]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping, parameter: parameter, secondArg: arguments[1]);
        }

        // Three-arg sentinel: (TArg, TVal, TPred, TSurrogate, TReal) — TReal at index 4.
        if (genericDef == AggSentinelThreeArgMethod)
        {
            var aggReal = genericArgs[4];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[3]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping,
                secondArg: arguments[1], thirdArg: arguments[2]);
        }

        // Three-arg + int-parametric: (TArg, TVal, TPred, TSurrogate, TReal) with `(N)` prefix.
        // Used by topKWeightedIf(k)(col, weight, predicate).
        if (genericDef == AggSentinelThreeArgIntParamMethod)
        {
            var aggReal = genericArgs[4];
            var mapping = _typeMappingSource.FindMapping(aggReal);
            var functionName = RequireStringConstant(arguments[3]);
            var parameter = (double)RequireIntConstant(arguments[4]);
            return new ClickHouseMergeSentinelExpression(
                arguments[0], functionName, aggReal, mapping,
                parameter: parameter, secondArg: arguments[1], thirdArg: arguments[2]);
        }

        return null;
    }

    private static int RequireIntConstant(SqlExpression expr)
    {
        if (expr is SqlConstantExpression { Value: int i }) return i;
        throw new InvalidOperationException($"Expected constant int, got {expr.GetType().Name}.");
    }

    private static string RequireStringConstant(SqlExpression expr)
    {
        if (expr is SqlConstantExpression { Value: string s }) return s;
        throw new InvalidOperationException($"Expected constant string, got {expr.GetType().Name}.");
    }

    private static double RequireDoubleConstant(SqlExpression expr)
    {
        if (expr is SqlConstantExpression { Value: double d }) return d;
        throw new InvalidOperationException($"Expected constant double, got {expr.GetType().Name}.");
    }

    private static double[] RequireDoubleArrayConstant(SqlExpression expr)
    {
        if (expr is SqlConstantExpression { Value: double[] arr }) return arr;
        throw new InvalidOperationException($"Expected constant double[], got {expr.GetType().Name}.");
    }
}
