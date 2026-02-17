using System.Linq.Expressions;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Prevents EF Core from parameterizing arguments to ClickHouse-specific extension methods
/// like Sample(), WithSetting(), WithSettings(), and window functions.
/// </summary>
/// <remarks>
/// <para>
/// EF Core normally extracts constant values into parameters for query caching.
/// However, ClickHouse requires literal values for SAMPLE clauses - parameters don't work.
/// This plugin tells EF Core to keep these values as constants in the expression tree.
/// </para>
/// <para>
/// Window functions (RowNumber, Lag, Lead, etc.) and their WindowBuilder are
/// marker methods that exist only for LINQ expression tree capture. They must not be
/// evaluated before translation.
/// </para>
/// </remarks>
public class ClickHouseEvaluatableExpressionFilterPlugin : IEvaluatableExpressionFilterPlugin
{
    /// <summary>
    /// Determines if an expression should be evaluated (parameterized) or kept as-is.
    /// </summary>
    /// <param name="expression">The expression to check.</param>
    /// <returns>
    /// <c>false</c> if the expression is an argument to a ClickHouse method that requires literals;
    /// <c>true</c> otherwise (allowing normal evaluation/parameterization).
    /// </returns>
    public bool IsEvaluatableExpression(Expression expression)
    {
        // We need to check if this expression is an argument to one of our special methods.
        // The expression itself might be a constant - we need to check its parent context.
        // Unfortunately, we can't see the parent here directly.

        // However, we CAN check if this is a MethodCallExpression to one of our methods.
        // In that case, we tell EF Core not to evaluate (parameterize) it.
        if (expression is MethodCallExpression methodCall)
        {
            var method = methodCall.Method;
            var declaringType = method.DeclaringType;

            // Never evaluate any expression that returns WindowBuilder<T>.
            // This type is a marker for window function OVER clauses and should
            // be kept as-is for expression tree capture and translation.
            if (method.ReturnType.IsGenericType &&
                method.ReturnType.GetGenericTypeDefinition() == typeof(WindowBuilder<>))
            {
                return false;
            }

            // Never evaluate WindowBuilder<T> methods - these are marker methods
            // that build an expression tree for window function translation
            if (declaringType != null &&
                declaringType.IsGenericType &&
                declaringType.GetGenericTypeDefinition() == typeof(WindowBuilder<>))
            {
                return false;
            }

            // Never evaluate Window class methods - these are entry points
            // for window function translation (RowNumber, Rank, Lag, Lead, etc.)
            if (declaringType == typeof(Window))
            {
                return false;
            }

            // Never evaluate WindowSpec methods - these are used in lambda-style
            // window function configuration and must be preserved for translation
            if (declaringType == typeof(WindowSpec))
            {
                return false;
            }

            // Never evaluate EF.Constant() calls - these are used to prevent parameterization
            // of literal values that must appear as constants in generated SQL (e.g., SAMPLE, LIMIT BY)
            if (declaringType?.FullName == "Microsoft.EntityFrameworkCore.EF" &&
                method.Name == "Constant")
            {
                return false;
            }

            // Never evaluate ClickHouseFunctions methods - these are translation stubs
            if (declaringType == typeof(ClickHouseFunctions))
            {
                return false;
            }

            // Never evaluate NewGuidV7() - it's a server-side function stub
            if (declaringType == typeof(ClickHouseUuidDbFunctionsExtensions))
            {
                return false;
            }

            if (method.IsGenericMethod)
            {
                var genericDef = method.GetGenericMethodDefinition();

                // Don't parameterize calls to Sample, WithSetting, WithSettings, or LimitBy
                if (genericDef == ClickHouseQueryableExtensions.SampleMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.SampleWithOffsetMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.WithSettingsMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.LimitByMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.LimitByWithOffsetMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.AsCteMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.WithRawFilterMethodInfo)
                {
                    return false;
                }

                // Don't parameterize calls to Interpolate extension methods
                if (InterpolateExtensions.AllMethodInfos.Contains(genericDef))
                {
                    return false;
                }
            }
        }

        return true;
    }
}
