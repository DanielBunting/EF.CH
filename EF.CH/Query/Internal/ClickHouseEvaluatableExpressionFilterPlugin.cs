using System.Linq.Expressions;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Query;

namespace EF.CH.Query.Internal;

/// <summary>
/// Prevents EF Core from parameterizing arguments to ClickHouse-specific extension methods
/// like Sample(), WithSetting(), and WithSettings().
/// </summary>
/// <remarks>
/// <para>
/// EF Core normally extracts constant values into parameters for query caching.
/// However, ClickHouse requires literal values for SAMPLE clauses - parameters don't work.
/// This plugin tells EF Core to keep these values as constants in the expression tree.
/// </para>
/// <para>
/// When EF Core visits a method call like <c>Sample(0.1)</c>, it normally would extract
/// <c>0.1</c> as a parameter. With this plugin, the constant stays in place and our
/// translator can read it directly.
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

            if (method.IsGenericMethod)
            {
                var genericDef = method.GetGenericMethodDefinition();

                // Don't parameterize calls to Sample, WithSetting, or WithSettings
                if (genericDef == ClickHouseQueryableExtensions.SampleMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.SampleWithOffsetMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.WithSettingMethodInfo ||
                    genericDef == ClickHouseQueryableExtensions.WithSettingsMethodInfo)
                {
                    return false;
                }
            }
        }

        return true;
    }
}
