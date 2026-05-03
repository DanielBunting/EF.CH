using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Query.Internal;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// <see cref="ClickHouseEvaluatableExpressionFilterPlugin"/> is an explicit allow-list:
/// any custom queryable method that takes a constant-literal arg but isn't listed will
/// have its arg silently parameterized — and ClickHouse functions that *require* a
/// literal (SAMPLE coefficient, SETTINGS values, etc.) then fail at execution. There's
/// no marker attribute today, so this test enumerates the <c>internal static readonly
/// MethodInfo</c> fields on <see cref="ClickHouseQueryableExtensions"/> and forces the
/// author of any new method to consciously decide: list it in
/// <see cref="ExemptFromFilter"/> (the method takes no constant-literal args), or wire
/// it into the filter.
///
/// The same discipline applies to <see cref="ClickHouseArrayJoinExtensions"/>.
/// </summary>
public class EvaluatableFilterCoverageTests
{
    /// <summary>
    /// MethodInfo fields whose methods take no parameters that need to remain literal,
    /// or that are intentionally not in the filter for another reason. Every entry needs
    /// a one-line justification next to it.
    /// </summary>
    private static readonly HashSet<string> ExemptFromFilter =
    [
        // Final() / WithRollup / WithCube / WithTotals take no args beyond the source.
        "FinalMethodInfo",
        "WithRollupMethodInfo",
        "WithCubeMethodInfo",
        "WithTotalsMethodInfo",

        // PreWhere takes a Expression<Func<,>>; predicate body translation handles it.
        "PreWhereMethodInfo",

        // Join variants below take Expression<Func<,>> predicate args; the join
        // translator handles them and there are no constant-literal args that need to
        // stay unparameterized. AsofJoin / AsofLeftJoin *are* in the filter because
        // they additionally carry an inequality marker arg that must remain literal.
        "AnyJoinMethodInfo",
        "AnyLeftJoinMethodInfo",
        "AnyRightJoinMethodInfo",
        "RightJoinMethodInfo",
        "FullOuterJoinMethodInfo",
        "LeftSemiJoinMethodInfo",
        "LeftAntiJoinMethodInfo",
        "RightSemiJoinMethodInfo",
        "RightAntiJoinMethodInfo",
        "CrossJoinMethodInfo",
    ];

    [Fact]
    public void Every_ClickHouseQueryableExtensions_MethodInfo_Is_Covered_Or_Exempt()
    {
        var filter = new ClickHouseEvaluatableExpressionFilterPlugin();

        var allFields = typeof(ClickHouseQueryableExtensions)
            .GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(f => f.FieldType == typeof(MethodInfo))
            .ToList();

        Assert.NotEmpty(allFields); // sanity: we are still finding fields

        var notCovered = new List<string>();
        foreach (var field in allFields)
        {
            if (ExemptFromFilter.Contains(field.Name))
            {
                continue;
            }

            var method = (MethodInfo)field.GetValue(null)!;
            if (!IsCoveredByFilter(filter, method))
            {
                notCovered.Add(field.Name);
            }
        }

        Assert.True(notCovered.Count == 0,
            "These MethodInfo fields on ClickHouseQueryableExtensions are not " +
            "referenced by ClickHouseEvaluatableExpressionFilterPlugin and are not in the " +
            "ExemptFromFilter list. Either wire them into the filter or add them to the " +
            "exempt list with a justification:\n  " + string.Join("\n  ", notCovered));
    }

    private static bool IsCoveredByFilter(ClickHouseEvaluatableExpressionFilterPlugin filter, MethodInfo method)
    {
        // The filter uses generic-definition equality; for non-generic methods it falls
        // through to declaring-type checks (which the test methods don't hit because
        // ClickHouseQueryableExtensions itself isn't blocklisted by type).
        // Build a fake MethodCallExpression and ask the filter directly.
        try
        {
            var methodToTest = method.IsGenericMethodDefinition
                ? CloseGeneric(method)
                : method;

            var args = methodToTest.GetParameters()
                .Select(p => (System.Linq.Expressions.Expression)System.Linq.Expressions.Expression.Default(p.ParameterType))
                .ToArray();

            var call = System.Linq.Expressions.Expression.Call(methodToTest, args);
            return !filter.IsEvaluatableExpression(call);
        }
        catch
        {
            // If we can't construct the call (e.g. constraints), assume the field is not
            // covered — better to fail loudly and prompt the dev to inspect.
            return false;
        }
    }

    private static MethodInfo CloseGeneric(MethodInfo openGeneric)
    {
        var typeArgs = openGeneric.GetGenericArguments()
            .Select(_ => typeof(object))
            .ToArray();
        return openGeneric.MakeGenericMethod(typeArgs);
    }
}
