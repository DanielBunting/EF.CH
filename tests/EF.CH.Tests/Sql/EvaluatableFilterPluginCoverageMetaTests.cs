using System.Linq.Expressions;
using EF.CH.Extensions;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Reflection meta-test: every <c>*DbFunctionsExtensions</c> class that
/// declares translation stubs (methods that throw "for LINQ translation
/// only") must be registered with
/// <see cref="ClickHouseEvaluatableExpressionFilterPlugin"/>. Otherwise EF
/// Core happily evaluates the stub body at parameterisation time, which
/// throws — long before any helpful translation error reaches the user.
/// <para>
/// Rather than maintain a hand-curated mirror of the plugin's checks, this
/// test feeds a synthetic <c>MethodCallExpression</c> for each stub method
/// into the plugin and asserts the plugin returns <c>false</c>
/// (non-evaluatable). That walks the actual production code path, so any
/// new extension class missing its plugin entry fails this test by virtue
/// of the plugin saying "go ahead and evaluate me" — exactly the bug we're
/// guarding against.
/// </para>
/// </summary>
public class EvaluatableFilterPluginCoverageMetaTests
{
    [Fact]
    public void EveryClickHouseExtensionMethod_IsListedInPlugin()
    {
        var asm = typeof(ClickHouseDbContextOptionsExtensions).Assembly;
        var stubExtensionTypes = asm.GetExportedTypes()
            .Where(t => t.IsClass && t.IsAbstract && t.IsSealed) // static class
            .Where(t => t.Name.EndsWith("DbFunctionsExtensions", StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(stubExtensionTypes);
        var plugin = new ClickHouseEvaluatableExpressionFilterPlugin();
        var missingClasses = new List<string>();

        foreach (var stubType in stubExtensionTypes)
        {
            // Find ANY public static stub method on this class — the plugin
            // is per-DECLARING-TYPE, so checking one method per class is
            // sufficient. Use the first method that has DbFunctions as its
            // first parameter (the canonical stub shape).
            // Pick the first stub method whose parameter list can be reified
            // into a synthetic call expression — i.e. no by-ref / pointer
            // params, and either non-generic or 1-arg-generic so we can close
            // it over `string` for the test.
            var stubMethod = stubType.GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
                .Where(m =>
                {
                    var ps = m.GetParameters();
                    return ps.Length > 0
                        && ps[0].ParameterType == typeof(DbFunctions)
                        && ps.All(p => !p.ParameterType.IsByRef && !p.ParameterType.IsPointer);
                })
                .Select(m => m.IsGenericMethodDefinition && m.GetGenericArguments().Length == 1
                    ? m.MakeGenericMethod(typeof(string))
                    : m)
                .FirstOrDefault(m => !m.IsGenericMethodDefinition);

            if (stubMethod is null) continue;

            var fakeArgs = new List<Expression>
            {
                Expression.Constant(null, typeof(DbFunctions)),
            };
            foreach (var p in stubMethod.GetParameters().Skip(1))
            {
                fakeArgs.Add(Expression.Default(p.ParameterType));
            }
            var call = Expression.Call(stubMethod, fakeArgs);

            if (plugin.IsEvaluatableExpression(call))
            {
                missingClasses.Add(stubType.Name);
            }
        }

        Assert.True(missingClasses.Count == 0,
            $"the following *DbFunctionsExtensions classes are not registered with " +
            $"ClickHouseEvaluatableExpressionFilterPlugin: {string.Join(", ", missingClasses)}. " +
            "Their constant arguments will be evaluated client-side (throwing the stub " +
            "message) instead of being preserved for translation. Add a `declaringType == typeof(...)` " +
            "branch in IsEvaluatableExpression returning `false`.");
    }
}
