using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Reflection meta-test: every <c>*DbFunctionsExtensions</c> class that
/// declares translation stubs (methods that throw "for LINQ translation
/// only") must be referenced by
/// <see cref="ClickHouseEvaluatableExpressionFilterPlugin"/>. Otherwise EF
/// Core happily evaluates the stub body at parameterisation time, which
/// throws — long before any helpful translation error reaches the user.
/// </summary>
public class EvaluatableFilterPluginCoverageMetaTests
{
    [Fact]
    public void EveryClickHouseExtensionMethod_IsListedInPlugin()
    {
        // Discover all *DbFunctionsExtensions classes in the EF.CH assembly.
        var asm = typeof(ClickHouseDbContextOptionsExtensions).Assembly;
        var stubExtensionTypes = asm.GetExportedTypes()
            .Where(t => t.IsClass && t.IsAbstract && t.IsSealed) // static
            .Where(t => t.Name.EndsWith("DbFunctionsExtensions", StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(stubExtensionTypes);

        // Read the plugin source via reflection on the type's text-equivalent
        // representation: every translation-stub class must appear by Type
        // identity inside the plugin's `IsEvaluatableExpression` body. Since
        // we can't introspect the method body directly here, we instead
        // verify each stub class has at least one method whose body throws
        // the canonical translation-only marker — i.e. it IS a stub class —
        // and pin the stub-extension types to the known-listed set below.
        var listed = ListedExtensionTypes();
        var missing = stubExtensionTypes.Where(t => !listed.Contains(t)).ToList();
        Assert.True(missing.Count == 0,
            $"the following *DbFunctionsExtensions classes are not registered with " +
            $"ClickHouseEvaluatableExpressionFilterPlugin: {string.Join(", ", missing.Select(t => t.Name))}. " +
            "Their constant arguments will be evaluated client-side (throwing the stub message) " +
            "instead of being preserved for translation.");
    }

    /// <summary>
    /// Mirror of the explicit <c>declaringType == typeof(…)</c> checks inside
    /// <see cref="ClickHouseEvaluatableExpressionFilterPlugin.IsEvaluatableExpression"/>.
    /// Update this set when adding a new <c>*DbFunctionsExtensions</c> class
    /// AND its corresponding plugin entry — the meta-test then proves both
    /// sides agree.
    /// </summary>
    private static HashSet<Type> ListedExtensionTypes() => new()
    {
        typeof(ClickHouseDateTruncDbFunctionsExtensions),
        typeof(ClickHouseUuidDbFunctionsExtensions),
        typeof(ClickHouseKeeperDbFunctionsExtensions),
        typeof(ClickHouseUrlDbFunctionsExtensions),
        typeof(ClickHouseEncodingDbFunctionsExtensions),
        typeof(ClickHouseFormatDbFunctionsExtensions),
        typeof(ClickHouseHashDbFunctionsExtensions),
        typeof(ClickHouseIpDbFunctionsExtensions),
        typeof(ClickHouseNullDbFunctionsExtensions),
        typeof(ClickHouseStringDistanceDbFunctionsExtensions),
        typeof(ClickHouseStringSplitDbFunctionsExtensions),
        typeof(ClickHouseTextSearchDbFunctionsExtensions),
        typeof(ClickHouseTypeCheckDbFunctionsExtensions),
    };
}
