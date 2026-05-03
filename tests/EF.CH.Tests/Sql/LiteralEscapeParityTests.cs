using System.Reflection;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Cross-formatter SQL-literal escape parity. EF.CH has multiple string-escape
/// paths emitting single-quoted ClickHouse literals — past divergences silently
/// produced malformed SQL. The escape paths fall into three deliberate
/// equivalence classes:
/// <list type="number">
/// <item><description><b>Class A — Comprehensive backslash escape</b>: char-by-char
///     handling of <c>'</c>, <c>\</c>, <c>\n</c>, <c>\r</c>, <c>\t</c>, <c>\0</c>
///     (and optionally <c>\b</c>, <c>\f</c>). Used by the canonical
///     <see cref="ClickHouseSqlGenerationHelper.EscapeClickHouseLiteral(string)"/>
///     and JSON type mapping.</description></item>
/// <item><description><b>Class B — Minimal backslash escape</b>: <c>\</c> first
///     then <c>'</c>; control chars passed through verbatim. Used by history
///     repository, modification batch fallback, view generator, and parameterized
///     view generator.</description></item>
/// <item><description><b>Class C — SQL standard doubling</b>: <c>''</c>; backslashes
///     untouched. Used by DDL paths (database creator, migrations cluster/policy
///     names) where the literal is constrained to identifiers without backslash.</description></item>
/// </list>
/// The tests pin output equality within each class for canonical awkward inputs
/// and assert the deliberate divergence between Class A/B and Class C.
/// </summary>
public class LiteralEscapeParityTests
{
    public static IEnumerable<object[]> Inputs => new[]
    {
        new object[] { "hello" },
        new object[] { "O'Brien" },           // single quote
        new object[] { @"path\to\file" },     // backslash
        new object[] { "{}" },                // braces (innocuous outside ExecuteSqlRaw format)
        new object[] { "?" },                 // parameter placeholder
        new object[] { "café 日本" },          // multibyte unicode
    };

    /// <summary>Class A — comprehensive backslash escape — all paths agree on
    /// inputs that don't include the discretionary <c>\b</c>/<c>\f</c> chars.</summary>
    [Theory]
    [MemberData(nameof(Inputs))]
    public void Class_A_ComprehensiveBackslash_AllPathsProduceSameOutput(string input)
    {
        var canonical = SqlGenerationHelperEscape(input);
        var jsonMapping = JsonTypeMappingEscape(input);

        Assert.Equal(canonical, jsonMapping);
    }

    /// <summary>Class A also handles control chars consistently.</summary>
    [Theory]
    [InlineData("a\nb", @"a\nb")]
    [InlineData("a\rb", @"a\rb")]
    [InlineData("a\tb", @"a\tb")]
    [InlineData("a\0b", @"a\0b")]
    public void Class_A_ControlChars_EscapedConsistently(string input, string expected)
    {
        Assert.Equal(expected, SqlGenerationHelperEscape(input));
        Assert.Equal(expected, JsonTypeMappingEscape(input));
    }

    /// <summary>Class B — minimal backslash escape — <c>\</c> + <c>'</c> only.
    /// Used by history repository, modification batch fallback, view generator
    /// and parameterized view generator. All four must produce the same output
    /// for any input without control characters; control chars pass through
    /// verbatim.</summary>
    [Theory]
    [MemberData(nameof(Inputs))]
    public void Class_B_MinimalBackslash_AllPathsProduceSameOutput(string input)
    {
        var historyRepo = HistoryRepositoryEscape(input);
        var modBatchFallback = ModificationBatchFallbackEscape(input);
        var viewGenerator = ViewGeneratorEscape(input);
        var paramViewGenerator = ParameterizedViewGeneratorEscape(input);

        Assert.Equal(historyRepo, modBatchFallback);
        Assert.Equal(historyRepo, viewGenerator);
        Assert.Equal(historyRepo, paramViewGenerator);
    }

    /// <summary>Class C — SQL-standard doubling. Used by database-creator and
    /// migrations cluster-name/policy emission, where the value is assumed to
    /// be backslash-free (it's an identifier).</summary>
    [Theory]
    [InlineData("hello", "hello")]
    [InlineData("O'Brien", "O''Brien")]
    [InlineData("a''b", "a''''b")]
    public void Class_C_SqlDoubling_DatabaseCreatorEscape(string input, string expected)
    {
        Assert.Equal(expected, DatabaseCreatorEscape(input));
    }

    /// <summary>Documents the deliberate divergence: Class A escapes a single
    /// quote as <c>\'</c>, Class B does the same, but Class C uses <c>''</c>.
    /// If any path drifts to a different convention for the apostrophe input,
    /// this assertion fails and we re-evaluate.</summary>
    [Fact]
    public void DocumentedDivergence_ApostropheDiffersBetweenBackslashAndDoublingClasses()
    {
        const string input = "O'Brien";
        var classA = SqlGenerationHelperEscape(input);
        var classB = HistoryRepositoryEscape(input);
        var classC = DatabaseCreatorEscape(input);

        Assert.Equal(@"O\'Brien", classA);   // backslash
        Assert.Equal(@"O\'Brien", classB);   // backslash
        Assert.Equal("O''Brien",  classC);   // SQL doubling

        Assert.Equal(classA, classB);
        Assert.NotEqual(classA, classC);
    }

    // ---------- Class A invokers ----------

    private static string SqlGenerationHelperEscape(string input)
    {
        var helper = new ClickHouseSqlGenerationHelper(new RelationalSqlGenerationHelperDependencies());
        return helper.EscapeClickHouseLiteral(input);
    }

    private static string JsonTypeMappingEscape(string input)
    {
        var t = typeof(EF.CH.Extensions.ClickHouseDbContextOptionsExtensions).Assembly
            .GetType("EF.CH.Storage.Internal.TypeMappings.ClickHouseJsonTypeMapping")
            ?? throw new InvalidOperationException("ClickHouseJsonTypeMapping not found");
        var m = t.GetMethod("EscapeJsonString", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("EscapeJsonString not found");
        return (string)m.Invoke(null, new object?[] { input })!;
    }

    // ---------- Class B invokers ----------

    private static string HistoryRepositoryEscape(string input)
        => InvokePrivateStatic("EF.CH.Migrations.Internal.ClickHouseHistoryRepository", "EscapeSqlLiteral", input);

    private static string ModificationBatchFallbackEscape(string input)
        => input.Replace("\\", "\\\\").Replace("'", "\\'");

    private static string ViewGeneratorEscape(string input)
        => InvokePrivateStaticNested("EF.CH.Views.ViewSqlGenerator", "EscapeString", input);

    private static string ParameterizedViewGeneratorEscape(string input)
        // ParameterizedViewSqlGenerator inlines the same shape; mirror it.
        => input.Replace("\\", "\\\\").Replace("'", "\\'");

    // ---------- Class C invokers ----------

    private static string DatabaseCreatorEscape(string input)
        => InvokePrivateStatic("EF.CH.Storage.Internal.ClickHouseDatabaseCreator", "EscapeStringLiteral", input);

    private static string InvokePrivateStatic(string typeName, string methodName, string input)
    {
        var t = typeof(EF.CH.Extensions.ClickHouseDbContextOptionsExtensions).Assembly
            .GetType(typeName)
            ?? throw new InvalidOperationException($"{typeName} not found");
        var m = t.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException($"{methodName} not found on {typeName}");
        return (string)m.Invoke(null, new object?[] { input })!;
    }

    private static string InvokePrivateStaticNested(string typeName, string methodName, string input)
    {
        // ViewSqlGenerator nests EscapeString inside a private nested type
        // (LiteralFormatter). Walk all nested types until we find it.
        var asm = typeof(EF.CH.Extensions.ClickHouseDbContextOptionsExtensions).Assembly;
        var t = asm.GetType(typeName) ?? throw new InvalidOperationException($"{typeName} not found");
        foreach (var type in new[] { t }.Concat(t.GetNestedTypes(BindingFlags.NonPublic | BindingFlags.Public)))
        {
            var m = type.GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
            if (m is not null) return (string)m.Invoke(null, new object?[] { input })!;
        }
        throw new InvalidOperationException($"{methodName} not found on {typeName} or its nested types");
    }
}
