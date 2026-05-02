using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Reflection meta-test: every <see cref="MigrationOperation"/> implementation
/// declared by EF.CH must have at least one reference inside
/// <c>MigrationSplitterTests.cs</c>. The migration splitter has to know about
/// every custom operation so it can route it into the correct phase; an
/// implementation added without a splitter test would silently land in the
/// default phase, often DDL-after-DML.
/// </summary>
public class MigrationSplitterCoverageMetaTests
{
    [Fact]
    public void EveryIMutationCommandImplementation_HasASplitterTest()
    {
        var asm = typeof(ClickHouseDbContextOptionsExtensions).Assembly;
        var operations = asm.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(t => typeof(MigrationOperation).IsAssignableFrom(t))
            .Where(t => t.Namespace?.StartsWith("EF.CH.", StringComparison.Ordinal) == true)
            .OrderBy(t => t.Name, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(operations);

        // Locate MigrationSplitterTests.cs and the integration tests directly
        // on disk so the meta-test compares against the source-of-truth (not
        // a frozen list). The unit-test project root is two levels up from
        // bin/Debug/net10.0/.
        var rootDir = FindTestSrcRoot();
        var splitterTestSources = Directory
            .EnumerateFiles(rootDir, "*.cs", SearchOption.AllDirectories)
            .Where(p => p.Contains("MigrationSplitter", StringComparison.Ordinal)
                || p.Contains("Migrations/", StringComparison.Ordinal)
                || p.Contains("Migrations\\", StringComparison.Ordinal))
            .ToList();

        var combined = string.Concat(splitterTestSources.Select(File.ReadAllText));

        var unreferenced = operations
            .Where(op => !combined.Contains(op.Name, StringComparison.Ordinal))
            .ToList();

        Assert.True(unreferenced.Count == 0,
            $"the following MigrationOperation implementations have no reference in any " +
            $"Migrations/*Tests.cs file: {string.Join(", ", unreferenced.Select(t => t.Name))}. " +
            "Add a splitter test that exercises the operation's phase classification.");
    }

    private static string FindTestSrcRoot()
    {
        var dir = AppContext.BaseDirectory; // …/bin/Debug/net10.0/
        // Walk up to the project directory containing Migrations/.
        while (dir != null && !Directory.Exists(Path.Combine(dir, "Migrations")))
            dir = Path.GetDirectoryName(dir);
        return dir ?? throw new InvalidOperationException("could not locate Migrations test directory");
    }
}
