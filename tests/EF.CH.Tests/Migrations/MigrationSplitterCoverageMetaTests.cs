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
        // Try a few strategies in order:
        // 1) AppContext.BaseDirectory walk-up (works in `dotnet test` from
        //    the repo with the test project's source nearby).
        // 2) MSBuild-injected `EF_CH_TEST_SOURCE_ROOT` environment variable
        //    (set by CI / publish flows that strip source from output).
        // 3) Walk up from the assembly Location.
        // If none locate a folder containing the meta-test's own source,
        // throw a clear error that names what was tried.
        var attempts = new List<string>();

        var fromEnv = Environment.GetEnvironmentVariable("EF_CH_TEST_SOURCE_ROOT");
        if (!string.IsNullOrEmpty(fromEnv) && Directory.Exists(fromEnv))
        {
            attempts.Add($"env EF_CH_TEST_SOURCE_ROOT={fromEnv}");
            return fromEnv;
        }

        var roots = new[] { AppContext.BaseDirectory, typeof(MigrationSplitterCoverageMetaTests).Assembly.Location };
        foreach (var root in roots.Where(s => !string.IsNullOrEmpty(s)))
        {
            attempts.Add(root);
            var dir = root.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
                ? Path.GetDirectoryName(root)
                : root;
            while (dir != null)
            {
                var migrationsDir = Path.Combine(dir, "Migrations");
                // Only accept folders that ALSO contain this very meta-test
                // source — proves we've found a real source tree, not a
                // packaged output that happens to have a Migrations folder.
                var selfSource = Path.Combine(migrationsDir, nameof(MigrationSplitterCoverageMetaTests) + ".cs");
                if (File.Exists(selfSource))
                    return dir;
                dir = Path.GetDirectoryName(dir);
            }
        }

        throw new InvalidOperationException(
            $"Could not locate Migrations test source directory. Tried: {string.Join("; ", attempts)}. " +
            "Set EF_CH_TEST_SOURCE_ROOT to the test project's source root if running from a " +
            "stripped publish output.");
    }
}
