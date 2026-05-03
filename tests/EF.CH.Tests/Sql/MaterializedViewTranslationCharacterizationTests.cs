using System.Reflection;
using System.Text;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Characterization snapshot of the materialized-view translator's SQL output
/// across every MV-bearing <see cref="DbContext"/> in this test assembly.
///
/// Purpose: lock the byte-level output before refactoring the MV translator
/// pipeline (eliminating reflection in the dispatch + visitor construction).
/// Any change in translated SQL fails this test fast, locally, before the
/// (slow, Docker-bound) MV system tests run.
///
/// Updating the snapshot: set <c>UPDATE_MV_SNAPSHOT=1</c> and re-run. Commit
/// the regenerated <c>MaterializedViewTranslation.snap.txt</c> alongside the
/// change that justified it. Any update should be inspected by hand — silent
/// snapshot churn defeats the point.
/// </summary>
public class MaterializedViewTranslationCharacterizationTests
{
    [Fact]
    public void TranslationOutput_AcrossAllMvContexts_MatchesSnapshot()
    {
        var actual = CaptureAllTranslations();
        var snapPath = SnapshotPath();

        if (Environment.GetEnvironmentVariable("UPDATE_MV_SNAPSHOT") == "1" || !File.Exists(snapPath))
        {
            Directory.CreateDirectory(Path.GetDirectoryName(snapPath)!);
            File.WriteAllText(snapPath, actual);
        }

        var expected = File.ReadAllText(snapPath);
        Assert.Equal(expected, actual);
    }

    private static string CaptureAllTranslations()
    {
        var sb = new StringBuilder();

        var contexts = typeof(MaterializedViewTranslationCharacterizationTests).Assembly
            .GetTypes()
            .Where(t => typeof(DbContext).IsAssignableFrom(t)
                        && !t.IsAbstract
                        && HasMatchingConstructor(t))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToArray();

        var contextsWithMvs = 0;
        foreach (var ctxType in contexts)
        {
            using var ctx = TryCreateContext(ctxType);
            if (ctx is null) continue;

            // ctx.Model triggers OnModelCreating lazily. Some test fixtures
            // intentionally throw there (e.g. validation tests for invalid
            // configurations). Skip those — they're not MV contexts anyway.
            IReadOnlyList<Microsoft.EntityFrameworkCore.Metadata.IEntityType> mvEntities;
            try
            {
                mvEntities = ctx.Model.GetEntityTypes()
                    .Where(e => e.FindAnnotation(ClickHouseAnnotationNames.MaterializedView)?.Value is true)
                    .OrderBy(e => e.Name, StringComparer.Ordinal)
                    .ToList();
            }
            catch
            {
                continue;
            }

            if (mvEntities.Count == 0) continue;
            contextsWithMvs++;

            foreach (var entity in mvEntities)
            {
                var sql = entity.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery)?.Value as string;
                sb.Append(ctxType.Name).Append("::").Append(entity.ClrType.Name).AppendLine(":");
                sb.AppendLine(sql ?? "<null>");
                sb.AppendLine("---");
            }
        }

        // Hard floor: the test assembly defines numerous MV-bearing contexts
        // (MaterializedViewTests alone has 26). If we capture nothing, the
        // sweep is broken — fail loudly rather than silently snapshotting "".
        if (contextsWithMvs == 0)
        {
            var diagnosis = new StringBuilder();
            diagnosis.AppendLine($"Found 0 MV-bearing contexts. Scanned {contexts.Length} candidate DbContext types.");
            foreach (var ctxType in contexts.Take(5))
            {
                _ = TryCreateContext(ctxType, out var reason);
                diagnosis.AppendLine($"  {ctxType.Name}: {reason ?? "ok-but-no-mv"}");
            }
            throw new InvalidOperationException(diagnosis.ToString());
        }

        return sb.ToString();
    }

    private static bool HasMatchingConstructor(Type contextType)
    {
        var optionsType = typeof(DbContextOptions<>).MakeGenericType(contextType);
        return contextType.GetConstructor(new[] { optionsType }) != null;
    }

    internal static DbContext? TryCreateContext(Type contextType, out string? failureReason)
    {
        failureReason = null;
        try
        {
            var optionsBuilderType = typeof(DbContextOptionsBuilder<>).MakeGenericType(contextType);
            var optionsBuilder = (DbContextOptionsBuilder)Activator.CreateInstance(optionsBuilderType)!;

            // Use the non-generic UseClickHouse overload to avoid threading a
            // type parameter through the call.
            optionsBuilder.UseClickHouse("Host=localhost;Database=test");

            // DbContextOptionsBuilder<T> has Options (returns DbContextOptions<T>),
            // and its base also has Options (returns DbContextOptions). Use DeclaredOnly
            // to grab the typed one — the untyped one's options aren't compatible with
            // the typed ctor.
            var optionsProp = optionsBuilderType.GetProperty(
                nameof(DbContextOptionsBuilder<DbContext>.Options),
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)!;
            var options = optionsProp.GetValue(optionsBuilder)!;

            var ctor = contextType.GetConstructor(new[] { options.GetType() });
            if (ctor is null)
            {
                failureReason = $"no ctor accepting {options.GetType().Name}";
                return null;
            }
            return (DbContext?)ctor.Invoke(new[] { options });
        }
        catch (TargetInvocationException tie) when (tie.InnerException is not null)
        {
            failureReason = $"{tie.InnerException.GetType().Name}: {tie.InnerException.Message}";
            return null;
        }
        catch (Exception ex)
        {
            failureReason = $"{ex.GetType().Name}: {ex.Message}";
            return null;
        }
    }

    private static DbContext? TryCreateContext(Type contextType) => TryCreateContext(contextType, out _);

    private static string SnapshotPath()
    {
        // The test assembly runs from bin/<config>/<tfm>/. The snapshot lives
        // alongside the source in tests/EF.CH.Tests/Sql/Snapshots/. Walk up
        // to find the project root, then descend.
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "EF.CH.Tests.csproj")))
            dir = dir.Parent;

        if (dir is null)
            throw new InvalidOperationException("Could not locate EF.CH.Tests project root.");

        return Path.Combine(dir.FullName, "Sql", "Snapshots", "MaterializedViewTranslation.snap.txt");
    }
}
