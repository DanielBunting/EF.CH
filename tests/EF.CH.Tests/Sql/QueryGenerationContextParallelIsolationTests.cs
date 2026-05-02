using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Pins thread-isolation of the per-feature
/// <c>ClickHouseQueryGenerationContext</c> on
/// <c>ClickHouseQuerySqlGenerator</c>. The state today lives in
/// <c>[ThreadStatic]</c> storage; concurrent queries on different threads
/// must each see only the settings they themselves applied. If this ever
/// regresses (e.g. the storage flips to a static field), this test will
/// fail loudly instead of silently cross-contaminating SQL.
/// </summary>
public class QueryGenerationContextParallelIsolationTests
{
    [Fact]
    public async Task ParallelQueries_WithDifferentSettings_DoNotCrossContaminate()
    {
        const int iterations = 64;
        var failures = new System.Collections.Concurrent.ConcurrentBag<string>();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, iterations),
            new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 },
            async (i, _) =>
            {
                using var context = CreateContext();
                var sql = context.LeakTestEntities
                    .WithSetting("max_threads", 100 + i)
                    .Where(e => e.Value > 0)
                    .Take(10)
                    .ToQueryString();

                var expected = 100 + i;
                if (!sql.Contains($"max_threads = {expected}", StringComparison.Ordinal))
                    failures.Add($"iter {i}: expected max_threads={expected}, got: {sql}");

                // Any other iteration's max_threads value must not appear.
                // Values are 100..(100+iterations-1) and uniformly 3 digits, so
                // no value is a proper prefix of another — substring search is safe.
                for (var other = 0; other < iterations; other++)
                {
                    if (other == i) continue;
                    var leaked = 100 + other;
                    if (sql.Contains($"max_threads = {leaked}", StringComparison.Ordinal))
                        failures.Add($"iter {i}: leaked max_threads={leaked}");
                }

                await Task.Yield();
            });

        Assert.Empty(failures);
    }

    [Fact]
    public void NestedToQueryString_OuterSettingsSurvive_InnerCompilation()
    {
        using var context = CreateContext();

        // Force a nested compile: build an inner queryable, project a Contains
        // against it from an outer WithSettings query. EF Core compiles the
        // outer query as a single unit, but the post-processor pass touches
        // multiple layers — pin that the outer's WithSettings makes it through.
        var innerNames = new[] { "a", "b", "c" };

        var sql = context.LeakTestEntities
            .WithSetting("max_threads", 8)
            .Where(e => innerNames.Contains(e.Name))
            .ToQueryString();

        Assert.Contains("max_threads", sql);
        Assert.Contains("8", sql);
    }

    private static LeakTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LeakTestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new LeakTestDbContext(options);
    }
}
