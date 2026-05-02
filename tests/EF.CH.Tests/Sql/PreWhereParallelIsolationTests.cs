using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Pins thread-isolation of the PREWHERE predicate captured by
/// <c>ClickHouseQueryTranslationPreprocessor</c> + the SQL translator's
/// <c>TranslatePreWhereMarker</c>.
///
/// <para>
/// Today's mechanism stores the captured predicate in a
/// <c>[ThreadStatic]</c> field on <c>ClickHouseQuerySqlGenerator</c> via the
/// postprocessor's <c>SetPreWhereExpression</c>. The SQL generator reads the
/// field in <c>VisitSelect</c>. When EF Core's pipeline crosses threads
/// between those two phases — which can happen under parallel
/// async-compilation pressure — the read sees a different thread's
/// (or no) state, and the emitted SQL silently omits the PREWHERE clause.
/// </para>
///
/// <para>
/// These tests are <b>currently broken</b>: they reproduce the leak. They
/// must pass after the fix described in
/// <c>plans/prewhere-state-leak-fix.md</c> lands. Each iteration uses a
/// distinct literal predicate so cross-contamination is observable as the
/// other iteration's literal appearing in the emitted SQL.
/// </para>
/// </summary>
public class PreWhereParallelIsolationTests
{
    [Fact]
    public async Task ParallelQueries_WithDifferentPreWherePredicates_DoNotCrossContaminate()
    {
        const int iterations = 64;
        var failures = new System.Collections.Concurrent.ConcurrentBag<string>();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, iterations),
            new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 2 },
            async (i, _) =>
            {
                using var context = CreateContext();
                // Distinct integer-typed sentinel per iteration. Use an integer literal
                // (not a captured local) so the value is part of the LINQ expression
                // shape itself, not parameterized — the cross-contamination would then
                // show up directly in the SQL string.
                var sentinel = 100_000 + i;
                var sql = context.LeakTestEntities
                    .PreWhere(e => e.Value == sentinel)
                    .ToQueryString();

                if (!sql.Contains("PREWHERE", StringComparison.OrdinalIgnoreCase))
                    failures.Add($"iter {i}: PREWHERE clause missing — sql: {Trim(sql)}");

                if (!sql.Contains(sentinel.ToString(), StringComparison.Ordinal))
                    failures.Add($"iter {i}: PREWHERE not filtering on {sentinel} — sql: {Trim(sql)}");

                // Any other iteration's sentinel must not appear in this iteration's SQL.
                // Sentinels are 6-digit (100_000+) so no value is a substring of another.
                for (var other = 0; other < iterations; other++)
                {
                    if (other == i) continue;
                    var leaked = 100_000 + other;
                    if (sql.Contains(leaked.ToString(), StringComparison.Ordinal))
                        failures.Add($"iter {i}: leaked sentinel {leaked} (from iter {other})");
                }

                await Task.Yield();
            });

        Assert.True(failures.IsEmpty,
            "PreWhere predicate cross-contaminated across parallel queries:\n" +
            string.Join("\n", failures.Take(10)));
    }

    [Fact]
    public async Task PreWhere_AfterTaskYield_PreservesPredicate()
    {
        // Forces a continuation that (under contention) lands on a different
        // ThreadPool worker than the synchronous prefix. If PreWhere's state
        // is `[ThreadStatic]`, the post-yield SQL generation may run on a
        // thread where the predicate was never written.
        using var context = CreateContext();

        var prefix = context.LeakTestEntities.PreWhere(e => e.Value == 42);
        await Task.Yield();
        var sql = prefix.ToQueryString();

        Assert.Contains("PREWHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("42", sql, StringComparison.Ordinal);
    }

    private static LeakTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LeakTestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new LeakTestDbContext(options);
    }

    private static string Trim(string sql) => sql.Length <= 240 ? sql : sql[..240] + "…";
}
