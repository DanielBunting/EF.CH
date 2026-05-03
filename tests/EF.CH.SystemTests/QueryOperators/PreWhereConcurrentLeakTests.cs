using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// End-to-end demonstration of the PreWhere [ThreadStatic] leak: 64 parallel
/// async queries each filtering on a distinct bucket. The bug is that the
/// PREWHERE predicate captured at translation time can be lost between the
/// postprocessor (writes thread-local state on Thread A) and the SQL
/// generator (reads it on Thread B). When the predicate is lost, the
/// emitted SQL has no PREWHERE clause and the query returns rows from every
/// bucket instead of just the requested one.
///
/// <para>
/// These tests are <b>currently broken</b> — they fail today, demonstrating
/// the bug. They must pass after the fix described in
/// <c>plans/prewhere-state-leak-fix.md</c> lands. The earlier
/// <see cref="ConcurrentOperatorIsolationTests"/> had to drop PreWhere from
/// its parallel mix because of this leak; this class is the focused
/// regression pin that lives until the fix lands and stays after it.
/// </para>
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class PreWhereConcurrentLeakTests
{
    private const int BucketCount = 16;
    private const int RowsPerBucket = 5;

    private readonly SingleNodeClickHouseFixture _fx;
    public PreWhereConcurrentLeakTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task ParallelPreWhereQueries_OnlyReturnRowsMatchingTheirPredicate()
    {
        await SeedAsync();

        var failures = new System.Collections.Concurrent.ConcurrentBag<string>();

        // Run several rounds. The leak is non-deterministic per round (depends on
        // EF Core's compiled-query cache miss timing across parallel threads), so
        // a single round can pass spuriously. Five rounds make a false-pass
        // exceedingly unlikely while keeping the test under a few seconds.
        for (var round = 0; round < 5; round++)
        {
            await Parallel.ForEachAsync(
                Enumerable.Range(0, BucketCount * 4),
                new ParallelOptions { MaxDegreeOfParallelism = BucketCount },
                async (taskId, ct) =>
                {
                    var bucket = $"b{taskId % BucketCount:D2}";
                    await using var ctx = TestContextFactory.Create<Ctx>(Conn);

                    var rows = await ctx.Items
                        .PreWhere(x => x.Bucket == bucket)
                        .ToListAsync(ct);

                    if (rows.Count != RowsPerBucket)
                    {
                        failures.Add(
                            $"round {round} task {taskId} (bucket={bucket}): expected {RowsPerBucket} rows, got {rows.Count}. " +
                            $"distinct buckets returned: {string.Join(",", rows.Select(r => r.Bucket).Distinct())}");
                        return;
                    }

                    if (rows.Any(r => r.Bucket != bucket))
                    {
                        var observed = string.Join(",", rows.Select(r => r.Bucket).Distinct());
                        failures.Add($"round {round} task {taskId} (bucket={bucket}): predicate leak — got buckets [{observed}]");
                    }
                });
        }

        Assert.True(failures.IsEmpty,
            "PreWhere predicate leaked across parallel async queries:\n" +
            string.Join("\n", failures.Take(10)));
    }

    [Fact]
    public async Task SequentialPreWhereWithYields_PreservesEachPredicate()
    {
        // Different angle: a single task that issues several PreWhere queries
        // back-to-back with `Task.Yield()` between them. Each query's predicate
        // must reach the SQL it produces. Under [ThreadStatic] storage and
        // continuation-driven thread hops the second query can see no state
        // (or the first query's stale state).
        await SeedAsync();
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);

        for (var b = 0; b < BucketCount; b++)
        {
            var bucket = $"b{b:D2}";
            await Task.Yield();
            var rows = await ctx.Items.PreWhere(x => x.Bucket == bucket).ToListAsync();
            Assert.Equal(RowsPerBucket, rows.Count);
            Assert.All(rows, r => Assert.Equal(bucket, r.Bucket));
        }
    }

    private async Task SeedAsync()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        for (var b = 0; b < BucketCount; b++)
        {
            for (var i = 0; i < RowsPerBucket; i++)
            {
                ctx.Items.Add(new Item
                {
                    Id = (uint)(b * RowsPerBucket + i + 1),
                    Bucket = $"b{b:D2}",
                    Value = i,
                });
            }
        }
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
    }

    public sealed class Item
    {
        public uint Id { get; set; }
        public string Bucket { get; set; } = "";
        public int Value { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Item>(e =>
            {
                e.ToTable("PreWhereLeak_Items");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Bucket, x.Id });
            });
    }
}
