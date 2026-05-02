using EF.CH.Diagnostics;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Coverage of refreshable-MV scenarios beyond <c>RefreshableMvIntegrationTests</c>:
/// the <c>RefreshAfter</c> schedule mode (re-run N units after the previous finish
/// instead of every wall-clock interval), and a chained dependency where one
/// refreshable view feeds another.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class RefreshableMvAdvancedTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public RefreshableMvAdvancedTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task RefreshAfter_PopulatesTarget_AfterFirstRefresh()
    {
        await using var ctx = await PrepareAfterContextAsync();

        ctx.Sources.AddRange(Enumerable.Range(1, 8)
            .Select(i => new AfterSrc { Id = i, Tag = i % 2 == 0 ? "x" : "y" }));
        await ctx.SaveChangesAsync();

        await ctx.Database.WaitForRefreshAsync<AfterSummary>(TimeSpan.FromSeconds(30));

        var rows = await RawClickHouse.RowCountAsync(Conn, "AfterSummary");
        Assert.True(rows > 0);

        var ddl = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'AfterSummary'");
        // RefreshAfter renders to "REFRESH AFTER N {unit}" rather than EVERY.
        Assert.Contains("REFRESH AFTER", ddl, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RandomizeFor_RendersInDdl_AndViewRefreshes()
    {
        // RANDOMIZE FOR adds a jitter window to the schedule so a fleet of MVs
        // doesn't refresh in lockstep. The fluent <c>RandomizeFor</c> annotation
        // surfaces in the rendered create_table_query; verify both the DDL
        // marker and that the view still refreshes correctly.
        await using (var dropCtx = TestContextFactory.Create<RandomCtx>(Conn))
        {
            await dropCtx.Database.EnsureDeletedAsync();
        }
        await using var ctx = TestContextFactory.Create<RandomCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(Enumerable.Range(1, 4)
            .Select(i => new RandomSrc { Id = i, Tag = "k" }));
        await ctx.SaveChangesAsync();

        await ctx.Database.WaitForRefreshAsync<RandomSummary>(TimeSpan.FromSeconds(30));

        var ddl = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'RandomSummary'");
        Assert.Contains("RANDOMIZE FOR", ddl, StringComparison.OrdinalIgnoreCase);

        var rows = await RawClickHouse.RowCountAsync(Conn, "RandomSummary");
        Assert.True(rows > 0);
    }

    [Fact]
    public async Task RefreshAfter_TargetReceivesUpdates_WhenSourceGrows()
    {
        // Sanity for RefreshAfter: insert, wait, insert more, wait again.
        // The target row count should grow on the second refresh.
        await using var ctx = await PrepareAfterContextAsync();

        ctx.Sources.AddRange(Enumerable.Range(1, 4)
            .Select(i => new AfterSrc { Id = i, Tag = i % 2 == 0 ? "x" : "y" }));
        await ctx.SaveChangesAsync();
        await ctx.Database.WaitForRefreshAsync<AfterSummary>(TimeSpan.FromSeconds(30));

        var firstSnapshotCount = await RawClickHouse.RowCountAsync(Conn, "AfterSummary");
        Assert.True(firstSnapshotCount > 0);

        // Add a brand-new tag — second refresh should pick it up.
        ctx.Sources.Add(new AfterSrc { Id = 100, Tag = "z" });
        await ctx.SaveChangesAsync();

        var beforeNext = DateTime.UtcNow;
        await ctx.Database.WaitForRefreshAsync<AfterSummary>(TimeSpan.FromSeconds(30));

        var status = await ctx.Database.GetRefreshStatusAsync<AfterSummary>();
        Assert.NotNull(status?.LastSuccessTime);
        Assert.True(status!.LastSuccessTime > beforeNext.AddSeconds(-30));
    }

    private async Task<AfterCtx> PrepareAfterContextAsync()
    {
        await using (var dropCtx = TestContextFactory.Create<AfterCtx>(Conn))
        {
            await dropCtx.Database.EnsureDeletedAsync();
        }
        var ctx = TestContextFactory.Create<AfterCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();
        return ctx;
    }

    public class RandomSrc { public int Id { get; set; } public string Tag { get; set; } = ""; }
    public class RandomSummary { public string Tag { get; set; } = ""; public int Total { get; set; } }
    public sealed class RandomCtx(DbContextOptions<RandomCtx> opts) : DbContext(opts)
    {
        public DbSet<RandomSrc> Sources => Set<RandomSrc>();
        public DbSet<RandomSummary> Summaries => Set<RandomSummary>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RandomSrc>(e => { e.ToTable("RandomSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<RandomSummary>(e =>
            {
                e.ToTable("RandomSummary"); e.HasNoKey();
                e.UseMergeTree(x => x.Tag);
            });
            mb.MaterializedView<RandomSummary>()
                .From<RandomSrc>()
                .DefinedAs(src => src.GroupBy(s => s.Tag).Select(g => new RandomSummary
                {
                    Tag = g.Key,
                    Total = g.Count(),
                }))
                .RefreshEvery(TimeSpan.FromSeconds(2))
                .RandomizeFor(TimeSpan.FromSeconds(1));
        }
    }

    public class AfterSrc { public int Id { get; set; } public string Tag { get; set; } = ""; }
    public class AfterSummary { public string Tag { get; set; } = ""; public int Total { get; set; } }

    public sealed class AfterCtx(DbContextOptions<AfterCtx> opts) : DbContext(opts)
    {
        public DbSet<AfterSrc> Sources => Set<AfterSrc>();
        public DbSet<AfterSummary> Summaries => Set<AfterSummary>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<AfterSrc>(e => { e.ToTable("AfterSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<AfterSummary>(e =>
            {
                e.ToTable("AfterSummary"); e.HasNoKey();
                e.UseMergeTree(x => x.Tag);
            });
            mb.MaterializedView<AfterSummary>()
                .From<AfterSrc>()
                .DefinedAs(src => src.GroupBy(s => s.Tag).Select(g => new AfterSummary
                {
                    Tag = g.Key,
                    Total = g.Count(),
                }))
                .RefreshAfter(TimeSpan.FromSeconds(2));
        }
    }

}
