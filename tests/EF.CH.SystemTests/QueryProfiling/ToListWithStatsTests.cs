using EF.CH.Extensions;
using EF.CH.QueryProfiling;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryProfiling;

/// <summary>
/// Coverage of <c>ToListWithStatsAsync</c>: the result list mirrors a plain
/// <c>ToListAsync</c>, and the <see cref="QueryStatistics"/> object is populated.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ToListWithStatsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ToListWithStatsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task ToListWithStats_ResultsMatchPlainToList_AndStatsAreReported()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 50; i++) ctx.Rows.Add(new Row { Id = i, V = (int)i });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var plain = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        var withStats = await ctx.Rows.OrderBy(r => r.Id).ToListWithStatsAsync(ctx);

        Assert.Equal(plain.Count, withStats.Results.Count);
        Assert.True(withStats.Elapsed > TimeSpan.Zero);
        Assert.NotNull(withStats.Statistics);
    }

    [Fact]
    public async Task ToListWithStats_StatisticsCarryRowsBytesAndDuration()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 100; i++) ctx.Rows.Add(new Row { Id = i, V = (int)i });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var withStats = await ctx.Rows.ToListWithStatsAsync(ctx);

        Assert.NotNull(withStats.Statistics);
        // X-ClickHouse-Summary should report the rows we just scanned.
        Assert.True(withStats.Statistics!.RowsRead >= 100,
            $"Expected at least 100 rows read, got {withStats.Statistics.RowsRead}");
        Assert.True(withStats.Statistics.BytesRead > 0,
            $"Expected non-zero bytes read, got {withStats.Statistics.BytesRead}");
        Assert.True(withStats.Statistics.QueryDurationMs >= 0,
            $"Expected non-negative duration, got {withStats.Statistics.QueryDurationMs}");
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ToListWithStats_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
