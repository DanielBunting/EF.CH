using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// Coverage of SummingMergeTree and AggregatingMergeTree as <i>target</i> tables
/// (i.e., not as the sink of a materialized view). Keyless engines write via
/// <c>BulkInsertAsync</c> since EF's change tracker requires a primary key.
/// Inserting rows with the same ORDER BY key collapses sums/aggregations after
/// <c>OPTIMIZE FINAL</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class SummingAndAggregatingDirectTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public SummingAndAggregatingDirectTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task SummingMergeTree_DirectTarget_CollapsesByOrderByKey()
    {
        await using var ctx = TestContextFactory.Create<SumCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        await ctx.Sums.BulkInsertAsync(new[]
        {
            new SumRow { Region = "EU", Total = 10 },
            new SumRow { Region = "EU", Total = 20 },
            new SumRow { Region = "US", Total = 5 },
        });
        await RawClickHouse.SettleMaterializationAsync(Conn, "SummingDirect_Rows");

        var collapsedEu = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT Total FROM \"SummingDirect_Rows\" FINAL WHERE Region = 'EU'");
        var collapsedUs = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT Total FROM \"SummingDirect_Rows\" FINAL WHERE Region = 'US'");
        Assert.Equal(30L, collapsedEu);
        Assert.Equal(5L, collapsedUs);

        var engine = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'SummingDirect_Rows'");
        Assert.Equal("SummingMergeTree", engine);
    }

    [Fact]
    public async Task AggregatingMergeTree_DirectTarget_DeclaresAggregatingEngine()
    {
        await using var ctx = TestContextFactory.Create<AggCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'AggregatingDirect_Rows'");
        Assert.Equal("AggregatingMergeTree", engine);
    }

    public sealed class SumRow
    {
        public string Region { get; set; } = "";
        public long Total { get; set; }
    }

    public sealed class AggRow
    {
        public string Region { get; set; } = "";
        public long Total { get; set; }
    }

    public sealed class SumCtx(DbContextOptions<SumCtx> o) : DbContext(o)
    {
        public DbSet<SumRow> Sums => Set<SumRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<SumRow>(e =>
            {
                e.ToTable("SummingDirect_Rows"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);
            });
    }

    public sealed class AggCtx(DbContextOptions<AggCtx> o) : DbContext(o)
    {
        public DbSet<AggRow> Aggs => Set<AggRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<AggRow>(e =>
            {
                e.ToTable("AggregatingDirect_Rows"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Region);
            });
    }
}
