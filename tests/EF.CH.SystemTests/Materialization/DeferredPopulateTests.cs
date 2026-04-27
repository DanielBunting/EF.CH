using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Deferred creation of a POPULATE materialised view — create source, seed
/// data, <em>then</em> attach the MV so POPULATE backfills from the existing
/// rows.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class DeferredPopulateTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public DeferredPopulateTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public async Task CreateMaterializedViewAsync_WithPopulate_BackfillsExistingSourceRows()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sales.AddRange(
            new Sale { Id = 1, Region = "eu", Amount = 10 },
            new Sale { Id = 2, Region = "eu", Amount = 20 },
            new Sale { Id = 3, Region = "us", Amount = 15 });
        await ctx.SaveChangesAsync();

        await ctx.Database.CreateMaterializedViewAsync<RegionSummary>(o => o.WithPopulate());
        await RawClickHouse.SettleMaterializationAsync(_fixture.ConnectionString, "RegionSummary");

        var rows = await RawClickHouse.RowsAsync(_fixture.ConnectionString,
            "SELECT Region, toFloat64(Total) AS Total FROM \"RegionSummary\" FINAL ORDER BY Region");
        Assert.Equal(2, rows.Count);
        Assert.Equal("eu", (string)rows[0]["Region"]!);
        Assert.Equal(30.0, Convert.ToDouble(rows[0]["Total"]), 3);
        Assert.Equal("us", (string)rows[1]["Region"]!);
        Assert.Equal(15.0, Convert.ToDouble(rows[1]["Total"]), 3);
    }

    public class Sale { public long Id { get; set; } public string Region { get; set; } = ""; public double Amount { get; set; } }
    public class RegionSummary { public string Region { get; set; } = ""; public double Total { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Sale> Sales => Set<Sale>();
        public DbSet<RegionSummary> Summaries => Set<RegionSummary>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Sale>(e => { e.ToTable("Sales"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<RegionSummary>(e =>
            {
                e.ToTable("RegionSummary"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Region);


            });
            mb.MaterializedView<RegionSummary>().From<Sale>().DefinedAs(src => src
                    .GroupBy(s => s.Region)
                    .Select(g => new RegionSummary { Region = g.Key, Total = g.Sum(s => s.Amount) }));
            // TODO: .Deferred() — chain .Deferred() onto the corresponding MaterializedView<RegionSummary>() chain.
        }
    }
}
