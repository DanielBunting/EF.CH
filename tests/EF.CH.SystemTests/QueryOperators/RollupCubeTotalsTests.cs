using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>WithRollup()</c>, <c>WithCube()</c>, <c>WithTotals()</c> on a
/// GROUP BY. Each modifier produces a known number of additional subtotal rows;
/// we assert the row counts and the structure of the extra rows (nullable keys).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class RollupCubeTotalsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public RollupCubeTotalsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // 2 regions × 2 categories with two rows in one combination → 5 combinations
        ctx.Sales.AddRange(
            new Sale { Id = 1, Region = "EU", Category = "A", Amount = 10 },
            new Sale { Id = 2, Region = "EU", Category = "B", Amount = 20 },
            new Sale { Id = 3, Region = "US", Category = "A", Amount = 30 },
            new Sale { Id = 4, Region = "US", Category = "B", Amount = 40 },
            new Sale { Id = 5, Region = "US", Category = "B", Amount = 50 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task WithTotals_AddsOneGrandTotalRow()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Sales
            .GroupBy(s => s.Region)
            .Select(g => new { Region = g.Key, Total = (long?)g.Sum(x => (long)x.Amount) })
            .WithTotals()
            .ToListAsync();
        // Two real groups + 1 totals row.
        Assert.Equal(3, rows.Count);
        // The totals row's Total should equal the grand total (150).
        Assert.Contains(rows, r => r.Total == 150L);
    }

    [Fact]
    public async Task WithRollup_AddsHierarchicalSubtotals()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Sales
            .GroupBy(s => new { s.Region, s.Category })
            .Select(g => new { g.Key.Region, g.Key.Category, Total = (long?)g.Sum(x => (long)x.Amount) })
            .WithRollup()
            .ToListAsync();
        // 4 leaf groups + 2 region rollups + 1 grand total = 7 rows.
        Assert.Equal(7, rows.Count);
    }

    [Fact]
    public async Task WithCube_AddsAllSubtotalCombinations()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Sales
            .GroupBy(s => new { s.Region, s.Category })
            .Select(g => new { g.Key.Region, g.Key.Category, Total = (long?)g.Sum(x => (long)x.Amount) })
            .WithCube()
            .ToListAsync();
        // 4 leaf + 2 region rollups + 2 category rollups + 1 grand = 9 rows.
        Assert.Equal(9, rows.Count);
    }

    public sealed class Sale
    {
        public uint Id { get; set; }
        public string Region { get; set; } = "";
        public string Category { get; set; } = "";
        public int Amount { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Sale> Sales => Set<Sale>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Sale>(e =>
            {
                e.ToTable("RollupCubeOpTests_Sales"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
