using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Validates that EF Core aggregate methods (<c>Average</c>, <c>Sum</c>, <c>Min</c>,
/// <c>Max</c>) translate to ClickHouse's <c>*OrNull</c> variants — meaning they
/// return NULL for empty groups instead of throwing or returning 0.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class AggregateOrNullAndQuantileTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public AggregateOrNullAndQuantileTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Aggregates_OverPopulatedGroups_ReturnExpectedScalarValues()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, G = "a", V = 10 },
            new Row { Id = 2, G = "a", V = 20 },
            new Row { Id = 3, G = "b", V = 30 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var grouped = await ctx.Rows
            .GroupBy(r => r.G)
            .Select(g => new
            {
                G = g.Key,
                Avg = (double?)g.Average(x => x.V),
                Sum = (long?)g.Sum(x => (long)x.V),
                Min = (int?)g.Min(x => x.V),
                Max = (int?)g.Max(x => x.V),
                Count = g.Count(),
            })
            .OrderBy(x => x.G)
            .ToListAsync();

        Assert.Equal("a", grouped[0].G);
        Assert.Equal(15.0, grouped[0].Avg);
        Assert.Equal(30L, grouped[0].Sum);
        Assert.Equal(10, grouped[0].Min);
        Assert.Equal(20, grouped[0].Max);
        Assert.Equal(2, grouped[0].Count);

        Assert.Equal("b", grouped[1].G);
        Assert.Equal(30.0, grouped[1].Avg);
        Assert.Equal(30L, grouped[1].Sum);
    }

    [Fact]
    public async Task Average_OnEmptyTable_ReturnsNull()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // No inserts — empty table.
        var avg = await ctx.Rows.Select(r => (double?)r.V).AverageAsync();
        Assert.Null(avg);
    }

    [Fact]
    public async Task Sum_OnEmptyTable_ReturnsNullForNullableProjection()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var sum = await ctx.Rows.Select(r => (long?)r.V).SumAsync();
        // ClickHouse sumOrNull returns NULL on empty; EF should bubble that up as null on a nullable projection.
        Assert.Null(sum);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string G { get; set; } = "";
        public int V { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("AggOrNullCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
