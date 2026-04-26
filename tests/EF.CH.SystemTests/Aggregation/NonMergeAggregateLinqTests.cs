using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

/// <summary>
/// Coverage for non-merge <c>ClickHouseAggregates.*</c> methods on
/// <c>IGrouping</c> projections. These tests assert the preprocessor keeps
/// aggregate calls intact through EF Core's navigation expansion and SQL
/// translation paths.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class NonMergeAggregateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public NonMergeAggregateLinqTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    private async Task<Ctx> SeedAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(Enumerable.Range(1, 200).Select(i => new Row
        {
            Id = i, Bucket = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C"),
            UserId = i % 40, Value = (i % 11) * 1.5,
        }));
        await ctx.SaveChangesAsync();
        return ctx;
    }

    [Fact]
    public async Task UniqCombined_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, U = g.UniqCombined(r => r.UserId) })
            .ToListAsync();
    }

    [Fact]
    public async Task QuantileTDigest_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, P95 = g.QuantileTDigest(0.95, r => r.Value) })
            .ToListAsync();
    }

    [Fact]
    public async Task TopK_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, Top = g.TopK(3, r => r.UserId) })
            .ToListAsync();
    }

    [Fact]
    public async Task GroupArray_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, All = g.GroupArray(r => r.UserId) })
            .ToListAsync();
    }

    [Fact]
    public async Task ArgMax_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, Arg = g.ArgMax(r => r.UserId, r => r.Value) })
            .ToListAsync();
    }

    [Fact]
    public async Task Median_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, M = g.Median(r => r.Value) })
            .ToListAsync();
    }

    [Fact]
    public async Task Quantiles_OnGrouping_ShouldTranslate()
    {
        await using var ctx = await SeedAsync();
        _ = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new { g.Key, Ps = g.Quantiles(new[] { 0.5, 0.95 }, r => r.Value) })
            .ToListAsync();
    }

    public class Row
    {
        public long Id { get; set; }
        public string Bucket { get; set; } = "";
        public long UserId { get; set; }
        public double Value { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e => { e.ToTable("NonMergeAggregateRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
    }
}
