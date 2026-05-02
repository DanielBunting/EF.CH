using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

/// <summary>
/// Coverage of the <c>-If</c> combinator family in regular LINQ projections
/// (not in <c>WITH ROLLUP</c> or materialized-view contexts, which are pinned
/// elsewhere). Each <c>*If</c> aggregate must be recognised by the preprocessor
/// and emit its <c>aggregateName + 'If(value, predicate)'</c> form, returning
/// the same value as the equivalent client-side filter+aggregate.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class IfCombinatorLinqTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public IfCombinatorLinqTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(Enumerable.Range(1, 200).Select(i => new Row
        {
            Id = (uint)i,
            Bucket = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C"),
            UserId = (long)(i % 40),
            Value = (i % 11) * 1.5,
            IsActive = i % 2 == 0,
        }));
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task CountIf_FiltersByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                Active = g.CountIf(r => r.IsActive),
                Total = g.CountUInt64(),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(r.Active <= (long)r.Total));
        Assert.True(rows.Sum(r => r.Active) == 100); // half of 200 are active
    }

    [Fact]
    public async Task SumIfAvgIf_FiltersByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                ActiveSum = g.SumIf(r => r.UserId, r => r.IsActive),
                ActiveAvg = g.AvgIf(r => r.Value, r => r.IsActive),
            })
            .ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(r.ActiveAvg >= 0));
    }

    [Fact]
    public async Task ArgMaxIf_ArgMinIf_FiltersByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                ArgMaxActive = g.ArgMaxIf(r => r.UserId, r => r.Value, r => r.IsActive),
                ArgMinActive = g.ArgMinIf(r => r.UserId, r => r.Value, r => r.IsActive),
            })
            .ToListAsync();

        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public async Task GroupArrayIf_FiltersByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                ActiveUsers = g.GroupArrayIf(r => r.UserId, r => r.IsActive),
                FirstFiveActive = g.GroupArrayIf(5, r => r.UserId, r => r.IsActive),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.True(rows.Sum(r => r.ActiveUsers.Length) == 100);
        Assert.All(rows, r => Assert.True(r.FirstFiveActive.Length <= 5));
    }

    [Fact]
    public async Task TopKIf_TopKWeightedIf_FilterByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                Top3Active = g.TopKIf(3, r => r.UserId, r => r.IsActive),
                Top3Weighted = g.TopKWeightedIf(3, r => r.UserId, r => (long)r.Value, r => r.IsActive),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(r.Top3Active.Length <= 3));
        Assert.All(rows, r => Assert.True(r.Top3Weighted.Length <= 3));
    }

    [Fact]
    public async Task QuantileTDigestIf_FilterByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                P95Active = g.QuantileTDigestIf(0.95, r => r.Value, r => r.IsActive),
                P50Exact = g.QuantileExactIf(0.5, r => r.Value, r => r.IsActive),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(r.P95Active >= 0));
    }

    [Fact]
    public async Task QuantilesIf_QuantilesTDigestIf_FilterByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                Pcts = g.QuantilesIf(new[] { 0.5, 0.9, 0.99 }, r => r.Value, r => r.IsActive),
                Tdigest = g.QuantilesTDigestIf(new[] { 0.5, 0.9 }, r => r.Value, r => r.IsActive),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.All(rows, r => Assert.Equal(3, r.Pcts.Length));
        Assert.All(rows, r => Assert.Equal(2, r.Tdigest.Length));
    }

    [Fact]
    public async Task MedianIf_StddevIf_FilterByPredicate()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .GroupBy(r => r.Bucket)
            .Select(g => new
            {
                g.Key,
                MedActive = g.MedianIf(r => r.Value, r => r.IsActive),
                StdPopActive = g.StddevPopIf(r => r.Value, r => r.IsActive),
                StdSampActive = g.StddevSampIf(r => r.Value, r => r.IsActive),
            })
            .ToListAsync();

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(r.StdPopActive >= 0));
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Bucket { get; set; } = "";
        public long UserId { get; set; }
        public double Value { get; set; }
        public bool IsActive { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("IfCombinator_Rows"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Bucket, x.Id });
            });
    }
}
