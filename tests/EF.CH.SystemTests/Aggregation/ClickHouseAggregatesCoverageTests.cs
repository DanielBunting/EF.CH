using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

/// <summary>
/// Verifies that every fluent <see cref="ClickHouseAggregates"/> method exposed
/// on an <see cref="IGrouping{TKey,T}"/> can be used directly in a LINQ
/// projection — i.e. the preprocessor recognises it and rewrites it into a
/// surrogate aggregate that the SQL translator unwraps. Adjacent to the
/// QuantileDD / TopKWeighted bugs already fixed: the same pattern can break
/// for any ClickHouseAggregates entry that has no <c>MergeRewrites</c>
/// dictionary entry.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ClickHouseAggregatesCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ClickHouseAggregatesCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<AggCtx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<AggCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var rng = new Random(7);
        var rows = Enumerable.Range(0, 200).Select(i => new AggRow
        {
            Id = (uint)(i + 1),
            G = i % 3 == 0 ? "a" : i % 3 == 1 ? "b" : "c",
            V = rng.NextDouble() * 100,
            U = (ulong)rng.Next(1, 50),
            Tag = $"t{i % 7}",
        }).ToList();
        ctx.Rows.AddRange(rows);
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task UniqFamily_AllVariantsTranslate()
    {
        await using var ctx = await SeededAsync();

        var q = await ctx.Rows
            .GroupBy(r => r.G)
            .Select(g => new
            {
                g.Key,
                Uniq = g.Uniq(x => x.U),
                UniqExact = g.UniqExact(x => x.U),
                UniqHLL12 = g.UniqHLL12(x => x.U),
                UniqCombined = g.UniqCombined(x => x.U),
                UniqCombined64 = g.UniqCombined64(x => x.U),
                UniqTheta = g.UniqTheta(x => x.U),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.Equal(3, q.Count);
        Assert.All(q, row => Assert.True(row.Uniq > 0));
        Assert.All(q, row => Assert.True(row.UniqExact > 0));
    }

    [Fact]
    public async Task QuantileFamily_AllVariantsTranslate()
    {
        await using var ctx = await SeededAsync();

        var q = await ctx.Rows
            .GroupBy(r => r.G)
            .Select(g => new
            {
                g.Key,
                Q = g.Quantile(0.5, x => x.V),
                Tdigest = g.QuantileTDigest(0.95, x => x.V),
                Exact = g.QuantileExact(0.5, x => x.V),
                Timing = g.QuantileTiming(0.99, x => x.V),
                DD = g.QuantileDD(0.01, 0.95, x => x.V),
                Multi = g.Quantiles(new[] { 0.5, 0.9, 0.99 }, x => x.V),
                MultiTd = g.QuantilesTDigest(new[] { 0.5, 0.9, 0.99 }, x => x.V),
            })
            .OrderBy(x => x.Key).ToListAsync();

        Assert.Equal(3, q.Count);
        Assert.All(q, row => Assert.Equal(3, row.Multi.Length));
        Assert.All(q, row => Assert.Equal(3, row.MultiTd.Length));
    }

    [Fact]
    public async Task StatisticalFamily_AllVariantsTranslate()
    {
        await using var ctx = await SeededAsync();

        var q = await ctx.Rows
            .GroupBy(r => r.G)
            .Select(g => new
            {
                g.Key,
                Median = g.Median(x => x.V),
                StddevPop = g.StddevPop(x => x.V),
                StddevSamp = g.StddevSamp(x => x.V),
                VarPop = g.VarPop(x => x.V),
                VarSamp = g.VarSamp(x => x.V),
            })
            .ToListAsync();

        Assert.Equal(3, q.Count);
        Assert.All(q, row => Assert.True(row.StddevPop >= 0));
    }

    [Fact]
    public async Task AnyFamily_AllVariantsTranslate()
    {
        await using var ctx = await SeededAsync();

        var q = await ctx.Rows
            .GroupBy(r => r.G)
            .Select(g => new
            {
                g.Key,
                AnyU = g.AnyValue(x => x.U),
                AnyLastU = g.AnyLastValue(x => x.U),
                ArgMaxByV = g.ArgMax(x => x.U, x => x.V),
                ArgMinByV = g.ArgMin(x => x.U, x => x.V),
            })
            .ToListAsync();

        Assert.Equal(3, q.Count);
        Assert.All(q, row => Assert.True(row.AnyU > 0));
    }

    [Fact]
    public async Task ArrayFamily_AllVariantsTranslate()
    {
        await using var ctx = await SeededAsync();

        var q = await ctx.Rows
            .GroupBy(r => r.G)
            .Select(g => new
            {
                g.Key,
                Top3 = g.TopK(3, x => x.Tag),
                Weighted = g.TopKWeighted(3, x => x.Tag, x => x.U),
                Tags = g.GroupArray(x => x.Tag),
                CappedTags = g.GroupArray(5, x => x.Tag),
                UniqueTags = g.GroupUniqArray(x => x.Tag),
                CappedUnique = g.GroupUniqArray(5, x => x.Tag),
            })
            .ToListAsync();

        Assert.Equal(3, q.Count);
        Assert.All(q, row => Assert.NotEmpty(row.Top3));
        Assert.All(q, row => Assert.True(row.CappedTags.Length <= 5));
        Assert.All(q, row => Assert.True(row.CappedUnique.Length <= 5));
    }

    public sealed class AggRow
    {
        public uint Id { get; set; }
        public string G { get; set; } = "";
        public double V { get; set; }
        public ulong U { get; set; }
        public string Tag { get; set; } = "";
    }

    public sealed class AggCtx(DbContextOptions<AggCtx> o) : DbContext(o)
    {
        public DbSet<AggRow> Rows => Set<AggRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<AggRow>(e =>
            {
                e.ToTable("AggCoverageRows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
