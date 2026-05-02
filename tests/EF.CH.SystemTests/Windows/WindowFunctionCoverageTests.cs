using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Windows;

/// <summary>
/// Surface coverage of <see cref="Window"/> helpers that don't have a
/// dedicated test in <c>WindowFrameTests</c> / <c>NamedWindowTests</c>:
/// <c>Lead</c>, <c>FirstValue</c>, <c>LastValue</c>, aggregate window
/// expressions, and the rank family. The translator wraps the UInt64-returning
/// rank/count/row_number functions with <c>toInt64</c> so projecting them as
/// <c>long</c> doesn't trip <c>InvalidCastException</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class WindowFunctionCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public WindowFunctionCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // a-group has a tied value so Rank vs DenseRank diverge.
        ctx.Rows.AddRange(
            new Row { Id = 1, G = "a", V = 10 },
            new Row { Id = 2, G = "a", V = 10 },
            new Row { Id = 3, G = "a", V = 30 },
            new Row { Id = 4, G = "a", V = 40 },
            new Row { Id = 5, G = "b", V = 100 },
            new Row { Id = 6, G = "b", V = 200 },
            new Row { Id = 7, G = "b", V = 300 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task Rank_DenseRank_PercentRank_ReflectTies()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            r.G,
            Rank = Window.Rank(w => w.PartitionBy(r.G).OrderBy(r.V)),
            Dense = Window.DenseRank(w => w.PartitionBy(r.G).OrderBy(r.V)),
            Pct = Window.PercentRank(w => w.PartitionBy(r.G).OrderBy(r.V)),
        }).OrderBy(x => x.Id).ToListAsync();

        // a-group: [10, 10, 30, 40] → Rank 1,1,3,4   Dense 1,1,2,3
        var a = rows.Where(r => r.G == "a").ToList();
        Assert.Equal(new long[] { 1, 1, 3, 4 }, a.Select(x => x.Rank));
        Assert.Equal(new long[] { 1, 1, 2, 3 }, a.Select(x => x.Dense));
        Assert.Equal(0.0, a[0].Pct, 6);
    }

    [Fact]
    public async Task Lead_PullsNextRow()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Where(r => r.G == "b").Select(r => new
        {
            r.Id,
            Next = Window.Lead<int>(r.V, 1, -1, w => w.PartitionBy(r.G).OrderBy(r.Id)),
            NextTwo = Window.Lead<int>(r.V, 2, -1, w => w.PartitionBy(r.G).OrderBy(r.Id)),
        }).OrderBy(x => x.Id).ToListAsync();

        // b-group: 100, 200, 300 — Lead(1) → 200, 300, default(-1)
        Assert.Equal(new[] { 200, 300, -1 }, rows.Select(r => r.Next));
        Assert.Equal(new[] { 300, -1, -1 }, rows.Select(r => r.NextTwo));
    }

    [Fact]
    public async Task FirstValue_LastValue_OverPartition()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Where(r => r.G == "b").Select(r => new
        {
            r.Id,
            First = Window.FirstValue<int>(r.V,
                w => w.PartitionBy(r.G).OrderBy(r.Id).Rows().UnboundedPreceding().UnboundedFollowing()),
            Last = Window.LastValue<int>(r.V,
                w => w.PartitionBy(r.G).OrderBy(r.Id).Rows().UnboundedPreceding().UnboundedFollowing()),
        }).OrderBy(x => x.Id).ToListAsync();

        // Across the full partition [b: 100,200,300] every row should see First=100, Last=300.
        Assert.All(rows, r => Assert.Equal(100, r.First));
        Assert.All(rows, r => Assert.Equal(300, r.Last));
    }

    [Fact]
    public async Task Aggregate_Avg_Min_Max_Count_OverWindow()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Where(r => r.G == "b").Select(r => new
        {
            r.Id,
            Avg = Window.Avg<int>(r.V, w => w.PartitionBy(r.G)),
            Min = Window.Min<int>(r.V, w => w.PartitionBy(r.G)),
            Max = Window.Max<int>(r.V, w => w.PartitionBy(r.G)),
            Cnt = Window.Count<int>(r.V, w => w.PartitionBy(r.G)),
        }).ToListAsync();

        // b-group: 100/200/300 → avg 200, min 100, max 300, count 3 (constant per partition).
        Assert.All(rows, r => Assert.Equal(200.0, r.Avg!.Value, 6));
        Assert.All(rows, r => Assert.Equal(100, r.Min));
        Assert.All(rows, r => Assert.Equal(300, r.Max));
        Assert.All(rows, r => Assert.Equal(3L, r.Cnt));
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
                e.ToTable("WindowCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
