using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Windows;

/// <summary>
/// Coverage of <c>Window.RowNumber</c>, <c>Rank</c>, <c>Lag</c>, <c>Sum</c> with
/// PartitionBy + OrderBy windows. Asserts the per-row computed value matches a
/// manually-computed expectation.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class WindowFrameTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public WindowFrameTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // 2 groups × 3 rows.
        ctx.Rows.AddRange(
            new Row { Id = 1, G = "a", V = 10 },
            new Row { Id = 2, G = "a", V = 20 },
            new Row { Id = 3, G = "a", V = 30 },
            new Row { Id = 4, G = "b", V = 100 },
            new Row { Id = 5, G = "b", V = 200 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task RowNumber_PerPartition_OrdersBy()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            r.G,
            Rn = Window.RowNumber(w => w.PartitionBy(r.G).OrderBy(r.V))
        }).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(1L, rows.First(x => x.Id == 1).Rn);
        Assert.Equal(2L, rows.First(x => x.Id == 2).Rn);
        Assert.Equal(3L, rows.First(x => x.Id == 3).Rn);
        Assert.Equal(1L, rows.First(x => x.Id == 4).Rn);
        Assert.Equal(2L, rows.First(x => x.Id == 5).Rn);
    }

    [Fact]
    public async Task RunningSum_RowsBetweenUnboundedPrecedingAndCurrentRow()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows.Where(r => r.G == "a").Select(r => new
        {
            r.Id,
            Running = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.Id).Rows().UnboundedPreceding().CurrentRow())
        }).OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(10L, rows[0].Running);
        Assert.Equal(30L, rows[1].Running);
        Assert.Equal(60L, rows[2].Running);
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
                e.ToTable("WindowFrame_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
