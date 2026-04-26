using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Windows;

/// <summary>
/// Coverage of multiple window functions in one projection (Lag, RowNumber, Sum)
/// — exercises the SQL generator's named-window de-duplication path if any.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class NamedWindowTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public NamedWindowTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MultipleWindowFunctions_OnSameSpec_ProduceCorrectValues()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, G = "x", V = 10 },
            new Row { Id = 2, G = "x", V = 20 },
            new Row { Id = 3, G = "x", V = 30 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.Select(r => new
        {
            r.Id,
            Rn = Window.RowNumber(w => w.PartitionBy(r.G).OrderBy(r.Id)),
            Lagged = Window.Lag<int>(r.V, 1, 0, w => w.PartitionBy(r.G).OrderBy(r.Id)),
            Total = Window.Sum<long>(r.V, w => w.PartitionBy(r.G).OrderBy(r.Id))
        }).OrderBy(r => r.Id).ToListAsync();

        Assert.Equal(1L, rows[0].Rn);
        Assert.Equal(0,  rows[0].Lagged);
        Assert.Equal(10L, rows[0].Total);
        Assert.Equal(10, rows[1].Lagged);
        Assert.Equal(20, rows[2].Lagged);
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
                e.ToTable("NamedWindow_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
