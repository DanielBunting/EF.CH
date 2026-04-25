using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using EF.CH.TempTable;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.TempTable;

/// <summary>
/// Coverage of <c>query.ToTempTableAsync(ctx)</c> — populate a temp table from
/// an EF query and read it back.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ToTempTableFromQueryTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ToTempTableFromQueryTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task ToTempTable_PopulatesFromEFQuery_AndQueryReturnsSubset()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 50; i++) ctx.Rows.Add(new Row { Id = i, V = (int)i });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        await using var temp = await ctx.Rows.Where(r => r.V > 25).ToTempTableAsync(ctx);
        var n = await temp.Query().CountAsync();
        Assert.Equal(25, n);

        // The right rows landed — not just any 25 rows. min(V) should be 26 (V > 25),
        // max(V) should be 50 (the table tops out there).
        var min = await temp.Query().Select(r => (int?)r.V).MinAsync();
        var max = await temp.Query().Select(r => (int?)r.V).MaxAsync();
        Assert.Equal(26, min);
        Assert.Equal(50, max);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ToTempTable_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
