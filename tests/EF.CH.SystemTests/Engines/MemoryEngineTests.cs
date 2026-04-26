using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// Coverage of <c>UseMemoryEngine</c>. Memory tables hold rows in process memory; a
/// container restart loses data, but in-process round-trip should work.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MemoryEngineTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MemoryEngineTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MemoryEngine_DeclaresMemoryEngine_AndRoundTripsRows()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Note = "in memory" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var engine = await RawClickHouse.ScalarAsync<string>(Conn,
            "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'Memory_Rows'");
        Assert.Equal("Memory", engine);
        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal("in memory", read.Note);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Note { get; set; } = "";
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("Memory_Rows"); e.HasKey(x => x.Id); e.UseMemoryEngine();
            });
    }
}
