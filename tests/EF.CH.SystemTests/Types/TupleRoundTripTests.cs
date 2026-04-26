using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for direct <c>Tuple(...)</c> mapping. The brace-literal proxy in
/// existing tests is partial coverage; this asserts the column type renders
/// as <c>Tuple(...)</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TupleRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TupleRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Tuple_StringInt32_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.Add(new Row { Id = 1, Pair = ("alpha", 42) });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal("alpha", read.Pair.Item1);
        Assert.Equal(42, read.Pair.Item2);

        var t = await RawClickHouse.ColumnTypeAsync(Conn, "TupleRoundTrip_Rows", "Pair");
        Assert.Equal("Tuple(String, Int32)", t);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public (string, int) Pair { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("TupleRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Pair).HasColumnType("Tuple(String, Int32)");
            });
    }
}
