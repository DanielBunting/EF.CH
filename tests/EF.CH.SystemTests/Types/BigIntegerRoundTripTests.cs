using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>Int128</c> / <c>UInt128</c> columns. .NET 7+ ships native
/// <see cref="Int128"/> and <see cref="UInt128"/> types.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BigIntegerRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BigIntegerRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Int128_AndUInt128_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var i = (Int128)123456789012345678L * 10;
        var u = (UInt128)123456789012345678UL * 100;
        ctx.Rows.Add(new Row { Id = 1, Big = i, Big2 = u });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(i, read.Big);
        Assert.Equal(u, read.Big2);

        var tBig = await RawClickHouse.ColumnTypeAsync(Conn, "BigIntRoundTrip_Rows", "Big");
        var tBig2 = await RawClickHouse.ColumnTypeAsync(Conn, "BigIntRoundTrip_Rows", "Big2");
        Assert.Equal("Int128", tBig);
        Assert.Equal("UInt128", tBig2);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public Int128 Big { get; set; }
        public UInt128 Big2 { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("BigIntRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
