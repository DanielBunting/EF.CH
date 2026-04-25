using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>FixedString(N)</c>. Values shorter than N are right-padded
/// with NUL bytes — observable when reading back.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class FixedStringRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public FixedStringRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task FixedString8_RoundTrips_AndShortValuesPadWithNulls()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(
            new Row { Id = 1, Code = "ABCDEFGH" },   // exactly 8
            new Row { Id = 2, Code = "abc" });       // padded
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal("ABCDEFGH", rows[0].Code);
        Assert.StartsWith("abc", rows[1].Code);
        Assert.Equal(8, rows[1].Code.Length);

        var t = await RawClickHouse.ColumnTypeAsync(Conn, "FixedStringRoundTrip_Rows", "Code");
        Assert.Equal("FixedString(8)", t);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Code { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("FixedStringRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Code).HasColumnType("FixedString(8)");
            });
    }
}
