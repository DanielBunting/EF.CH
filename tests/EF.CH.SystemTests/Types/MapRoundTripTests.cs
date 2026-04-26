using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>Map(K, V)</c> columns with various key/value combinations.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MapRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MapRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MapStringInt32_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var row = new Row
        {
            Id = 1,
            Counts = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
            Names = new Dictionary<string, string> { ["k"] = "v", ["k2"] = "v2" },
        };
        ctx.Rows.Add(row);
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(row.Counts, read.Counts);
        Assert.Equal(row.Names, read.Names);

        var tCounts = await RawClickHouse.ColumnTypeAsync(Conn, "MapRoundTrip_Rows", "Counts");
        var tNames = await RawClickHouse.ColumnTypeAsync(Conn, "MapRoundTrip_Rows", "Names");
        Assert.Equal("Map(String, Int32)", tCounts);
        Assert.Equal("Map(String, String)", tNames);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public Dictionary<string, int> Counts { get; set; } = new();
        public Dictionary<string, string> Names { get; set; } = new();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("MapRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Counts).HasColumnType("Map(String, Int32)");
                e.Property(x => x.Names).HasColumnType("Map(String, String)");
            });
    }
}
