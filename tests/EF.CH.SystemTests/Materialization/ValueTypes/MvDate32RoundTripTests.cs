using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV round-trip with <c>Date32</c> as the GROUP BY key. Date32 supports years
/// outside the 1970–2149 range that plain Date does.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDate32RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDate32RoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Date32_GroupByKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var d1 = new DateTime(1925, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        var d2 = new DateTime(2120, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        ctx.Sources.AddRange(
            new Src { Id = 1, Day = d1, Hits = 5 },
            new Src { Id = 2, Day = d1, Hits = 7 },
            new Src { Id = 3, Day = d2, Hits = 3 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDate32Target");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toString(Day) AS Day, toInt64(Hits) AS Hits FROM \"MvDate32Target\" FINAL ORDER BY Day");
        Assert.Equal(2, rows.Count);
        Assert.Equal("1925-06-15", (string)rows[0]["Day"]!); Assert.Equal(12L, Convert.ToInt64(rows[0]["Hits"]));
        Assert.Equal("2120-01-01", (string)rows[1]["Day"]!); Assert.Equal( 3L, Convert.ToInt64(rows[1]["Hits"]));
    }

    public sealed class Src { public uint Id { get; set; } public DateTime Day { get; set; } public long Hits { get; set; } }
    public sealed class Tgt { public DateTime Day { get; set; } public long Hits { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvDate32Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Day).HasColumnType("Date32");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvDate32Target"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Day);
                e.Property(x => x.Day).HasColumnType("Date32");
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .GroupBy(r => r.Day)
                    .Select(g => new Tgt { Day = g.Key, Hits = g.Sum(r => r.Hits) }));
            });
        }
    }
}
