using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV with <c>LowCardinality(String)</c> as the GROUP BY key plus a Sum aggregate.
/// Stresses key-dedup behaviour and ensures the LowCardinality annotation
/// survives the MV target's CREATE statement.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvLowCardinalityKeyTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvLowCardinalityKeyTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LowCardinalityString_GroupByKey()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Country = "US", Amount = 100 },
            new Src { Id = 2, Country = "US", Amount = 200 },
            new Src { Id = 3, Country = "GB", Amount =  50 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvLcTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Country, toInt64(Total) AS Total FROM \"MvLcTarget\" FINAL ORDER BY Country");
        Assert.Equal(2, rows.Count);
        Assert.Equal("GB", (string)rows[0]["Country"]!); Assert.Equal( 50L, Convert.ToInt64(rows[0]["Total"]));
        Assert.Equal("US", (string)rows[1]["Country"]!); Assert.Equal(300L, Convert.ToInt64(rows[1]["Total"]));

        Assert.Equal("LowCardinality(String)", await RawClickHouse.ColumnTypeAsync(Conn, "MvLcTarget", "Country"));
    }

    public sealed class Src { public uint Id { get; set; } public string Country { get; set; } = ""; public long Amount { get; set; } }
    public sealed class Tgt { public string Country { get; set; } = ""; public long Total { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvLcSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Country).HasLowCardinality();
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvLcTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Country);
                e.Property(x => x.Country).HasLowCardinality();
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .GroupBy(r => r.Country)
                    .Select(g => new Tgt { Country = g.Key, Total = g.Sum(r => r.Amount) }));
            });
        }
    }
}
