using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV with <c>Decimal(38, 6)</c> (Decimal128) aggregated via Sum. Decimal128 is
/// the 128-bit storage variant for high precision; the aggregate must preserve
/// the precision/scale through the MV target.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDecimal128RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDecimal128RoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Decimal128_AggregatedViaSum()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Bucket = "a", Amount = 1.234567m },
            new Src { Id = 2, Bucket = "a", Amount = 2.345678m },
            new Src { Id = 3, Bucket = "b", Amount = 9.999999m });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDecimal128Target");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toString(Total) AS Total FROM \"MvDecimal128Target\" FINAL ORDER BY Bucket");
        Assert.Equal(2, rows.Count);
        Assert.Equal("a", (string)rows[0]["Bucket"]!); Assert.Equal("3.580245", (string)rows[0]["Total"]!);
        Assert.Equal("b", (string)rows[1]["Bucket"]!); Assert.Equal("9.999999", (string)rows[1]["Total"]!);
    }

    public sealed class Src { public uint Id { get; set; } public string Bucket { get; set; } = ""; public decimal Amount { get; set; } }
    public sealed class Tgt { public string Bucket { get; set; } = ""; public decimal Total { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvDecimal128Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Amount).HasColumnType("Decimal(38, 6)");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvDecimal128Target"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasColumnType("Decimal(38, 6)");

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new Tgt { Bucket = g.Key, Total = g.Sum(r => r.Amount) }));
        }
    }
}
