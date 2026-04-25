using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for standalone <c>LowCardinality(String)</c> and
/// <c>LowCardinality(Nullable(String))</c> columns. Existing MV tests cover
/// LowCardinality only as MV target columns; this asserts the standalone storage path.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class LowCardinalityRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public LowCardinalityRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task LowCardinalityString_AndNullable_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(
            new Row { Id = 1, Category = "alpha", MaybeRegion = "us" },
            new Row { Id = 2, Category = "beta",  MaybeRegion = null });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal("alpha", rows[0].Category);
        Assert.Equal("us", rows[0].MaybeRegion);
        Assert.Equal("beta", rows[1].Category);
        Assert.Null(rows[1].MaybeRegion);

        var tCat = await RawClickHouse.ColumnTypeAsync(Conn, "LowCardinalityRoundTrip_Rows", "Category");
        var tRegion = await RawClickHouse.ColumnTypeAsync(Conn, "LowCardinalityRoundTrip_Rows", "MaybeRegion");
        Assert.Equal("LowCardinality(String)", tCat);
        Assert.Equal("LowCardinality(Nullable(String))", tRegion);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Category { get; set; } = "";
        public string? MaybeRegion { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("LowCardinalityRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Category).HasColumnType("LowCardinality(String)");
                e.Property(x => x.MaybeRegion).HasColumnType("LowCardinality(Nullable(String))");
            });
    }
}
