using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Pins the Array element-promotion arm of <c>WrapWithClrTypeCast</c>:
/// <c>quantilesTDigest(...)</c> natively returns <c>Array(Float32)</c>, but
/// the declared property <c>double[]</c> requires <c>Array(Float64)</c>. The
/// translator emits <c>CAST(quantilesTDigest(...)(...) AS Array(Float64))</c>
/// so the MV target column comes out as <c>Array(Float64)</c> (not Float32).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvArrayElementPromotionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvArrayElementPromotionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task QuantilesTDigest_PromotesArrayFloat32_ToFloat64()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(7);
        for (int i = 0; i < 100; i++)
            ctx.Source.Add(new Row { Id = i + 1, Latency = rng.NextDouble() * 100 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvArrayPromoTarget");

        Assert.Equal("Array(Float64)",
            await RawClickHouse.ColumnTypeAsync(Conn, "MvArrayPromoTarget", "Quantiles"));

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT length(Quantiles) AS L FROM \"MvArrayPromoTarget\" FINAL");
        Assert.Equal(3UL, Convert.ToUInt64(rows[0]["L"]));
    }

    public sealed class Row { public long Id { get; set; } public double Latency { get; set; } }
    public sealed class Tgt { public long Bucket { get; set; } public double[] Quantiles { get; set; } = Array.Empty<double>(); }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvArrayPromoSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvArrayPromoTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<Tgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new Tgt
                    {
                        Bucket = g.Key,
                        Quantiles = g.QuantilesTDigest(new[] { 0.5, 0.9, 0.99 }, r => r.Latency),
                    }));
        }
    }
}
