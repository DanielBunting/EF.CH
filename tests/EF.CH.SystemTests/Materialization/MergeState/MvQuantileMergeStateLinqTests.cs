using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.QuantileMergeState(level, ...)</c> — Hourly populated via direct
/// INSERT-SELECT to fire Daily's MV through the LINQ <c>quantileMergeState</c> arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvQuantileMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvQuantileMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqQuantileMergeState_ReturnsBoundedQuantile()
    {
        // Explicit table drops first — EnsureDeletedAsync occasionally leaves
        // partial state from prior failed runs in the shared fixture, and the
        // parametric "quantile(0.5)" AggregateFunction column type is more
        // sensitive to that than the simpler -State combinator tests.
        foreach (var tbl in new[] { "MsQuantileDaily", "MsQuantileHourly", "MsQuantileRaw" })
            await RawClickHouse.ExecuteAsync(Conn, $"DROP TABLE IF EXISTS \"{tbl}\"");

        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(13);
        for (int i = 0; i < 50; i++)
            ctx.Raw.Add(new RawRow { Id = Guid.NewGuid(), Bucket = "a", Latency = rng.NextDouble() * 100 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsQuantileHourly\" SELECT Bucket, quantileState(0.5)(Latency) FROM \"MsQuantileRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsQuantileDaily");

        var p = await RawClickHouse.ScalarAsync<double>(Conn,
            "SELECT toFloat64(quantileMerge(0.5)(P50)) FROM \"MsQuantileDaily\"");
        Assert.InRange(p, 0.0, 100.0);
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsQuantileRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                e.ToTable("MsQuantileHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.P50).HasAggregateFunction("quantile(0.5)", typeof(double));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsQuantileDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.P50).HasAggregateFunction("quantile(0.5)", typeof(double));
                e.AsMaterializedView<DailyRow, HourlyRow>(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, P50 = g.QuantileMergeState(0.5, r => r.P50) }));
            });
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public double Latency { get; set; } }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] P50 { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] P50 { get; set; } = Array.Empty<byte>(); }
}
