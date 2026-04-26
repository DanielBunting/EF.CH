using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.MaxMergeState(...)</c> — Hourly populated via direct INSERT-SELECT
/// to fire Daily's MV through the LINQ <c>maxMergeState</c> arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvMaxMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvMaxMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqMaxMergeState_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Raw.AddRange(
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Amount = 30 },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Amount =  5 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsMaxHourly\" SELECT Bucket, maxState(Amount) FROM \"MsMaxRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsMaxDaily");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(maxMerge(Total)) AS T FROM \"MsMaxDaily\" GROUP BY Bucket");
        Assert.Equal(30L, Convert.ToInt64(rows[0]["T"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsMaxRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                e.ToTable("MsMaxHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("max", typeof(long));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsMaxDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("max", typeof(long));
                e.AsMaterializedView<DailyRow, HourlyRow>(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, Total = g.MaxMergeState(r => r.Total) }));
            });
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public long Amount { get; set; } }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
}
