using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.MinMergeState(...)</c> — Hourly populated via direct INSERT-SELECT
/// to fire Daily's MV through the LINQ <c>minMergeState</c> arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvMinMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvMinMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqMinMergeState_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Raw.AddRange(
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Amount = 30 },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Amount =  5 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsMinHourly\" SELECT Bucket, minState(Amount) FROM \"MsMinRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsMinDaily");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(minMerge(Total)) AS T FROM \"MsMinDaily\" GROUP BY Bucket");
        Assert.Equal(5L, Convert.ToInt64(rows[0]["T"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsMinRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                e.ToTable("MsMinHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("min", typeof(long));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsMinDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("min", typeof(long));
                e.AsMaterializedView<DailyRow, HourlyRow>(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, Total = g.MinMergeState(r => r.Total) }));
            });
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public long Amount { get; set; } }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
}
