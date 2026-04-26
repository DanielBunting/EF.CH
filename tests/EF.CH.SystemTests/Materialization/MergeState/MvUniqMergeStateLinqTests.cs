using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.UniqMergeState(...)</c> — Hourly populated via direct INSERT-SELECT
/// to fire Daily's MV through the LINQ <c>uniqMergeState</c> arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvUniqMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvUniqMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqUniqMergeState_ShouldEventuallyWork()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Raw.AddRange(
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", UserId = 1 },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", UserId = 2 },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", UserId = 1 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsUniqHourly\" SELECT Bucket, uniqState(UserId) FROM \"MsUniqRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsUniqDaily");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(uniqMerge(Total)) AS T FROM \"MsUniqDaily\" GROUP BY Bucket");
        Assert.Equal(2L, Convert.ToInt64(rows[0]["T"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsUniqRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                e.ToTable("MsUniqHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("uniq", typeof(long));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsUniqDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("uniq", typeof(long));
                e.AsMaterializedView<DailyRow, HourlyRow>(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, Total = g.UniqMergeState(r => r.Total) }));
            });
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public long UserId { get; set; } }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
}
