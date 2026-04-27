using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.SumMergeState(...)</c> for AMT → AMT. Hourly is a plain AMT
/// target (no MV) populated via direct INSERT-SELECT; that direct write
/// fires Daily's MV — which exercises the LINQ <c>sumMergeState</c> arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvSumMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvSumMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqSumMergeState_ReturnsExpectedSums()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Raw.AddRange(
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Amount = 10 },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Amount = 20 },
            new RawRow { Id = Guid.NewGuid(), Bucket = "b", Amount =  5 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsSumHourly\" SELECT Bucket, sumState(Amount) FROM \"MsSumRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsSumDaily");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toInt64(sumMerge(Total)) AS T FROM \"MsSumDaily\" GROUP BY Bucket ORDER BY Bucket");
        Assert.Equal(30L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal( 5L, Convert.ToInt64(rows[1]["T"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsSumRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                e.ToTable("MsSumHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("sum", typeof(long));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsSumDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("sum", typeof(long));

            });
            mb.MaterializedView<DailyRow>().From<HourlyRow>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, Total = g.SumMergeState(r => r.Total) }));
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public long Amount { get; set; } }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
}
