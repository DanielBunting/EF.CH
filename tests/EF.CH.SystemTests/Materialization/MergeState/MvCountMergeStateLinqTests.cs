using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.CountMergeState(...)</c> for AggregatingMergeTree → AggregatingMergeTree.
/// Hourly is a plain AMT target (no MV) populated via direct INSERT-SELECT;
/// that direct write fires Daily's MV, which uses the LINQ MergeState combinator
/// under test. This isolates the translator's <c>countMergeState</c> arm from
/// ClickHouse cascading-MV propagation behaviour.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvCountMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvCountMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqCountMergeState_ReturnsExpectedCounts()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Raw.AddRange(
            new RawRow { Id = Guid.NewGuid(), Bucket = "a" },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a" },
            new RawRow { Id = Guid.NewGuid(), Bucket = "b" });
        await ctx.SaveChangesAsync();

        // Direct INSERT into Hourly fires Daily's MV (which exercises the
        // LINQ CountMergeState translation).
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsCountHourly\" SELECT Bucket, countState() FROM \"MsCountRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsCountDaily");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toInt64(countMerge(Total)) AS T FROM \"MsCountDaily\" GROUP BY Bucket ORDER BY Bucket");
        Assert.Equal(2L, Convert.ToInt64(rows[0]["T"]));
        Assert.Equal(1L, Convert.ToInt64(rows[1]["T"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsCountRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                // Plain AMT target — no MV. Populated via raw INSERT-SELECT in the test.
                e.ToTable("MsCountHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("count", typeof(ulong));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsCountDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Total).HasAggregateFunction("count", typeof(ulong));
                e.AsMaterializedView<DailyRow, HourlyRow>(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, Total = g.CountMergeState(r => r.Total) }));
            });
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] Total { get; set; } = Array.Empty<byte>(); }
}
