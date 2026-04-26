using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// Working baseline using <c>AsMaterializedViewRaw</c> with hand-written
/// <c>countMergeState</c> / <c>sumMergeState</c> / <c>uniqMergeState</c>.
/// Like the LINQ MergeState tests, Hourly is a plain AMT target (no MV)
/// populated via direct INSERT-SELECT — that direct write fires Daily's
/// raw-SQL MV. Sits next to the LINQ MergeState tests so the green/green
/// contrast (raw vs LINQ) is explicit.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvMergeStateRawSqlBaselineTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvMergeStateRawSqlBaselineTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task RawSqlChain_CountSumUniqMergeState_Works()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(17);
        for (int i = 0; i < 30; i++)
            ctx.Raw.Add(new RawRow { Id = Guid.NewGuid(), Bucket = i % 2 == 0 ? "a" : "b", Amount = i, UserId = rng.Next(1, 5) });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            """
            INSERT INTO "MsRawHourly"
            SELECT Bucket, countState(), sumState(Amount), uniqState(UserId) FROM "MsRawSrc" GROUP BY Bucket
            """);
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsRawDaily");

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Bucket,
                   toInt64(countMerge(C)) AS C,
                   toInt64(sumMerge(S))   AS S,
                   toInt64(uniqMerge(U))  AS U
            FROM "MsRawDaily" GROUP BY Bucket ORDER BY Bucket
            """);
        Assert.Equal(2, rows.Count);
        Assert.Equal(15L, Convert.ToInt64(rows[0]["C"]));
        Assert.Equal(15L, Convert.ToInt64(rows[1]["C"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsRawSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                // Plain AMT target — populated via raw INSERT-SELECT in the test.
                e.ToTable("MsRawHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.C).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.S).HasAggregateFunction("sum", typeof(long));
                e.Property(x => x.U).HasAggregateFunction("uniq", typeof(long));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsRawDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.C).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.S).HasAggregateFunction("sum", typeof(long));
                e.Property(x => x.U).HasAggregateFunction("uniq", typeof(long));
                e.AsMaterializedViewRaw(
                    sourceTable: "MsRawHourly",
                    selectSql: """
                    SELECT Bucket AS "Bucket",
                           countMergeState(C) AS "C",
                           sumMergeState(S)   AS "S",
                           uniqMergeState(U)  AS "U"
                    FROM "MsRawHourly"
                    GROUP BY Bucket
                    """);
            });
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public long Amount { get; set; } public long UserId { get; set; } }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] C { get; set; } = Array.Empty<byte>(); public byte[] S { get; set; } = Array.Empty<byte>(); public byte[] U { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow  { public string Bucket { get; set; } = ""; public byte[] C { get; set; } = Array.Empty<byte>(); public byte[] S { get; set; } = Array.Empty<byte>(); public byte[] U { get; set; } = Array.Empty<byte>(); }
}
