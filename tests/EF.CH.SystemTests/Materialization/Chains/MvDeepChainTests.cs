using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Chains;

/// <summary>
/// 3-level rollup: raw → hourly (AMT, fluent LINQ MV) → daily (AMT) → monthly (AMT).
/// Hop 1 is fluent LINQ; hops 2 and 3 are populated by explicit
/// <c>INSERT … SELECT … *MergeState … FROM previousLevel GROUP BY …</c> because
/// the LINQ surface lacks <c>-MergeState</c> combinators (covered by the
/// deliberately-broken Materialization/MergeState/* tests) and because
/// ClickHouse cascading MV triggering does not propagate reliably through
/// chained AMT MVs in this configuration. Mirrors the working 2-level shape
/// from <c>MaterializedViewPopulateAndChainTests</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDeepChainTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDeepChainTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ThreeLevelChain_RawToHourlyToDailyToMonthly()
    {
        await using var ctx = TestContextFactory.Create<ChainCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var baseTime = new DateTime(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc);
        var rng = new Random(11);
        var data = new List<RawClick>();
        for (int hour = 0; hour < 48; hour++)
            for (int j = 0; j < 5; j++)
                data.Add(new RawClick { Id = Guid.NewGuid(), At = baseTime.AddHours(hour), UserId = rng.Next(1, 8) });

        ctx.RawClicks.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "DeepChainHourly");

        // Hop 2: hourly → daily
        await RawClickHouse.ExecuteAsync(Conn,
            """
            INSERT INTO "DeepChainDaily"
            SELECT toStartOfDay(HourBucket) AS "DayBucket",
                   countMergeState(ClickCount) AS "ClickCount",
                   uniqMergeState(UniqUsers)   AS "UniqUsers"
            FROM "DeepChainHourly" GROUP BY DayBucket
            """);
        await RawClickHouse.SettleMaterializationAsync(Conn, "DeepChainDaily");

        // Hop 3: daily → monthly
        await RawClickHouse.ExecuteAsync(Conn,
            """
            INSERT INTO "DeepChainMonthly"
            SELECT toStartOfMonth(DayBucket) AS "MonthBucket",
                   countMergeState(ClickCount) AS "ClickCount",
                   uniqMergeState(UniqUsers)   AS "UniqUsers"
            FROM "DeepChainDaily" GROUP BY MonthBucket
            """);
        await RawClickHouse.SettleMaterializationAsync(Conn, "DeepChainMonthly");

        var monthly = (await RawClickHouse.RowsAsync(Conn,
            """
            SELECT toInt64(countMerge(ClickCount)) AS C,
                   toInt64(uniqMerge(UniqUsers))   AS U
            FROM "DeepChainMonthly"
            """)).Single();

        Assert.Equal(data.Count, Convert.ToInt64(monthly["C"]));
        Assert.Equal(data.Select(d => d.UserId).Distinct().LongCount(), Convert.ToInt64(monthly["U"]));
    }

    public sealed class ChainCtx(DbContextOptions<ChainCtx> o) : DbContext(o)
    {
        public DbSet<RawClick> RawClicks => Set<RawClick>();
        public DbSet<Hourly> Hourly => Set<Hourly>();
        public DbSet<Daily>  Daily  => Set<Daily>();
        public DbSet<Monthly> Monthly => Set<Monthly>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawClick>(e =>
            {
                e.ToTable("DeepChainRaw"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.At, x.Id });
            });

            mb.Entity<Hourly>(e =>
            {
                e.ToTable("DeepChainHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.HourBucket);
                e.Property(x => x.ClickCount).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.UniqUsers).HasAggregateFunction("uniq", typeof(long));

            });
            mb.MaterializedView<Hourly>().From<RawClick>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfHour(r.At))
                    .Select(g => new Hourly
                    {
                        HourBucket = g.Key,
                        ClickCount = g.CountState(),
                        UniqUsers = g.UniqState(r => r.UserId),
                    }));

            // Plain AMT target tables (no MV) — populated by explicit INSERT-SELECT.
            mb.Entity<Daily>(e =>
            {
                e.ToTable("DeepChainDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.DayBucket);
                e.Property(x => x.ClickCount).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.UniqUsers).HasAggregateFunction("uniq", typeof(long));
            });

            mb.Entity<Monthly>(e =>
            {
                e.ToTable("DeepChainMonthly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.MonthBucket);
                e.Property(x => x.ClickCount).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.UniqUsers).HasAggregateFunction("uniq", typeof(long));
            });
        }
    }

    public sealed class RawClick { public Guid Id { get; set; } public DateTime At { get; set; } public long UserId { get; set; } }
    public sealed class Hourly  { public DateTime HourBucket  { get; set; } public byte[] ClickCount { get; set; } = Array.Empty<byte>(); public byte[] UniqUsers { get; set; } = Array.Empty<byte>(); }
    public sealed class Daily   { public DateTime DayBucket   { get; set; } public byte[] ClickCount { get; set; } = Array.Empty<byte>(); public byte[] UniqUsers { get; set; } = Array.Empty<byte>(); }
    public sealed class Monthly { public DateTime MonthBucket { get; set; } public byte[] ClickCount { get; set; } = Array.Empty<byte>(); public byte[] UniqUsers { get; set; } = Array.Empty<byte>(); }
}
