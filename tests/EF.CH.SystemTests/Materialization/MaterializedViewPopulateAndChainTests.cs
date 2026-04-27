using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

[Collection(SingleNodeCollection.Name)]
public class MaterializedViewPopulateAndChainTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MaterializedViewPopulateAndChainTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MaterializedView_WithPopulateTrue_BackfillsHistoricalRows()
    {
        await using var seed = TestContextFactory.Create<SeedCtx>(Conn);
        await seed.Database.EnsureDeletedAsync();
        await seed.Database.EnsureCreatedAsync();

        seed.Sales.AddRange(
            new Sale { Id = 1, Region = "eu", Amount = 10 },
            new Sale { Id = 2, Region = "eu", Amount = 20 },
            new Sale { Id = 3, Region = "us", Amount = 15 });
        await seed.SaveChangesAsync();

        // The MV is created through raw DDL here because POPULATE is not a fluent-API primitive —
        // but the source schema was created through EF. The verification is still via raw SQL.
        await RawClickHouse.ExecuteAsync(Conn,
            """
            CREATE MATERIALIZED VIEW "PopulatedSummary"
            ENGINE = SummingMergeTree() ORDER BY Region
            POPULATE
            AS SELECT Region AS "Region", sum(Amount) AS "Total" FROM "Sales" GROUP BY Region
            """);

        await RawClickHouse.SettleMaterializationAsync(Conn, "PopulatedSummary");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toFloat64(Total) AS Total FROM \"PopulatedSummary\" FINAL ORDER BY Region");
        Assert.Equal(2, rows.Count);
        Assert.Equal(30.0, Convert.ToDouble(rows[0]["Total"]), 3);
        Assert.Equal(15.0, Convert.ToDouble(rows[1]["Total"]), 3);
    }

    [Fact]
    public async Task Chained_RawEvents_To_Hourly_To_Daily_ViaStateMerging()
    {
        await using var ctx = TestContextFactory.Create<ChainCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var baseTime = new DateTime(2026, 5, 10, 0, 0, 0, DateTimeKind.Utc);
        var rng = new Random(3);
        var data = new List<RawClick>();
        for (int minute = 0; minute < 120; minute++)
            for (int j = 0; j < 6; j++)
                data.Add(new RawClick
                {
                    Id = Guid.NewGuid(),
                    At = baseTime.AddMinutes(minute),
                    UserId = rng.Next(1, 10),
                });

        ctx.RawClicks.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "HourlyClicks");

        var hourly = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT HourBucket,
                   toInt64(countMerge(ClickCount)) AS ClickCount,
                   toInt64(uniqMerge(UniqUsers)) AS UniqUsers
            FROM "HourlyClicks" GROUP BY HourBucket ORDER BY HourBucket
            """);
        Assert.Equal(2, hourly.Count);
        Assert.Equal(720L, hourly.Sum(h => Convert.ToInt64(h["ClickCount"])));
        Assert.All(hourly, h => Assert.Equal(360L, Convert.ToInt64(h["ClickCount"])));

        // Fold hourly state into the daily target (states chain via INSERT SELECT).
        await RawClickHouse.ExecuteAsync(Conn,
            """
            INSERT INTO "DailyClicks"
            SELECT toDateTime(toDate(HourBucket)) AS "DayBucket",
                   countMergeState(ClickCount) AS "ClickCount",
                   uniqMergeState(UniqUsers) AS "UniqUsers"
            FROM "HourlyClicks" GROUP BY DayBucket
            """);

        await RawClickHouse.SettleMaterializationAsync(Conn, "DailyClicks");

        var daily = (await RawClickHouse.RowsAsync(Conn,
            """
            SELECT toInt64(countMerge(ClickCount)) AS ClickCount,
                   toInt64(uniqMerge(UniqUsers)) AS UniqUsers
            FROM "DailyClicks"
            """)).Single();

        Assert.Equal(data.Count, Convert.ToInt64(daily["ClickCount"]));
        Assert.Equal(data.Select(d => d.UserId).Distinct().LongCount(), Convert.ToInt64(daily["UniqUsers"]));
    }

    public sealed class SeedCtx(DbContextOptions<SeedCtx> o) : DbContext(o)
    {
        public DbSet<Sale> Sales => Set<Sale>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Sale>(e => { e.ToTable("Sales"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    public sealed class ChainCtx(DbContextOptions<ChainCtx> o) : DbContext(o)
    {
        public DbSet<RawClick> RawClicks => Set<RawClick>();
        public DbSet<HourlyClick> HourlyClicks => Set<HourlyClick>();
        public DbSet<DailyClick> DailyClicks => Set<DailyClick>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawClick>(e =>
            {
                e.ToTable("RawClicks"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.At, x.Id });
            });

            mb.Entity<HourlyClick>(e =>
            {
                e.ToTable("HourlyClicks"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.HourBucket);
                e.Property(x => x.ClickCount).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.UniqUsers).HasAggregateFunction("uniq", typeof(long));

            });
            mb.MaterializedView<HourlyClick>().From<RawClick>().DefinedAs(clicks => clicks
                    .GroupBy(r => EF.CH.Extensions.ClickHouseFunctions.ToStartOfHour(r.At))
                    .Select(g => new HourlyClick
                    {
                        HourBucket = g.Key,
                        ClickCount = g.CountState(),
                        UniqUsers = g.UniqState(r => r.UserId),
                    }));

            mb.Entity<DailyClick>(e =>
            {
                e.ToTable("DailyClicks"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.DayBucket);
                e.Property(x => x.ClickCount).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.UniqUsers).HasAggregateFunction("uniq", typeof(long));
            });
        }
    }

    public class Sale { public long Id { get; set; } public string Region { get; set; } = ""; public double Amount { get; set; } }

    public class RawClick { public Guid Id { get; set; } public DateTime At { get; set; } public long UserId { get; set; } }
    public class HourlyClick { public DateTime HourBucket { get; set; } public byte[] ClickCount { get; set; } = Array.Empty<byte>(); public byte[] UniqUsers { get; set; } = Array.Empty<byte>(); }
    public class DailyClick { public DateTime DayBucket { get; set; } public byte[] ClickCount { get; set; } = Array.Empty<byte>(); public byte[] UniqUsers { get; set; } = Array.Empty<byte>(); }
}
