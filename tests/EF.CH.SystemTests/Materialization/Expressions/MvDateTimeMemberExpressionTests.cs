using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// DateTime member-access translations: <c>.Year</c>, <c>.Month</c>, <c>.Day</c>,
/// <c>.Hour</c>, <c>.Minute</c>, <c>.Second</c>, <c>.Date</c>. Used in both GROUP BY
/// keys and selectors.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvDateTimeMemberExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvDateTimeMemberExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Year_InGroupByKey()
    {
        await using var ctx = TestContextFactory.Create<YearCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        Seed(ctx);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDtYearTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt32(Year) AS Year, toInt64(Hits) AS Hits FROM \"MvDtYearTarget\" FINAL ORDER BY Year");
        Assert.Equal(2, rows.Count);
        Assert.Equal(2025, Convert.ToInt32(rows[0]["Year"])); Assert.Equal(2L, Convert.ToInt64(rows[0]["Hits"]));
        Assert.Equal(2026, Convert.ToInt32(rows[1]["Year"])); Assert.Equal(2L, Convert.ToInt64(rows[1]["Hits"]));
    }

    [Fact]
    public async Task Month_InGroupByKey()
    {
        await using var ctx = TestContextFactory.Create<MonthCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        Seed(ctx);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDtMonthTarget");
        var months = await RawClickHouse.ColumnAsync<int>(Conn,
            "SELECT toInt32(Month) FROM \"MvDtMonthTarget\" FINAL ORDER BY Month");
        Assert.Contains(1, months);
        Assert.Contains(6, months);
    }

    [Fact]
    public async Task Date_InGroupByKey()
    {
        await using var ctx = TestContextFactory.Create<DateCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        Seed(ctx);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDtDateTarget");
        // 4 rows over 4 distinct dates.
        Assert.Equal(4UL, await RawClickHouse.RowCountAsync(Conn, "MvDtDateTarget", final: true));
    }

    [Fact]
    public async Task Hour_Minute_Second_InSelector()
    {
        await using var ctx = TestContextFactory.Create<HmsCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, At = new DateTime(2026, 4, 25, 14, 37, 12, DateTimeKind.Utc) });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvDtHmsTarget");
        var row = (await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt32(Hour) H, toInt32(Minute) M, toInt32(Second) S FROM \"MvDtHmsTarget\"")).Single();
        Assert.Equal(14, Convert.ToInt32(row["H"]));
        Assert.Equal(37, Convert.ToInt32(row["M"]));
        Assert.Equal(12, Convert.ToInt32(row["S"]));
    }

    private static void Seed<TCtx>(TCtx ctx) where TCtx : DbContext
    {
        var src = ctx.Set<Row>();
        src.AddRange(
            new Row { Id = 1, At = new DateTime(2025, 1, 10, 10, 0, 0, DateTimeKind.Utc) },
            new Row { Id = 2, At = new DateTime(2025, 6, 15, 14, 0, 0, DateTimeKind.Utc) },
            new Row { Id = 3, At = new DateTime(2026, 1,  1,  9, 0, 0, DateTimeKind.Utc) },
            new Row { Id = 4, At = new DateTime(2026, 6, 30, 23, 0, 0, DateTimeKind.Utc) });
    }

    public sealed class Row { public long Id { get; set; } public DateTime At { get; set; } }
    public sealed class YearTgt { public int Year { get; set; } public long Hits { get; set; } }
    public sealed class MonthTgt { public int Month { get; set; } public long Hits { get; set; } }
    public sealed class DateTgt { public DateTime Day { get; set; } public long Hits { get; set; } }
    public sealed class HmsTgt { public long Id { get; set; } public int Hour { get; set; } public int Minute { get; set; } public int Second { get; set; } }

    public sealed class YearCtx(DbContextOptions<YearCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<YearTgt> Target => Set<YearTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvDtYearSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<YearTgt>(e =>
            {
                e.ToTable("MvDtYearTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Year);

            });
            mb.MaterializedView<YearTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.At.Year)
                    .Select(g => new YearTgt { Year = g.Key, Hits = g.Count() }));
        }
    }

    public sealed class MonthCtx(DbContextOptions<MonthCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<MonthTgt> Target => Set<MonthTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvDtMonthSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MonthTgt>(e =>
            {
                e.ToTable("MvDtMonthTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Month);

            });
            mb.MaterializedView<MonthTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.At.Month)
                    .Select(g => new MonthTgt { Month = g.Key, Hits = g.Count() }));
        }
    }

    public sealed class DateCtx(DbContextOptions<DateCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<DateTgt> Target => Set<DateTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvDtDateSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<DateTgt>(e =>
            {
                e.ToTable("MvDtDateTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Day);

            });
            mb.MaterializedView<DateTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => r.At.Date)
                    .Select(g => new DateTgt { Day = g.Key, Hits = g.Count() }));
        }
    }

    public sealed class HmsCtx(DbContextOptions<HmsCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<HmsTgt> Target => Set<HmsTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvDtHmsSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HmsTgt>(e =>
            {
                e.ToTable("MvDtHmsTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<HmsTgt>().From<Row>().DefinedAs(rows => rows
                    .Select(r => new HmsTgt { Id = r.Id, Hour = r.At.Hour, Minute = r.At.Minute, Second = r.At.Second }));
        }
    }
}
