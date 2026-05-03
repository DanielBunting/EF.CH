using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// ClickHouseFunctions date helpers (<c>ToStartOfHour</c>, <c>ToYYYYMM</c>,
/// <c>ToISOYear</c>, etc.) used as MV GROUP BY keys.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvClickHouseDateFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvClickHouseDateFunctionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ToStartOfHour_GroupKey()
    {
        await using var ctx = TestContextFactory.Create<HourCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        var t = new DateTime(2026, 4, 25, 10, 0, 0, DateTimeKind.Utc);
        ctx.Source.AddRange(
            new Row { Id = 1, At = t.AddMinutes( 5), Hits = 1 },
            new Row { Id = 2, At = t.AddMinutes(20), Hits = 2 },
            new Row { Id = 3, At = t.AddHours(1).AddMinutes(5), Hits = 4 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChDtHourTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvChDtHourTarget", final: true));
    }

    [Fact]
    public async Task ToStartOfDay_GroupKey()
    {
        await using var ctx = TestContextFactory.Create<DayCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        var t = new DateTime(2026, 4, 25, 10, 0, 0, DateTimeKind.Utc);
        ctx.Source.AddRange(
            new Row { Id = 1, At = t,                Hits = 1 },
            new Row { Id = 2, At = t.AddHours(5),    Hits = 2 },
            new Row { Id = 3, At = t.AddDays(1),     Hits = 4 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChDtDayTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvChDtDayTarget", final: true));
    }

    [Fact]
    public async Task ToYYYYMM_GroupKey()
    {
        await using var ctx = TestContextFactory.Create<YYYYMMCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, At = new DateTime(2026, 4, 10, 0, 0, 0, DateTimeKind.Utc), Hits = 1 },
            new Row { Id = 2, At = new DateTime(2026, 4, 25, 0, 0, 0, DateTimeKind.Utc), Hits = 1 },
            new Row { Id = 3, At = new DateTime(2026, 5,  1, 0, 0, 0, DateTimeKind.Utc), Hits = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChDtYyyymmTarget");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt32(YearMonth) AS YM, toInt64(Hits) AS Hits FROM \"MvChDtYyyymmTarget\" FINAL ORDER BY YearMonth");
        Assert.Equal(2, rows.Count);
        Assert.Equal(202604, Convert.ToInt32(rows[0]["YM"])); Assert.Equal(2L, Convert.ToInt64(rows[0]["Hits"]));
        Assert.Equal(202605, Convert.ToInt32(rows[1]["YM"])); Assert.Equal(1L, Convert.ToInt64(rows[1]["Hits"]));
    }

    [Fact]
    public async Task ToStartOfWeek_GroupKey()
    {
        await using var ctx = TestContextFactory.Create<WeekCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        var monday = new DateTime(2026, 4, 27, 0, 0, 0, DateTimeKind.Utc);
        ctx.Source.AddRange(
            new Row { Id = 1, At = monday.AddDays(0), Hits = 1 },
            new Row { Id = 2, At = monday.AddDays(2), Hits = 1 },
            new Row { Id = 3, At = monday.AddDays(8), Hits = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChDtWeekTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvChDtWeekTarget", final: true));
    }

    [Fact]
    public async Task ToStartOfMonth_GroupKey()
    {
        await using var ctx = TestContextFactory.Create<MonthCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.AddRange(
            new Row { Id = 1, At = new DateTime(2026, 4, 10, 0, 0, 0, DateTimeKind.Utc), Hits = 1 },
            new Row { Id = 2, At = new DateTime(2026, 4, 25, 0, 0, 0, DateTimeKind.Utc), Hits = 1 },
            new Row { Id = 3, At = new DateTime(2026, 5,  1, 0, 0, 0, DateTimeKind.Utc), Hits = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChDtMonthTarget");
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvChDtMonthTarget", final: true));
    }

    [Fact]
    public async Task ToYYYYMMDD_GroupKey()
    {
        await using var ctx = TestContextFactory.Create<YyyymmddCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        ctx.Source.Add(new Row { Id = 1, At = new DateTime(2026, 4, 25, 0, 0, 0, DateTimeKind.Utc), Hits = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChDtYyyymmddTarget");
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT toInt32(Day) AS D FROM \"MvChDtYyyymmddTarget\" FINAL");
        Assert.Equal(20260425, Convert.ToInt32(rows[0]["D"]));
    }

    public sealed class Row { public long Id { get; set; } public DateTime At { get; set; } public long Hits { get; set; } }
    public sealed class HourTgt { public DateTime Bucket { get; set; } public long Hits { get; set; } }
    public sealed class DayTgt { public DateTime Bucket { get; set; } public long Hits { get; set; } }
    public sealed class YYYYMMTgt { public int YearMonth { get; set; } public long Hits { get; set; } }
    public sealed class WeekTgt { public DateTime Bucket { get; set; } public long Hits { get; set; } }
    public sealed class MonthTgt { public DateTime Bucket { get; set; } public long Hits { get; set; } }
    public sealed class YyyymmddTgt { public int Day { get; set; } public long Hits { get; set; } }

    public sealed class HourCtx(DbContextOptions<HourCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<HourTgt> Target => Set<HourTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChDtHourSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourTgt>(e =>
            {
                e.ToTable("MvChDtHourTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<HourTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfHour(r.At))
                    .Select(g => new HourTgt { Bucket = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }

    public sealed class DayCtx(DbContextOptions<DayCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<DayTgt> Target => Set<DayTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChDtDaySource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<DayTgt>(e =>
            {
                e.ToTable("MvChDtDayTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<DayTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfDay(r.At))
                    .Select(g => new DayTgt { Bucket = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }

    public sealed class YYYYMMCtx(DbContextOptions<YYYYMMCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<YYYYMMTgt> Target => Set<YYYYMMTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChDtYyyymmSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<YYYYMMTgt>(e =>
            {
                e.ToTable("MvChDtYyyymmTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.YearMonth);

            });
            mb.MaterializedView<YYYYMMTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToYYYYMM(r.At))
                    .Select(g => new YYYYMMTgt { YearMonth = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }

    public sealed class WeekCtx(DbContextOptions<WeekCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<WeekTgt> Target => Set<WeekTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChDtWeekSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<WeekTgt>(e =>
            {
                e.ToTable("MvChDtWeekTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<WeekTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfWeek(r.At))
                    .Select(g => new WeekTgt { Bucket = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }

    public sealed class MonthCtx(DbContextOptions<MonthCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<MonthTgt> Target => Set<MonthTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChDtMonthSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<MonthTgt>(e =>
            {
                e.ToTable("MvChDtMonthTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<MonthTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfMonth(r.At))
                    .Select(g => new MonthTgt { Bucket = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }

    public sealed class YyyymmddCtx(DbContextOptions<YyyymmddCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<YyyymmddTgt> Target => Set<YyyymmddTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChDtYyyymmddSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<YyyymmddTgt>(e =>
            {
                e.ToTable("MvChDtYyyymmddTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Day);

            });
            mb.MaterializedView<YyyymmddTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToYYYYMMDD(r.At))
                    .Select(g => new YyyymmddTgt { Day = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }
}
