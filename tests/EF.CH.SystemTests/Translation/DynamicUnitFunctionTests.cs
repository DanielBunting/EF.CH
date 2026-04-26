using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Verifies functions that take a <c>ClickHouseIntervalUnit</c> argument actually
/// execute against modern ClickHouse. The DateAdd/DateSub fix already showed
/// the dynamic-unit form (<c>date_add(unit, n, dt)</c>) is rejected by CH 24+;
/// these tests guard adjacent dynamic-unit APIs from the same regression.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class DynamicUnitFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public DynamicUnitFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task DateDiff_AllUnits_RoundTripCorrectly()
    {
        await using var ctx = TestContextFactory.Create<TimeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Events.Add(new TimeRow
        {
            Id = 1,
            Start = new DateTime(2024, 1, 15, 10, 0, 0, DateTimeKind.Utc),
            End = new DateTime(2024, 1, 18, 14, 30, 0, DateTimeKind.Utc),
        });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var row = await ctx.Events.Select(e => new
        {
            Days = Microsoft.EntityFrameworkCore.EF.Functions.DateDiff(ClickHouseIntervalUnit.Day, e.Start, e.End),
            Hours = Microsoft.EntityFrameworkCore.EF.Functions.DateDiff(ClickHouseIntervalUnit.Hour, e.Start, e.End),
            Minutes = Microsoft.EntityFrameworkCore.EF.Functions.DateDiff(ClickHouseIntervalUnit.Minute, e.Start, e.End),
        }).FirstAsync();

        // 2024-01-15 → 2024-01-18 spans 3 day boundaries.
        Assert.Equal(3, row.Days);
        Assert.Equal(76, row.Hours);
        Assert.Equal(76 * 60 + 30, row.Minutes);
    }

    [Fact]
    public async Task DateAdd_DateSub_Multiple_Units_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<TimeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 1, 15, 10, 0, 0, DateTimeKind.Utc);
        ctx.Events.Add(new TimeRow { Id = 1, Start = t, End = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var row = await ctx.Events.Select(e => new
        {
            PlusDays = Microsoft.EntityFrameworkCore.EF.Functions.DateAdd(ClickHouseIntervalUnit.Day, 7, e.Start),
            PlusHours = Microsoft.EntityFrameworkCore.EF.Functions.DateAdd(ClickHouseIntervalUnit.Hour, 6, e.Start),
            MinusMinutes = Microsoft.EntityFrameworkCore.EF.Functions.DateSub(ClickHouseIntervalUnit.Minute, 30, e.Start),
        }).FirstAsync();

        Assert.Equal(t.AddDays(7), row.PlusDays);
        Assert.Equal(t.AddHours(6), row.PlusHours);
        Assert.Equal(t.AddMinutes(-30), row.MinusMinutes);
    }

    [Fact]
    public async Task ToStartOf_Family_RoundTripCorrectly()
    {
        await using var ctx = TestContextFactory.Create<TimeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 13, 47, 23, DateTimeKind.Utc);
        ctx.Events.Add(new TimeRow { Id = 1, Start = t, End = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var row = await ctx.Events.Select(e => new
        {
            Year = Microsoft.EntityFrameworkCore.EF.Functions.ToStartOfYear(e.Start),
            Month = Microsoft.EntityFrameworkCore.EF.Functions.ToStartOfMonth(e.Start),
            Day = Microsoft.EntityFrameworkCore.EF.Functions.ToStartOfDay(e.Start),
            Hour = Microsoft.EntityFrameworkCore.EF.Functions.ToStartOfHour(e.Start),
            Minute = Microsoft.EntityFrameworkCore.EF.Functions.ToStartOfMinute(e.Start),
        }).FirstAsync();

        Assert.Equal(new DateTime(2024, 1, 1), row.Year);
        Assert.Equal(new DateTime(2024, 6, 1), row.Month);
        Assert.Equal(new DateTime(2024, 6, 15), row.Day);
        Assert.Equal(new DateTime(2024, 6, 15, 13, 0, 0), row.Hour);
        Assert.Equal(new DateTime(2024, 6, 15, 13, 47, 0), row.Minute);
    }

    [Fact]
    public async Task FormatDateTime_RoundTripsToString()
    {
        await using var ctx = TestContextFactory.Create<TimeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 13, 47, 23, DateTimeKind.Utc);
        ctx.Events.Add(new TimeRow { Id = 1, Start = t, End = t });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var row = await ctx.Events.Select(e => new
        {
            Iso = Microsoft.EntityFrameworkCore.EF.Functions.FormatDateTime(e.Start, "%Y-%m-%d"),
        }).FirstAsync();

        Assert.Equal("2024-06-15", row.Iso);
    }

    public sealed class TimeRow
    {
        public uint Id { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }

    public sealed class TimeCtx(DbContextOptions<TimeCtx> o) : DbContext(o)
    {
        public DbSet<TimeRow> Events => Set<TimeRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<TimeRow>(e =>
            {
                e.ToTable("DynUnitRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
