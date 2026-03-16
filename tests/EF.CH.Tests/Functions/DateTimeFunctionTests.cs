using EF.CH;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Tests.Functions;

/// <summary>
/// Tests for ClickHouse date/time function SQL generation.
/// Covers date truncation, unix timestamps, relative numbers,
/// date_add/date_sub/age, TimeSpan member access, and toStartOfInterval.
/// </summary>
public class DateTimeFunctionTests
{
    #region Phase 1: Simple Function Mappings

    [Fact]
    public void ToStartOfSecond_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfSecond(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfSecond(", sql);
    }

    [Fact]
    public void ToStartOfTenMinutes_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfTenMinutes(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfTenMinutes(", sql);
    }

    [Fact]
    public void ToUnixTimestamp_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Ts = EfClass.Functions.ToUnixTimestamp(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toUnixTimestamp(", sql);
    }

    [Fact]
    public void FromUnixTimestamp_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Dt = EfClass.Functions.FromUnixTimestamp(e.UnixTs) });

        var sql = query.ToQueryString();
        Assert.Contains("fromUnixTimestamp(", sql);
    }

    [Fact]
    public void FromUnixTimestamp64Milli_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Dt = EfClass.Functions.FromUnixTimestamp64Milli(e.UnixTs) });

        var sql = query.ToQueryString();
        Assert.Contains("fromUnixTimestamp64Milli(", sql);
    }

    [Fact]
    public void ToRelativeYearNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeYearNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeYearNum(", sql);
    }

    [Fact]
    public void ToRelativeMonthNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeMonthNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeMonthNum(", sql);
    }

    [Fact]
    public void ToRelativeWeekNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeWeekNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeWeekNum(", sql);
    }

    [Fact]
    public void ToRelativeDayNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeDayNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeDayNum(", sql);
    }

    [Fact]
    public void ToRelativeHourNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeHourNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeHourNum(", sql);
    }

    [Fact]
    public void ToRelativeMinuteNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeMinuteNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeMinuteNum(", sql);
    }

    [Fact]
    public void ToRelativeSecondNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Num = EfClass.Functions.ToRelativeSecondNum(e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("toRelativeSecondNum(", sql);
    }

    [Fact]
    public void DateAdd_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Future = EfClass.Functions.DateAdd(ClickHouseIntervalUnit.Day, 5, e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("date_add(", sql);
    }

    [Fact]
    public void DateSub_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Past = EfClass.Functions.DateSub(ClickHouseIntervalUnit.Day, 5, e.Timestamp) });

        var sql = query.ToQueryString();
        Assert.Contains("date_sub(", sql);
    }

    [Fact]
    public void Age_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Months = EfClass.Functions.Age(ClickHouseIntervalUnit.Month, e.StartTime, e.EndTime) });

        var sql = query.ToQueryString();
        Assert.Contains("age(", sql);
    }

    #endregion

    #region Phase 3: ToStartOfInterval with ClickHouseInterval

    [Fact]
    public void ToStartOfInterval_Seconds_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfInterval(e.Timestamp, 15, ClickHouseIntervalUnit.Second) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfInterval(", sql);
        Assert.Contains("INTERVAL 15 SECOND", sql);
    }

    [Fact]
    public void ToStartOfInterval_Minutes_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfInterval(e.Timestamp, 5, ClickHouseIntervalUnit.Minute) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfInterval(", sql);
        Assert.Contains("INTERVAL 5 MINUTE", sql);
    }

    [Fact]
    public void ToStartOfInterval_Hours_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfInterval(e.Timestamp, 2, ClickHouseIntervalUnit.Hour) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfInterval(", sql);
        Assert.Contains("INTERVAL 2 HOUR", sql);
    }

    [Fact]
    public void ToStartOfInterval_Days_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfInterval(e.Timestamp, 7, ClickHouseIntervalUnit.Day) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfInterval(", sql);
        Assert.Contains("INTERVAL 7 DAY", sql);
    }

    [Fact]
    public void ToStartOfInterval_Months_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Events
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfInterval(e.Timestamp, 3, ClickHouseIntervalUnit.Month) });

        var sql = query.ToQueryString();
        Assert.Contains("toStartOfInterval(", sql);
        Assert.Contains("INTERVAL 3 MONTH", sql);
    }

    #endregion

    #region Test Infrastructure

    private static DateTimeTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<DateTimeTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new DateTimeTestContext(options);
    }

    #endregion
}

public class DateTimeTestEntity
{
    public uint Id { get; set; }
    public DateTime Timestamp { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public long UnixTs { get; set; }
}

public class DateTimeTestContext : DbContext
{
    public DateTimeTestContext(DbContextOptions<DateTimeTestContext> options)
        : base(options)
    {
    }

    public DbSet<DateTimeTestEntity> Events => Set<DateTimeTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DateTimeTestEntity>(entity =>
        {
            entity.ToTable("date_function_test");
            entity.HasKey(e => e.Id);
        });
    }
}
