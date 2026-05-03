using EF.CH.Extensions;
using EF.CH.Tests.Sql;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Tests.Functions;

/// <summary>
/// Translation tests for Tier 1 of the missing-CH-functions rollout:
///   1a — multiIf (conditional)
///   1b — string pattern matching (ilike, notLike, match, replaceRegexp*, position*)
///   1c — safe type conversion (to*OrNull / to*OrZero / parseDateTimeBestEffort*)
///   1d — datetime extras (dateTrunc, toStartOfInterval, toTimeZone, timeZoneOf, now64)
/// </summary>
public class Tier1FunctionTranslationTests
{
    // ---- 1a — MultiIf ----

    [Fact]
    public void MultiIf_3Args_EmitsMultiIf()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.MultiIf(e.Value > 100, "high", "low"))
            .ToQueryString();
        Assert.Contains("multiIf(", sql);
    }

    [Fact]
    public void MultiIf_5Args_EmitsMultiIf()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.MultiIf(e.Value > 100, "high", e.Value > 50, "mid", "low"))
            .ToQueryString();
        Assert.Contains("multiIf(", sql);
    }

    [Fact]
    public void MultiIf_9Args_EmitsMultiIf()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.MultiIf(
                e.Value > 100, "h",
                e.Value > 50, "m",
                e.Value > 10, "l",
                e.Value > 0, "z",
                "x"))
            .ToQueryString();
        Assert.Contains("multiIf(", sql);
    }

    // ---- 1b — string patterns ----

    [Fact]
    public void ILike_Emits_ilike()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Where(e => EfClass.Functions.ILike(e.Name, "%CASE%")).ToQueryString();
        Assert.Contains("ilike(", sql);
    }

    [Fact]
    public void NotLike_Emits_notLike()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Where(e => EfClass.Functions.NotLike(e.Name, "%abc%")).ToQueryString();
        Assert.Contains("notLike(", sql);
    }

    [Fact]
    public void Match_Emits_match()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Where(e => EfClass.Functions.Match(e.Name, "^a.*z$")).ToQueryString();
        Assert.Contains("match(", sql);
    }

    [Fact]
    public void ReplaceRegex_Emits_replaceRegexpOne()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.ReplaceRegex(e.Name, @"\d+", "#"))
            .ToQueryString();
        Assert.Contains("replaceRegexpOne(", sql);
    }

    [Fact]
    public void ReplaceRegexAll_Emits_replaceRegexpAll()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.ReplaceRegexAll(e.Name, @"\s+", "_"))
            .ToQueryString();
        Assert.Contains("replaceRegexpAll(", sql);
    }

    [Fact]
    public void Position_Emits_position()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.Position(e.Name, "x")).ToQueryString();
        Assert.Contains("position(", sql);
    }

    [Fact]
    public void PositionCaseInsensitive_Emits_positionCaseInsensitiveUTF8()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.PositionCaseInsensitive(e.Name, "X"))
            .ToQueryString();
        Assert.Contains("positionCaseInsensitiveUTF8(", sql);
    }

    // ---- 1c — safe type conversion ----

    [Fact]
    public void ToInt32OrNull_Emits_toInt32OrNull()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ToInt32OrNull(e.Name)).ToQueryString();
        Assert.Contains("toInt32OrNull(", sql);
    }

    [Fact]
    public void ToInt64OrZero_Emits_toInt64OrZero()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ToInt64OrZero(e.Name)).ToQueryString();
        Assert.Contains("toInt64OrZero(", sql);
    }

    [Fact]
    public void ToFloat64OrNull_Emits_toFloat64OrNull()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ToFloat64OrNull(e.Name)).ToQueryString();
        Assert.Contains("toFloat64OrNull(", sql);
    }

    [Fact]
    public void ToDateTimeOrNull_Emits_toDateTimeOrNull()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ToDateTimeOrNull(e.Name)).ToQueryString();
        Assert.Contains("toDateTimeOrNull(", sql);
    }

    [Fact]
    public void ParseDateTimeBestEffort_Emits_parseDateTimeBestEffort()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ParseDateTimeBestEffort(e.Name)).ToQueryString();
        Assert.Contains("parseDateTimeBestEffort(", sql);
    }

    [Fact]
    public void ParseDateTimeBestEffortOrNull_Emits_parseDateTimeBestEffortOrNull()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ParseDateTimeBestEffortOrNull(e.Name)).ToQueryString();
        Assert.Contains("parseDateTimeBestEffortOrNull(", sql);
    }

    [Fact]
    public void ParseDateTimeBestEffortOrZero_Emits_parseDateTimeBestEffortOrZero()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Select(e => EfClass.Functions.ParseDateTimeBestEffortOrZero(e.Name)).ToQueryString();
        Assert.Contains("parseDateTimeBestEffortOrZero(", sql);
    }

    // ---- 1d — datetime extras ----

    [Fact]
    public void DateTrunc_Emits_dateTrunc_WithLowercaseUnit()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.DateTrunc(ClickHouseIntervalUnit.Day, e.CreatedAt))
            .ToQueryString();
        Assert.Contains("dateTrunc(", sql);
        // Unit is lowercased per ConvertIntervalUnitArgs
        Assert.Contains("'day'", sql);
    }

    [Fact]
    public void ToStartOfInterval_Emits_toStartOfInterval_WithInterval()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.ToStartOfInterval(e.CreatedAt, 15, ClickHouseIntervalUnit.Minute))
            .ToQueryString();
        Assert.Contains("toStartOfInterval(", sql);
        // EF.CH lowers our toIntervalUnit(value) wrapper to CH's literal INTERVAL syntax.
        Assert.Contains("INTERVAL 15 MINUTE", sql);
    }

    [Fact]
    public void ToTimeZone_Emits_toTimeZone()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.ToTimeZone(e.CreatedAt, "Europe/London"))
            .ToQueryString();
        Assert.Contains("toTimeZone(", sql);
        Assert.Contains("'Europe/London'", sql);
    }

    [Fact]
    public void TimeZoneOf_Emits_timeZoneOf()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.TimeZoneOf(e.CreatedAt))
            .ToQueryString();
        Assert.Contains("timeZoneOf(", sql);
    }

    [Fact]
    public void Now64_Emits_now64_WithPrecision()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.Now64(6))
            .ToQueryString();
        Assert.Contains("now64(", sql);
        Assert.Contains("6", sql);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new TestDbContext(options);
    }
}
