using EF.CH;
using EF.CH.Internal.Intervals;
using Xunit;

namespace EF.CH.Tests.Intervals;

public class SubSecondIntervalsTests
{
    [Fact]
    public void Nanoseconds_FactoryProducesNanosecondUnit()
    {
        var interval = ClickHouseInterval.Nanoseconds(100);
        Assert.Equal(100, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Nanosecond, interval.Unit);
    }

    [Fact]
    public void Microseconds_FactoryProducesMicrosecondUnit()
    {
        var interval = ClickHouseInterval.Microseconds(250);
        Assert.Equal(250, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Microsecond, interval.Unit);
    }

    [Fact]
    public void Milliseconds_FactoryProducesMillisecondUnit()
    {
        var interval = ClickHouseInterval.Milliseconds(500);
        Assert.Equal(500, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Millisecond, interval.Unit);
    }

    [Fact]
    public void Milliseconds_ToSqlMatchesExpectedShape()
    {
        var sql = ClickHouseInterval.Milliseconds(500).ToSql();
        Assert.Equal("INTERVAL 500 MILLISECOND", sql);
    }

    [Fact]
    public void IntervalLiteralConverter_RoundTripsMillisecond()
    {
        var parsed = IntervalLiteralConverter.TryParse("500 MILLISECOND");
        Assert.NotNull(parsed);
        Assert.Equal(500L, parsed!.Value.Count);
        Assert.Equal(ClickHouseIntervalUnit.Millisecond, parsed.Value.Unit);
    }

    [Fact]
    public void IntervalLiteralConverter_RoundTripsMicrosecond()
    {
        var parsed = IntervalLiteralConverter.TryParse("123 MICROSECOND");
        Assert.NotNull(parsed);
        Assert.Equal(123L, parsed!.Value.Count);
        Assert.Equal(ClickHouseIntervalUnit.Microsecond, parsed.Value.Unit);
    }

    [Fact]
    public void IntervalLiteralConverter_RoundTripsNanosecond()
    {
        var parsed = IntervalLiteralConverter.TryParse("999 NANOSECOND");
        Assert.NotNull(parsed);
        Assert.Equal(999L, parsed!.Value.Count);
        Assert.Equal(ClickHouseIntervalUnit.Nanosecond, parsed.Value.Unit);
    }

    [Fact]
    public void Format_RendersMillisecondLiteral()
    {
        Assert.Equal("500 MILLISECOND", IntervalLiteralConverter.Format(500, ClickHouseIntervalUnit.Millisecond));
    }
}
