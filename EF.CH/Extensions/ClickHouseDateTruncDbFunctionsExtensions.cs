using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse date truncation and bucketing functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseDateTruncDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>toStartOfYear(dt)</c>.
    /// Rounds down to the first day of the year.
    /// </summary>
    public static DateTime ToStartOfYear(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfQuarter(dt)</c>.
    /// Rounds down to the first day of the quarter.
    /// </summary>
    public static DateTime ToStartOfQuarter(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfMonth(dt)</c>.
    /// Rounds down to the first day of the month.
    /// </summary>
    public static DateTime ToStartOfMonth(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfWeek(dt)</c>.
    /// Rounds down to the start of the week (Sunday by default).
    /// </summary>
    public static DateTime ToStartOfWeek(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toMonday(dt)</c>.
    /// Rounds down to the nearest Monday.
    /// </summary>
    public static DateTime ToMonday(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfDay(dt)</c>.
    /// Rounds down to the start of the day.
    /// </summary>
    public static DateTime ToStartOfDay(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfHour(dt)</c>.
    /// Rounds down to the start of the hour.
    /// </summary>
    public static DateTime ToStartOfHour(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfMinute(dt)</c>.
    /// Rounds down to the start of the minute.
    /// </summary>
    public static DateTime ToStartOfMinute(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfFiveMinutes(dt)</c>.
    /// Rounds down to the nearest five-minute interval.
    /// </summary>
    public static DateTime ToStartOfFiveMinutes(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfFifteenMinutes(dt)</c>.
    /// Rounds down to the nearest fifteen-minute interval.
    /// </summary>
    public static DateTime ToStartOfFifteenMinutes(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>dateDiff(unit, start, end)</c>.
    /// Returns the difference between two dates in the specified unit.
    /// Unit is specified via <see cref="ClickHouseIntervalUnit"/>.
    /// </summary>
    public static long DateDiff(this DbFunctions _, ClickHouseIntervalUnit unit, DateTime start, DateTime end)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfSecond(dt)</c>.
    /// Rounds down to the start of the second (strips sub-second precision).
    /// </summary>
    public static DateTime ToStartOfSecond(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfTenMinutes(dt)</c>.
    /// Rounds down to the nearest ten-minute interval.
    /// </summary>
    public static DateTime ToStartOfTenMinutes(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toUnixTimestamp(dt)</c>.
    /// Returns the Unix timestamp (seconds since epoch) for the given DateTime.
    /// </summary>
    public static long ToUnixTimestamp(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>fromUnixTimestamp(ts)</c>.
    /// Converts a Unix timestamp (seconds since epoch) to DateTime.
    /// </summary>
    public static DateTime FromUnixTimestamp(this DbFunctions _, long ts)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>fromUnixTimestamp64Milli(ts)</c>.
    /// Converts a Unix timestamp in milliseconds to DateTime64.
    /// </summary>
    public static DateTime FromUnixTimestamp64Milli(this DbFunctions _, long ts)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeYearNum(dt)</c>.
    /// Returns the number of the year from a past fixed point.
    /// </summary>
    public static int ToRelativeYearNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeMonthNum(dt)</c>.
    /// Returns the number of the month from a past fixed point.
    /// </summary>
    public static long ToRelativeMonthNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeWeekNum(dt)</c>.
    /// Returns the number of the week from a past fixed point.
    /// </summary>
    public static long ToRelativeWeekNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeDayNum(dt)</c>.
    /// Returns the number of the day from a past fixed point.
    /// </summary>
    public static long ToRelativeDayNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeHourNum(dt)</c>.
    /// Returns the number of the hour from a past fixed point.
    /// </summary>
    public static long ToRelativeHourNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeMinuteNum(dt)</c>.
    /// Returns the number of the minute from a past fixed point.
    /// </summary>
    public static long ToRelativeMinuteNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toRelativeSecondNum(dt)</c>.
    /// Returns the number of the second from a past fixed point.
    /// </summary>
    public static long ToRelativeSecondNum(this DbFunctions _, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>date_add(unit, value, dt)</c>.
    /// Adds a specified number of units to a DateTime.
    /// Unit is specified via <see cref="ClickHouseIntervalUnit"/>.
    /// </summary>
    public static DateTime DateAdd(this DbFunctions _, ClickHouseIntervalUnit unit, int value, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>date_sub(unit, value, dt)</c>.
    /// Subtracts a specified number of units from a DateTime.
    /// Unit is specified via <see cref="ClickHouseIntervalUnit"/>.
    /// </summary>
    public static DateTime DateSub(this DbFunctions _, ClickHouseIntervalUnit unit, int value, DateTime dt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>age(unit, start, end)</c>.
    /// Returns the difference between two dates in the specified unit, accounting for calendar boundaries.
    /// Unit is specified via <see cref="ClickHouseIntervalUnit"/>.
    /// </summary>
    public static long Age(this DbFunctions _, ClickHouseIntervalUnit unit, DateTime start, DateTime end)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toStartOfInterval(dt, INTERVAL n unit)</c>.
    /// Rounds down a DateTime to the start of the specified interval.
    /// Unit is specified via <see cref="ClickHouseIntervalUnit"/>.
    /// </summary>
    public static DateTime ToStartOfInterval(this DbFunctions _, DateTime dt, int value, ClickHouseIntervalUnit unit)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
