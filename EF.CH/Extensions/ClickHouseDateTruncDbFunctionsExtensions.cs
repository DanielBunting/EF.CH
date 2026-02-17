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
    /// Translates to ClickHouse <c>dateDiff('unit', start, end)</c>.
    /// Returns the difference between two dates in the specified unit.
    /// Valid units: 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'.
    /// </summary>
    public static long DateDiff(this DbFunctions _, string unit, DateTime start, DateTime end)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
