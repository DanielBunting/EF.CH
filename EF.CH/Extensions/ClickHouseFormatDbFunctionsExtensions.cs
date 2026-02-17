using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse formatting and parsing functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseFormatDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>formatDateTime(dt, fmt)</c>.
    /// Formats a DateTime using the specified format string.
    /// </summary>
    public static string FormatDateTime(this DbFunctions _, DateTime dt, string fmt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>formatReadableSize(bytes)</c>.
    /// Formats a byte count as a human-readable string (e.g. "1.00 GiB").
    /// </summary>
    public static string FormatReadableSize(this DbFunctions _, long bytes)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>formatReadableDecimalSize(bytes)</c>.
    /// Formats a byte count as a human-readable string using decimal units (e.g. "1.00 GB").
    /// </summary>
    public static string FormatReadableDecimalSize(this DbFunctions _, long bytes)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>formatReadableQuantity(n)</c>.
    /// Formats a number as a human-readable string (e.g. "1.00 million").
    /// </summary>
    public static string FormatReadableQuantity(this DbFunctions _, double n)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>formatReadableTimeDelta(seconds)</c>.
    /// Formats a number of seconds as a human-readable time delta (e.g. "1 hour, 30 minutes").
    /// </summary>
    public static string FormatReadableTimeDelta(this DbFunctions _, double seconds)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>parseDateTime(s, fmt)</c>.
    /// Parses a string into a DateTime using the specified format string.
    /// </summary>
    public static DateTime ParseDateTime(this DbFunctions _, string s, string fmt)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
