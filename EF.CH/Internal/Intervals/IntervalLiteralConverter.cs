using System.Globalization;
using System.Text.RegularExpressions;

namespace EF.CH.Internal.Intervals;

/// <summary>
/// Conversions between <see cref="TimeSpan"/> / <see cref="ClickHouseIntervalUnit"/> and
/// the ClickHouse interval literal fragment used inside <c>INTERVAL n UNIT</c>.
/// Reuses the existing <see cref="ClickHouseInterval"/> struct's units.
/// </summary>
public static class IntervalLiteralConverter
{
    private static readonly Regex Pattern = new(
        @"^\s*(?<count>\d+)\s+(?<unit>NANOSECOND|MICROSECOND|MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR)S?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// Render <c>"&lt;count&gt; &lt;UNIT&gt;"</c> (the body that follows <c>INTERVAL</c>).
    /// </summary>
    public static string Format(long count, ClickHouseIntervalUnit unit)
    {
        if (count <= 0)
            throw new ArgumentOutOfRangeException(nameof(count), count, "Interval count must be positive.");
        return $"{count.ToString(CultureInfo.InvariantCulture)} {unit.ToString().ToUpperInvariant()}";
    }

    /// <summary>
    /// Convert a <see cref="TimeSpan"/> to the largest exact ClickHouse unit that divides it.
    /// </summary>
    public static string FromTimeSpan(TimeSpan span)
    {
        if (span <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(span), span, "Interval must be positive.");

        var ticks = span.Ticks;
        const long ticksPerWeek = TimeSpan.TicksPerDay * 7;
        if (ticks % ticksPerWeek == 0) return Format(ticks / ticksPerWeek, ClickHouseIntervalUnit.Week);
        if (ticks % TimeSpan.TicksPerDay == 0) return Format(ticks / TimeSpan.TicksPerDay, ClickHouseIntervalUnit.Day);
        if (ticks % TimeSpan.TicksPerHour == 0) return Format(ticks / TimeSpan.TicksPerHour, ClickHouseIntervalUnit.Hour);
        if (ticks % TimeSpan.TicksPerMinute == 0) return Format(ticks / TimeSpan.TicksPerMinute, ClickHouseIntervalUnit.Minute);
        if (ticks % TimeSpan.TicksPerSecond == 0) return Format(ticks / TimeSpan.TicksPerSecond, ClickHouseIntervalUnit.Second);

        throw new ArgumentOutOfRangeException(nameof(span), span,
            "Interval is not a whole number of seconds; use the (count, ClickHouseIntervalUnit) overload for sub-second or calendar units.");
    }

    /// <summary>
    /// Parse a literal fragment such as <c>"5 MINUTE"</c> back to its components.
    /// Returns null on failure.
    /// </summary>
    public static (long Count, ClickHouseIntervalUnit Unit)? TryParse(string? literal)
    {
        if (string.IsNullOrWhiteSpace(literal)) return null;
        var match = Pattern.Match(literal);
        if (!match.Success) return null;

        if (!long.TryParse(match.Groups["count"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var count))
            return null;

        var unitRaw = match.Groups["unit"].Value.ToUpperInvariant();
        ClickHouseIntervalUnit? unit = unitRaw switch
        {
            "SECOND" => ClickHouseIntervalUnit.Second,
            "MINUTE" => ClickHouseIntervalUnit.Minute,
            "HOUR" => ClickHouseIntervalUnit.Hour,
            "DAY" => ClickHouseIntervalUnit.Day,
            "WEEK" => ClickHouseIntervalUnit.Week,
            "MONTH" => ClickHouseIntervalUnit.Month,
            "QUARTER" => ClickHouseIntervalUnit.Quarter,
            "YEAR" => ClickHouseIntervalUnit.Year,
            // Sub-second units exist in CH but aren't in our enum; reject with null.
            _ => null,
        };
        return unit is null ? null : (count, unit.Value);
    }

    /// <summary>
    /// Try to convert an interval literal back to a <see cref="TimeSpan"/>.
    /// Returns null for MONTH/QUARTER/YEAR (calendar-dependent).
    /// </summary>
    public static TimeSpan? TryToTimeSpan(string? literal)
    {
        var parsed = TryParse(literal);
        if (parsed is not { } p) return null;
        return p.Unit switch
        {
            ClickHouseIntervalUnit.Second => TimeSpan.FromSeconds(p.Count),
            ClickHouseIntervalUnit.Minute => TimeSpan.FromMinutes(p.Count),
            ClickHouseIntervalUnit.Hour => TimeSpan.FromHours(p.Count),
            ClickHouseIntervalUnit.Day => TimeSpan.FromDays(p.Count),
            ClickHouseIntervalUnit.Week => TimeSpan.FromDays(p.Count * 7),
            _ => null,
        };
    }
}
