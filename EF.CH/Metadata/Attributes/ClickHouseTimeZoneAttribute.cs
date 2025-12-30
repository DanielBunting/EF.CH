namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Specifies the timezone for a DateTimeOffset column.
/// </summary>
/// <remarks>
/// <para>
/// When reading DateTimeOffset values from ClickHouse, the timezone is used to calculate
/// the appropriate offset, accounting for DST transitions. Values are always stored as UTC
/// in ClickHouse; the timezone determines how the offset is calculated when reading.
/// </para>
/// <para>
/// Use IANA timezone names (e.g., "America/New_York", "Europe/London", "Asia/Tokyo").
/// These are supported on all platforms with .NET 6+.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Event
/// {
///     public Guid Id { get; set; }
///
///     [ClickHouseTimeZone("America/New_York")]
///     public DateTimeOffset CreatedAt { get; set; }
///
///     [ClickHouseTimeZone("Europe/London")]
///     public DateTimeOffset? ScheduledAt { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class ClickHouseTimeZoneAttribute : Attribute
{
    /// <summary>
    /// Gets the IANA timezone name.
    /// </summary>
    public string TimeZone { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClickHouseTimeZoneAttribute"/> class.
    /// </summary>
    /// <param name="timeZone">The IANA timezone name (e.g., "America/New_York", "Europe/London").</param>
    /// <exception cref="ArgumentException">If timeZone is null or whitespace.</exception>
    public ClickHouseTimeZoneAttribute(string timeZone)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(timeZone);
        TimeZone = timeZone;
    }
}
