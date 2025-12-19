namespace EF.CH;

/// <summary>
/// Represents a ClickHouse INTERVAL value for use in TTL expressions and other time-based operations.
/// </summary>
/// <remarks>
/// <para>
/// This type provides a type-safe way to specify intervals that maps to ClickHouse's INTERVAL syntax.
/// Unlike <see cref="TimeSpan"/>, this type supports calendar-based units like months, quarters, and years.
/// </para>
/// <para>
/// For day-based intervals and smaller, you can use either this type or <see cref="TimeSpan"/>.
/// For months, quarters, and years, you must use this type as TimeSpan cannot represent them accurately.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// // In entity configuration
/// entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Days(30));
/// entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Months(1));
/// entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Years(1));
/// </code>
/// </example>
public readonly struct ClickHouseInterval : IEquatable<ClickHouseInterval>
{
    /// <summary>
    /// Gets the numeric value of the interval.
    /// </summary>
    public int Value { get; }

    /// <summary>
    /// Gets the unit of the interval.
    /// </summary>
    public ClickHouseIntervalUnit Unit { get; }

    private ClickHouseInterval(int value, ClickHouseIntervalUnit unit)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);
        Value = value;
        Unit = unit;
    }

    /// <summary>
    /// Creates an interval of the specified number of seconds.
    /// </summary>
    public static ClickHouseInterval Seconds(int value) => new(value, ClickHouseIntervalUnit.Second);

    /// <summary>
    /// Creates an interval of the specified number of minutes.
    /// </summary>
    public static ClickHouseInterval Minutes(int value) => new(value, ClickHouseIntervalUnit.Minute);

    /// <summary>
    /// Creates an interval of the specified number of hours.
    /// </summary>
    public static ClickHouseInterval Hours(int value) => new(value, ClickHouseIntervalUnit.Hour);

    /// <summary>
    /// Creates an interval of the specified number of days.
    /// </summary>
    public static ClickHouseInterval Days(int value) => new(value, ClickHouseIntervalUnit.Day);

    /// <summary>
    /// Creates an interval of the specified number of weeks.
    /// </summary>
    public static ClickHouseInterval Weeks(int value) => new(value, ClickHouseIntervalUnit.Week);

    /// <summary>
    /// Creates an interval of the specified number of months.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="TimeSpan"/>, this correctly represents calendar months (28-31 days).
    /// </remarks>
    public static ClickHouseInterval Months(int value) => new(value, ClickHouseIntervalUnit.Month);

    /// <summary>
    /// Creates an interval of the specified number of quarters (3 months each).
    /// </summary>
    public static ClickHouseInterval Quarters(int value) => new(value, ClickHouseIntervalUnit.Quarter);

    /// <summary>
    /// Creates an interval of the specified number of years.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="TimeSpan"/>, this correctly represents calendar years (365-366 days).
    /// </remarks>
    public static ClickHouseInterval Years(int value) => new(value, ClickHouseIntervalUnit.Year);

    /// <summary>
    /// Converts to ClickHouse SQL INTERVAL expression (e.g., "INTERVAL 30 DAY").
    /// </summary>
    public string ToSql() => $"INTERVAL {Value} {Unit.ToString().ToUpperInvariant()}";

    /// <inheritdoc />
    public override string ToString() => ToSql();

    /// <inheritdoc />
    public bool Equals(ClickHouseInterval other) => Value == other.Value && Unit == other.Unit;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is ClickHouseInterval other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Value, Unit);

    /// <summary>
    /// Determines whether two intervals are equal.
    /// </summary>
    public static bool operator ==(ClickHouseInterval left, ClickHouseInterval right) => left.Equals(right);

    /// <summary>
    /// Determines whether two intervals are not equal.
    /// </summary>
    public static bool operator !=(ClickHouseInterval left, ClickHouseInterval right) => !left.Equals(right);
}

/// <summary>
/// ClickHouse interval units for TTL and time expressions.
/// </summary>
public enum ClickHouseIntervalUnit
{
    /// <summary>
    /// Interval in seconds.
    /// </summary>
    Second,

    /// <summary>
    /// Interval in minutes.
    /// </summary>
    Minute,

    /// <summary>
    /// Interval in hours.
    /// </summary>
    Hour,

    /// <summary>
    /// Interval in days.
    /// </summary>
    Day,

    /// <summary>
    /// Interval in weeks (7 days).
    /// </summary>
    Week,

    /// <summary>
    /// Interval in calendar months.
    /// </summary>
    Month,

    /// <summary>
    /// Interval in quarters (3 months).
    /// </summary>
    Quarter,

    /// <summary>
    /// Interval in calendar years.
    /// </summary>
    Year
}
