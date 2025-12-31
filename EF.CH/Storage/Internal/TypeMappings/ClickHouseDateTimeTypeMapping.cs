using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse DateTime64 type with configurable precision.
/// DateTime64 stores datetime with sub-second precision (0-9 decimal places).
/// </summary>
public class ClickHouseDateTimeTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The precision (number of decimal places for seconds).
    /// 3 = milliseconds, 6 = microseconds, 9 = nanoseconds
    /// </summary>
    public new int Precision { get; }

    /// <summary>
    /// Optional timezone for the DateTime64 type.
    /// </summary>
    public string? TimeZone { get; }

    /// <summary>
    /// Creates a DateTime64 type mapping with the specified precision.
    /// </summary>
    /// <param name="precision">Decimal places for sub-second precision (default: 3 for milliseconds).</param>
    /// <param name="timeZone">Optional timezone (e.g., "UTC", "America/New_York").</param>
    public ClickHouseDateTimeTypeMapping(int precision = 3, string? timeZone = null)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateTime)),
            BuildStoreType(precision, timeZone),
            StoreTypePostfix.None,
            System.Data.DbType.DateTime2,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: precision,
            scale: null))
    {
        Precision = precision;
        TimeZone = timeZone;
    }

    protected ClickHouseDateTimeTypeMapping(
        RelationalTypeMappingParameters parameters,
        int precision,
        string? timeZone)
        : base(parameters)
    {
        Precision = precision;
        TimeZone = timeZone;
    }

    private static string BuildStoreType(int precision, string? timeZone)
    {
        if (timeZone is null)
        {
            return $"DateTime64({precision})";
        }
        return $"DateTime64({precision}, '{timeZone}')";
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTimeTypeMapping(parameters, Precision, TimeZone);

    /// <summary>
    /// Generates a SQL literal for a DateTime value.
    /// ClickHouse accepts ISO 8601 format: 'YYYY-MM-DD HH:MM:SS.fff'
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dateTime = (DateTime)value;

        // Format with appropriate precision
        var format = Precision switch
        {
            0 => "yyyy-MM-dd HH:mm:ss",
            1 => "yyyy-MM-dd HH:mm:ss.f",
            2 => "yyyy-MM-dd HH:mm:ss.ff",
            3 => "yyyy-MM-dd HH:mm:ss.fff",
            4 => "yyyy-MM-dd HH:mm:ss.ffff",
            5 => "yyyy-MM-dd HH:mm:ss.fffff",
            6 => "yyyy-MM-dd HH:mm:ss.ffffff",
            _ => "yyyy-MM-dd HH:mm:ss.fffffff"
        };

        return $"'{dateTime.ToString(format, CultureInfo.InvariantCulture)}'";
    }
}

/// <summary>
/// Type mapping for ClickHouse Date type (date only, no time component).
/// </summary>
public class ClickHouseDateTypeMapping : RelationalTypeMapping
{
    public ClickHouseDateTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateOnly)),
            "Date",
            StoreTypePostfix.None,
            System.Data.DbType.Date))
    {
    }

    protected ClickHouseDateTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var date = (DateOnly)value;
        return $"'{date:yyyy-MM-dd}'";
    }
}

/// <summary>
/// Type mapping for ClickHouse Date32 type (extended date range).
/// Supports dates from 1900-01-01 to 2299-12-31.
/// </summary>
public class ClickHouseDate32TypeMapping : RelationalTypeMapping
{
    public ClickHouseDate32TypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateOnly)),
            "Date32",
            StoreTypePostfix.None,
            System.Data.DbType.Date))
    {
    }

    protected ClickHouseDate32TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDate32TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var date = (DateOnly)value;
        return $"'{date:yyyy-MM-dd}'";
    }
}

/// <summary>
/// Type mapping for DateTimeOffset, stored as DateTime64 with configurable timezone.
/// Converts DateTimeOffset to UTC DateTime on write, and applies the configured timezone on read.
/// </summary>
/// <remarks>
/// <para>
/// When a timezone is specified, reading a DateTime64 value from ClickHouse will return a
/// DateTimeOffset with the offset calculated for that timezone at that point in time,
/// properly accounting for DST transitions.
/// </para>
/// <para>
/// Writing always converts to UTC to ensure the correct instant is stored, regardless
/// of the input offset.
/// </para>
/// </remarks>
public class ClickHouseDateTimeOffsetTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// The precision (number of decimal places for seconds).
    /// 3 = milliseconds, 6 = microseconds, 9 = nanoseconds
    /// </summary>
    public new int Precision { get; }

    /// <summary>
    /// The IANA timezone name for this mapping (e.g., "America/New_York", "Europe/London").
    /// When null or "UTC", offsets will be zero.
    /// </summary>
    public string? TimeZone { get; }

    /// <summary>
    /// The resolved TimeZoneInfo for conversion operations.
    /// </summary>
    private TimeZoneInfo TimeZoneInfo { get; }

    /// <summary>
    /// Creates a DateTimeOffset type mapping with the specified precision and timezone.
    /// </summary>
    /// <param name="precision">Decimal places for sub-second precision (default: 3 for milliseconds).</param>
    /// <param name="timeZone">Optional IANA timezone name. Defaults to UTC.</param>
    public ClickHouseDateTimeOffsetTypeMapping(int precision = 3, string? timeZone = null)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(DateTimeOffset),
                CreateConverter(timeZone)),
            BuildStoreType(precision, timeZone ?? "UTC"),
            StoreTypePostfix.None,
            System.Data.DbType.DateTime2,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: precision,
            scale: null))
    {
        Precision = precision;
        TimeZone = timeZone;
        TimeZoneInfo = ResolveTimeZone(timeZone);
    }

    protected ClickHouseDateTimeOffsetTypeMapping(
        RelationalTypeMappingParameters parameters,
        int precision,
        string? timeZone)
        : base(parameters)
    {
        Precision = precision;
        TimeZone = timeZone;
        TimeZoneInfo = ResolveTimeZone(timeZone);
    }

    private static string BuildStoreType(int precision, string timeZone)
        => $"DateTime64({precision}, '{timeZone}')";

    /// <summary>
    /// Creates a value converter that writes as UTC and reads using the specified timezone.
    /// </summary>
    private static ValueConverter<DateTimeOffset, DateTime> CreateConverter(string? timeZone)
    {
        var tz = ResolveTimeZone(timeZone);
        return new ValueConverter<DateTimeOffset, DateTime>(
            // Write: always convert to UTC for storage
            dto => dto.UtcDateTime,
            // Read: interpret as UTC, then convert to target timezone with correct offset
            dt => ConvertToDateTimeOffset(dt, tz));
    }

    /// <summary>
    /// Converts a UTC DateTime to a DateTimeOffset in the specified timezone.
    /// </summary>
    private static DateTimeOffset ConvertToDateTimeOffset(DateTime utcDateTime, TimeZoneInfo tz)
    {
        // Ensure the DateTime is treated as UTC
        var utc = DateTime.SpecifyKind(utcDateTime, DateTimeKind.Utc);

        if (tz == TimeZoneInfo.Utc)
        {
            // Fast path for UTC - no conversion needed
            return new DateTimeOffset(utc, TimeSpan.Zero);
        }

        // Convert from UTC to the target timezone
        var local = TimeZoneInfo.ConvertTimeFromUtc(utc, tz);
        var offset = tz.GetUtcOffset(local);
        return new DateTimeOffset(local, offset);
    }

    /// <summary>
    /// Resolves an IANA timezone name to a TimeZoneInfo.
    /// </summary>
    private static TimeZoneInfo ResolveTimeZone(string? timeZone)
    {
        if (string.IsNullOrEmpty(timeZone) ||
            timeZone.Equals("UTC", StringComparison.OrdinalIgnoreCase))
        {
            return TimeZoneInfo.Utc;
        }

        // .NET 6+ supports IANA timezone names on all platforms
        return TimeZoneInfo.FindSystemTimeZoneById(timeZone);
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTimeOffsetTypeMapping(parameters, Precision, TimeZone);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // When a value converter is used, EF Core may pass the converted DateTime
        // instead of the original DateTimeOffset
        DateTime utc;
        if (value is DateTimeOffset dto)
        {
            utc = dto.UtcDateTime;
        }
        else if (value is DateTime dt)
        {
            utc = dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
        }
        else
        {
            throw new InvalidOperationException($"Unexpected value type: {value.GetType()}");
        }

        var format = Precision switch
        {
            0 => "yyyy-MM-dd HH:mm:ss",
            1 => "yyyy-MM-dd HH:mm:ss.f",
            2 => "yyyy-MM-dd HH:mm:ss.ff",
            3 => "yyyy-MM-dd HH:mm:ss.fff",
            _ => "yyyy-MM-dd HH:mm:ss.ffffff"
        };

        return $"'{utc.ToString(format, CultureInfo.InvariantCulture)}'";
    }
}
