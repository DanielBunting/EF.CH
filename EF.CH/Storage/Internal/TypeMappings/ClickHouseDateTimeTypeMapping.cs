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
    /// The resolved TimeZoneInfo for literal-side wall-clock conversion.
    /// </summary>
    private TimeZoneInfo TimeZoneInfo { get; }

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
        TimeZoneInfo = ResolveTimeZone(timeZone);
    }

    protected ClickHouseDateTimeTypeMapping(
        RelationalTypeMappingParameters parameters,
        int precision,
        string? timeZone)
        : base(parameters)
    {
        Precision = precision;
        TimeZone = timeZone;
        TimeZoneInfo = ResolveTimeZone(timeZone);
    }

    private static string BuildStoreType(int precision, string? timeZone)
    {
        if (timeZone is null)
        {
            return $"DateTime64({precision})";
        }
        return $"DateTime64({precision}, '{timeZone}')";
    }

    private static TimeZoneInfo ResolveTimeZone(string? timeZone)
    {
        if (string.IsNullOrEmpty(timeZone) ||
            timeZone.Equals("UTC", StringComparison.OrdinalIgnoreCase))
        {
            return TimeZoneInfo.Utc;
        }
        return TimeZoneInfo.FindSystemTimeZoneById(timeZone);
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

        if (TimeZoneInfo == TimeZoneInfo.Utc)
        {
            return $"'{dateTime.ToString(format, CultureInfo.InvariantCulture)}'";
        }

        // Non-UTC TZ: emit UTC + Z via parseDateTime64BestEffort. A plain
        // wall-clock literal would be reinterpreted in the column's TZ by CH,
        // collapsing the spring-forward gap and indistinguishing the fall-back
        // hour. UTC + Z is unambiguous across DST transitions.
        var utc = dateTime.Kind switch
        {
            DateTimeKind.Utc => dateTime,
            DateTimeKind.Local => dateTime.ToUniversalTime(),
            _ => DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)
        };
        var pSafe = Precision >= 1 ? Precision : 1;
        return $"parseDateTime64BestEffort('{utc.ToString(format, CultureInfo.InvariantCulture)}Z', {pSafe}, '{TimeZone}')";
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
/// Type mapping for ClickHouse Date / Date32 columns when the property is declared as DateTime.
/// The driver materialises Date and Date32 columns as DateTime, so no value converter is needed.
/// </summary>
public class ClickHouseDateTimeAsDateTypeMapping : RelationalTypeMapping
{
    public ClickHouseDateTimeAsDateTypeMapping(string storeType = "Date32")
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateTime)),
            storeType,
            StoreTypePostfix.None,
            System.Data.DbType.Date))
    {
    }

    protected ClickHouseDateTimeAsDateTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTimeAsDateTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var date = ((DateTime)value).Date;
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

    /// <summary>
    /// Converter: writes the UTC <see cref="DateTime"/> for parameter binding;
    /// reads it back as a <see cref="DateTimeOffset"/> in the column's timezone.
    /// </summary>
    private static ValueConverter<DateTimeOffset, DateTime> CreateConverter(string? timeZone)
    {
        var tz = ResolveTimeZone(timeZone);
        return new ValueConverter<DateTimeOffset, DateTime>(
            dto => dto.UtcDateTime,
            dt => ConvertToDateTimeOffset(dt, tz));
    }

    /// <summary>
    /// Converts the driver's materialised value for a <c>DateTime64(P, 'TZ')</c>
    /// column into a <see cref="DateTimeOffset"/>. ClickHouse.Driver returns the
    /// local-in-TZ wall-clock as a <see cref="DateTime"/>, with
    /// <see cref="DateTimeKind.Utc"/> when the column's offset at that instant
    /// is zero (e.g. Europe/London during GMT, including the post-fall-back
    /// occurrence of an ambiguous hour) and <see cref="DateTimeKind.Unspecified"/>
    /// when a non-zero offset is in effect. We use that distinction to pick the
    /// correct offset across DST transitions without losing information at the
    /// fall-back ambiguous hour.
    /// </summary>
    private static DateTimeOffset ConvertToDateTimeOffset(DateTime driverValue, TimeZoneInfo tz)
    {
        if (tz == TimeZoneInfo.Utc)
        {
            return new DateTimeOffset(DateTime.SpecifyKind(driverValue, DateTimeKind.Utc), TimeSpan.Zero);
        }

        if (driverValue.Kind == DateTimeKind.Utc)
        {
            // Driver's signal that the offset at this instant is zero; the
            // wall-clock value already equals the UTC instant.
            return new DateTimeOffset(driverValue, TimeSpan.Zero);
        }

        var local = DateTime.SpecifyKind(driverValue, DateTimeKind.Unspecified);
        TimeSpan offset;
        if (tz.IsAmbiguousTime(local))
        {
            // Fall-back ambiguous hour. Kind=Utc would have selected the zero
            // offset; Kind=Unspecified means we want the non-zero (DST) one.
            var offsets = tz.GetAmbiguousTimeOffsets(local);
            offset = offsets[0] != TimeSpan.Zero ? offsets[0] : offsets[1];
        }
        else
        {
            offset = tz.GetUtcOffset(local);
        }
        return new DateTimeOffset(local, offset);
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
            4 => "yyyy-MM-dd HH:mm:ss.ffff",
            5 => "yyyy-MM-dd HH:mm:ss.fffff",
            6 => "yyyy-MM-dd HH:mm:ss.ffffff",
            _ => "yyyy-MM-dd HH:mm:ss.fffffff"
        };

        if (TimeZoneInfo == TimeZoneInfo.Utc)
        {
            return $"'{utc.ToString(format, CultureInfo.InvariantCulture)}'";
        }

        // Non-UTC TZ: emit UTC + Z via parseDateTime64BestEffort. A plain
        // wall-clock literal would be reinterpreted in the column's TZ by CH,
        // collapsing the spring-forward gap and indistinguishing the fall-back
        // hour. UTC + Z is unambiguous across DST transitions.
        var pSafe = Precision >= 1 ? Precision : 1;
        return $"parseDateTime64BestEffort('{utc.ToString(format, CultureInfo.InvariantCulture)}Z', {pSafe}, '{TimeZone}')";
    }
}
