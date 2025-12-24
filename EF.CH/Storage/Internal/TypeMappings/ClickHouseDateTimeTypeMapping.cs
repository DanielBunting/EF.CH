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
/// Type mapping for DateTimeOffset, stored as DateTime64 with UTC timezone.
/// Converts DateTimeOffset to UTC DateTime on write, and assumes UTC on read.
/// </summary>
public class ClickHouseDateTimeOffsetTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// Value converter that normalizes DateTimeOffset to UTC DateTime.
    /// This ensures parameterized queries use the correct UTC instant.
    /// </summary>
    private static readonly ValueConverter<DateTimeOffset, DateTime> DateTimeOffsetConverter =
        new(
            dto => dto.UtcDateTime,
            dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc), TimeSpan.Zero));

    public new int Precision { get; }

    public ClickHouseDateTimeOffsetTypeMapping(int precision = 3)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(DateTimeOffset),
                DateTimeOffsetConverter),
            $"DateTime64({precision}, 'UTC')",
            StoreTypePostfix.None,
            System.Data.DbType.DateTime2,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: precision,
            scale: null))
    {
        Precision = precision;
    }

    protected ClickHouseDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters, int precision)
        : base(parameters)
    {
        Precision = precision;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDateTimeOffsetTypeMapping(parameters, Precision);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var dto = (DateTimeOffset)value;
        var utc = dto.UtcDateTime;

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
