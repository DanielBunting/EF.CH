using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Time type (time of day without date).
/// Maps to .NET TimeOnly type.
/// </summary>
/// <remarks>
/// ClickHouse Time type stores time as nanoseconds since midnight.
/// Format: HH:MM:SS[.fffffffff] (up to 9 decimal places).
/// </remarks>
public class ClickHouseTimeTypeMapping : RelationalTypeMapping
{
    public ClickHouseTimeTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(TimeOnly)),
            "Time",
            StoreTypePostfix.None,
            System.Data.DbType.Time))
    {
    }

    protected ClickHouseTimeTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseTimeTypeMapping(parameters);

    /// <summary>
    /// Generates a SQL literal for a TimeOnly value.
    /// ClickHouse accepts time format: 'HH:MM:SS.fff'
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var time = (TimeOnly)value;

        // Use millisecond precision if there are fractional seconds, otherwise just HH:mm:ss
        if (time.Millisecond > 0 || time.Microsecond > 0)
        {
            // Format with microsecond precision (6 decimal places)
            return $"'{time.ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture)}'";
        }

        return $"'{time.ToString("HH:mm:ss", CultureInfo.InvariantCulture)}'";
    }
}

/// <summary>
/// Type mapping for TimeSpan (duration) stored as Int64 nanoseconds in ClickHouse.
/// </summary>
/// <remarks>
/// ClickHouse doesn't have a native duration/interval type that maps well to TimeSpan.
/// We store as Int64 nanoseconds for maximum precision and range.
/// This allows arithmetic operations and comparisons to work correctly.
///
/// Range: TimeSpan.MinValue to TimeSpan.MaxValue (approx ±10,675,199 days).
/// Precision: 100-nanosecond ticks (TimeSpan's native precision).
/// </remarks>
public class ClickHouseTimeSpanTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// Conversion factor from TimeSpan ticks (100ns) to nanoseconds.
    /// </summary>
    private const long TicksToNanoseconds = 100;

    public ClickHouseTimeSpanTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(TimeSpan),
                converter: new TimeSpanToNanosecondsConverter()),
            "Int64",
            StoreTypePostfix.None,
            System.Data.DbType.Int64))
    {
    }

    protected ClickHouseTimeSpanTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseTimeSpanTypeMapping(parameters);

    /// <summary>
    /// Generates a SQL literal for a TimeSpan value as nanoseconds.
    /// When using a converter, the value may already be converted to Int64.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        // The value might already be converted to long by the converter
        if (value is long nanoseconds)
        {
            return nanoseconds.ToString(CultureInfo.InvariantCulture);
        }

        // Original TimeSpan value (direct call)
        var timeSpan = (TimeSpan)value;
        return (timeSpan.Ticks * TicksToNanoseconds).ToString(CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// Value converter for TimeSpan to/from Int64 (nanoseconds).
/// </summary>
internal class TimeSpanToNanosecondsConverter : Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter<TimeSpan, long>
{
    private const long TicksToNanoseconds = 100;

    public TimeSpanToNanosecondsConverter()
        : base(
            ts => ts.Ticks * TicksToNanoseconds,
            ns => TimeSpan.FromTicks(ns / TicksToNanoseconds))
    {
    }
}
