using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse's safe-cast
/// function family — <c>to&lt;Type&gt;OrNull</c> and <c>to&lt;Type&gt;OrZero</c>
/// variants that return null / zero on parse failure rather than throwing.
/// Use these when ingesting user-typed data where a single bad row should not
/// fail the whole query. These are LINQ translation stubs — calling them
/// outside of a query will throw.
/// </summary>
public static class ClickHouseSafeCastDbFunctionsExtensions
{
    // ---- Integer parsing ----

    /// <summary>Translates to <c>toInt8OrNull(s)</c>.</summary>
    public static sbyte? ToInt8OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toInt16OrNull(s)</c>.</summary>
    public static short? ToInt16OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toInt32OrNull(s)</c>.</summary>
    public static int? ToInt32OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toInt64OrNull(s)</c>.</summary>
    public static long? ToInt64OrNull(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>toUInt8OrNull(s)</c>.</summary>
    public static byte? ToUInt8OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toUInt16OrNull(s)</c>.</summary>
    public static ushort? ToUInt16OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toUInt32OrNull(s)</c>.</summary>
    public static uint? ToUInt32OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toUInt64OrNull(s)</c>.</summary>
    public static ulong? ToUInt64OrNull(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>toInt8OrZero(s)</c> — returns 0 when <paramref name="s"/> is not a valid Int8.</summary>
    public static sbyte ToInt8OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toInt16OrZero(s)</c>.</summary>
    public static short ToInt16OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toInt32OrZero(s)</c>.</summary>
    public static int ToInt32OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toInt64OrZero(s)</c>.</summary>
    public static long ToInt64OrZero(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>toUInt8OrZero(s)</c>.</summary>
    public static byte ToUInt8OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toUInt16OrZero(s)</c>.</summary>
    public static ushort ToUInt16OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toUInt32OrZero(s)</c>.</summary>
    public static uint ToUInt32OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toUInt64OrZero(s)</c>.</summary>
    public static ulong ToUInt64OrZero(this DbFunctions _, string s) => throw NotSupported();

    // ---- Floating-point parsing ----

    /// <summary>Translates to <c>toFloat32OrNull(s)</c>.</summary>
    public static float? ToFloat32OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toFloat64OrNull(s)</c>.</summary>
    public static double? ToFloat64OrNull(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toFloat32OrZero(s)</c>.</summary>
    public static float ToFloat32OrZero(this DbFunctions _, string s) => throw NotSupported();
    /// <summary>Translates to <c>toFloat64OrZero(s)</c>.</summary>
    public static double ToFloat64OrZero(this DbFunctions _, string s) => throw NotSupported();

    // ---- Date / DateTime parsing ----

    /// <summary>Translates to <c>toDateOrNull(s)</c>.</summary>
    public static DateOnly? ToDateOrNull(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>toDateTimeOrNull(s)</c>.</summary>
    public static DateTime? ToDateTimeOrNull(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>
    /// Translates to <c>parseDateTimeBestEffort(s)</c>. Tolerant parser that
    /// accepts a wide range of human-readable date/time formats; throws on
    /// failure. Use <see cref="ParseDateTimeBestEffortOrNull"/> /
    /// <see cref="ParseDateTimeBestEffortOrZero"/> for non-throwing variants.
    /// </summary>
    public static DateTime ParseDateTimeBestEffort(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>parseDateTimeBestEffortOrNull(s)</c>.</summary>
    public static DateTime? ParseDateTimeBestEffortOrNull(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>parseDateTimeBestEffortOrZero(s)</c> — returns the Unix epoch on failure.</summary>
    public static DateTime ParseDateTimeBestEffortOrZero(this DbFunctions _, string s) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
