namespace EF.CH.Extensions;

/// <summary>
/// ClickHouse-specific functions for use in LINQ expressions.
/// These methods are translated to ClickHouse SQL at query/view definition time.
/// </summary>
/// <remarks>
/// <para>
/// These are stub methods - they throw at runtime if accidentally invoked directly.
/// They are intended for use in:
/// - Materialized view definitions with <c>AsMaterializedView()</c>
/// - LINQ queries that will be translated to ClickHouse SQL
/// </para>
/// <para>
/// Example usage in a materialized view:
/// <code>
/// entity.AsMaterializedView&lt;HourlySummary, RawEvent&gt;(
///     source: events => events,
///     query: events => events
///         .GroupBy(e => e.EventTime.ToStartOfHour())
///         .Select(g => new HourlySummary
///         {
///             Hour = g.Key,
///             EventCount = g.Count()
///         }));
/// </code>
/// </para>
/// </remarks>
public static class ClickHouseFunctions
{
    private static T Throw<T>() =>
        throw new InvalidOperationException(
            "This method is a ClickHouse translation stub and should not be invoked directly. " +
            "It is intended for use in LINQ expressions that are translated to ClickHouse SQL.");

    #region Raw SQL

    /// <summary>
    /// Embeds a raw SQL expression in a LINQ projection.
    /// The SQL string is emitted verbatim in the SELECT clause.
    /// </summary>
    /// <typeparam name="T">The CLR return type of the expression.</typeparam>
    /// <param name="sql">The raw SQL expression (e.g. <c>"quantile(0.95)(value)"</c>).</param>
    /// <returns>Never returns — this method is translated to SQL at query time.</returns>
    public static T RawSql<T>(string sql) => Throw<T>();

    #endregion

    #region Date/Time Truncation Functions

    /// <summary>
    /// Converts a DateTime to an integer in YYYYMM format.
    /// Translates to: toYYYYMM(column)
    /// </summary>
    public static int ToYYYYMM(this DateTime dateTime) => Throw<int>();

    /// <summary>
    /// Converts a DateTime to an integer in YYYYMMDD format.
    /// Translates to: toYYYYMMDD(column)
    /// </summary>
    public static int ToYYYYMMDD(this DateTime dateTime) => Throw<int>();

    /// <summary>
    /// Truncates DateTime to the start of the hour.
    /// Translates to: toStartOfHour(column)
    /// </summary>
    public static DateTime ToStartOfHour(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the day.
    /// Translates to: toStartOfDay(column)
    /// </summary>
    public static DateTime ToStartOfDay(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the week (Monday).
    /// Translates to: toStartOfWeek(column)
    /// </summary>
    public static DateTime ToStartOfWeek(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the month.
    /// Translates to: toStartOfMonth(column)
    /// </summary>
    public static DateTime ToStartOfMonth(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the quarter.
    /// Translates to: toStartOfQuarter(column)
    /// </summary>
    public static DateTime ToStartOfQuarter(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the year.
    /// Translates to: toStartOfYear(column)
    /// </summary>
    public static DateTime ToStartOfYear(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the minute.
    /// Translates to: toStartOfMinute(column)
    /// </summary>
    public static DateTime ToStartOfMinute(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the 5-minute interval.
    /// Translates to: toStartOfFiveMinutes(column)
    /// </summary>
    public static DateTime ToStartOfFiveMinutes(this DateTime dateTime) => Throw<DateTime>();

    /// <summary>
    /// Truncates DateTime to the start of the 15-minute interval.
    /// Translates to: toStartOfFifteenMinutes(column)
    /// </summary>
    public static DateTime ToStartOfFifteenMinutes(this DateTime dateTime) => Throw<DateTime>();

    #endregion

    #region Timestamp Conversion Functions

    /// <summary>
    /// Converts a DateTime to Unix timestamp in milliseconds.
    /// Translates to: toUnixTimestamp64Milli(column)
    /// </summary>
    public static long ToUnixTimestamp64Milli(this DateTime dateTime) => Throw<long>();

    /// <summary>
    /// Converts a nullable DateTime to Unix timestamp in milliseconds.
    /// </summary>
    public static long? ToUnixTimestamp64Milli(this DateTime? dateTime) => Throw<long?>();

    #endregion

    #region Hash Functions

    /// <summary>
    /// Computes the CityHash64 of a string.
    /// Translates to: cityHash64(column)
    /// </summary>
    public static ulong CityHash64(this string value) => Throw<ulong>();

    #endregion

    #region Date Extraction Functions

    /// <summary>
    /// Gets the ISO year from a DateTime.
    /// Translates to: toISOYear(column)
    /// </summary>
    public static int ToISOYear(this DateTime dateTime) => Throw<int>();

    /// <summary>
    /// Gets the ISO week number from a DateTime (1-53).
    /// Translates to: toISOWeek(column)
    /// </summary>
    public static int ToISOWeek(this DateTime dateTime) => Throw<int>();

    /// <summary>
    /// Gets the day of the week (1=Monday, 7=Sunday).
    /// Translates to: toDayOfWeek(column)
    /// </summary>
    public static int ToDayOfWeek(this DateTime dateTime) => Throw<int>();

    /// <summary>
    /// Gets the day of the year (1-366).
    /// Translates to: toDayOfYear(column)
    /// </summary>
    public static int ToDayOfYear(this DateTime dateTime) => Throw<int>();

    /// <summary>
    /// Gets the quarter (1-4).
    /// Translates to: toQuarter(column)
    /// </summary>
    public static int ToQuarter(this DateTime dateTime) => Throw<int>();

    #endregion

    #region Nullable DateTime Overloads

    /// <summary>
    /// Converts a nullable DateTime to an integer in YYYYMM format.
    /// </summary>
    public static int? ToYYYYMM(this DateTime? dateTime) => Throw<int?>();

    /// <summary>
    /// Converts a nullable DateTime to an integer in YYYYMMDD format.
    /// </summary>
    public static int? ToYYYYMMDD(this DateTime? dateTime) => Throw<int?>();

    /// <summary>
    /// Truncates nullable DateTime to the start of the hour.
    /// </summary>
    public static DateTime? ToStartOfHour(this DateTime? dateTime) => Throw<DateTime?>();

    /// <summary>
    /// Truncates nullable DateTime to the start of the day.
    /// </summary>
    public static DateTime? ToStartOfDay(this DateTime? dateTime) => Throw<DateTime?>();

    /// <summary>
    /// Truncates nullable DateTime to the start of the month.
    /// </summary>
    public static DateTime? ToStartOfMonth(this DateTime? dateTime) => Throw<DateTime?>();

    #endregion
}
