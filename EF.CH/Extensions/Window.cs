namespace EF.CH.Extensions;

/// <summary>
/// Entry point for ClickHouse window functions in LINQ queries.
/// </summary>
/// <remarks>
/// <para>
/// Window functions perform calculations across a set of rows related to the current row.
/// They are used with the OVER clause to define the window (partition and ordering).
/// </para>
/// <para>
/// Two API styles are supported:
/// </para>
/// <para>
/// <b>Fluent style</b> (requires .Value at end):
/// <code>
/// RowNum = Window.RowNumber()
///     .PartitionBy(o.Region)
///     .OrderBy(o.Date)
///     .Value
/// </code>
/// </para>
/// <para>
/// <b>Lambda style</b> (no .Value needed):
/// <code>
/// RowNum = Window.RowNumber(w => w
///     .PartitionBy(o.Region)
///     .OrderBy(o.Date))
/// </code>
/// </para>
/// </remarks>
public static class Window
{
    #region Ranking Functions

    /// <summary>
    /// Returns the sequential row number within a partition, starting at 1.
    /// Translates to: row_number() OVER (...)
    /// </summary>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<long> RowNumber() => new("row_number");

    /// <summary>
    /// Returns the sequential row number within a partition, starting at 1.
    /// Translates to: row_number() OVER (...)
    /// </summary>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The row number value.</returns>
    public static long RowNumber(Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the rank of the current row within its partition, with gaps for ties.
    /// Translates to: rank() OVER (...)
    /// </summary>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<long> Rank() => new("rank");

    /// <summary>
    /// Returns the rank of the current row within its partition, with gaps for ties.
    /// Translates to: rank() OVER (...)
    /// </summary>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The rank value.</returns>
    public static long Rank(Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the rank of the current row within its partition, without gaps for ties.
    /// Translates to: dense_rank() OVER (...)
    /// </summary>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<long> DenseRank() => new("dense_rank");

    /// <summary>
    /// Returns the rank of the current row within its partition, without gaps for ties.
    /// Translates to: dense_rank() OVER (...)
    /// </summary>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The dense rank value.</returns>
    public static long DenseRank(Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the relative rank of the current row: (rank - 1) / (total rows - 1).
    /// Translates to: percent_rank() OVER (...)
    /// </summary>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<double> PercentRank() => new("percent_rank");

    /// <summary>
    /// Returns the relative rank of the current row: (rank - 1) / (total rows - 1).
    /// Translates to: percent_rank() OVER (...)
    /// </summary>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The percent rank value.</returns>
    public static double PercentRank(Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Divides rows into a specified number of roughly equal buckets.
    /// Translates to: ntile(n) OVER (...)
    /// </summary>
    /// <param name="buckets">The number of buckets to divide the partition into.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<long> NTile(int buckets) => new("ntile", buckets);

    /// <summary>
    /// Divides rows into a specified number of roughly equal buckets.
    /// Translates to: ntile(n) OVER (...)
    /// </summary>
    /// <param name="buckets">The number of buckets to divide the partition into.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The bucket number.</returns>
    public static long NTile(int buckets, Func<WindowSpec, WindowSpec> over) => default;

    #endregion

    #region Value Functions (Lag/Lead)

    /// <summary>
    /// Returns the value from the previous row in the partition (offset = 1).
    /// Translates to: lagInFrame(value, 1) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Lag<T>(T value) => new("lagInFrame", value, 1);

    /// <summary>
    /// Returns the value from the previous row in the partition (offset = 1).
    /// Translates to: lagInFrame(value, 1) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The lagged value.</returns>
    public static T? Lag<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the value from a row at the specified offset before the current row.
    /// Translates to: lagInFrame(value, offset) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows back from the current row.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Lag<T>(T value, int offset) => new("lagInFrame", value, offset);

    /// <summary>
    /// Returns the value from a row at the specified offset before the current row.
    /// Translates to: lagInFrame(value, offset) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows back from the current row.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The lagged value.</returns>
    public static T? Lag<T>(T value, int offset, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the value from a row at the specified offset before the current row,
    /// or the default value if no such row exists.
    /// Translates to: lagInFrame(value, offset, default) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows back from the current row.</param>
    /// <param name="defaultValue">The value to return if no row exists at the offset.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Lag<T>(T value, int offset, T defaultValue) => new("lagInFrame", value, offset, defaultValue);

    /// <summary>
    /// Returns the value from a row at the specified offset before the current row,
    /// or the default value if no such row exists.
    /// Translates to: lagInFrame(value, offset, default) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows back from the current row.</param>
    /// <param name="defaultValue">The value to return if no row exists at the offset.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The lagged value or default.</returns>
    public static T Lag<T>(T value, int offset, T defaultValue, Func<WindowSpec, WindowSpec> over) => default!;

    /// <summary>
    /// Returns the value from the next row in the partition (offset = 1).
    /// Translates to: leadInFrame(value, 1) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Lead<T>(T value) => new("leadInFrame", value, 1);

    /// <summary>
    /// Returns the value from the next row in the partition (offset = 1).
    /// Translates to: leadInFrame(value, 1) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The lead value.</returns>
    public static T? Lead<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the value from a row at the specified offset after the current row.
    /// Translates to: leadInFrame(value, offset) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows forward from the current row.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Lead<T>(T value, int offset) => new("leadInFrame", value, offset);

    /// <summary>
    /// Returns the value from a row at the specified offset after the current row.
    /// Translates to: leadInFrame(value, offset) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows forward from the current row.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The lead value.</returns>
    public static T? Lead<T>(T value, int offset, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the value from a row at the specified offset after the current row,
    /// or the default value if no such row exists.
    /// Translates to: leadInFrame(value, offset, default) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows forward from the current row.</param>
    /// <param name="defaultValue">The value to return if no row exists at the offset.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Lead<T>(T value, int offset, T defaultValue) => new("leadInFrame", value, offset, defaultValue);

    /// <summary>
    /// Returns the value from a row at the specified offset after the current row,
    /// or the default value if no such row exists.
    /// Translates to: leadInFrame(value, offset, default) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="offset">The number of rows forward from the current row.</param>
    /// <param name="defaultValue">The value to return if no row exists at the offset.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The lead value or default.</returns>
    public static T Lead<T>(T value, int offset, T defaultValue, Func<WindowSpec, WindowSpec> over) => default!;

    /// <summary>
    /// Returns the first value in the window frame.
    /// Translates to: first_value(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> FirstValue<T>(T value) => new("first_value", value);

    /// <summary>
    /// Returns the first value in the window frame.
    /// Translates to: first_value(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The first value.</returns>
    public static T? FirstValue<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the last value in the window frame.
    /// Translates to: last_value(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> LastValue<T>(T value) => new("last_value", value);

    /// <summary>
    /// Returns the last value in the window frame.
    /// Translates to: last_value(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The last value.</returns>
    public static T? LastValue<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Returns the nth value in the window frame.
    /// Translates to: nth_value(value, n) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="n">The 1-based position of the value to return.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> NthValue<T>(T value, int n) => new("nth_value", value, n);

    /// <summary>
    /// Returns the nth value in the window frame.
    /// Translates to: nth_value(value, n) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value expression to retrieve.</param>
    /// <param name="n">The 1-based position of the value to return.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The nth value.</returns>
    public static T? NthValue<T>(T value, int n, Func<WindowSpec, WindowSpec> over) => default;

    #endregion

    #region Aggregate Window Functions

    /// <summary>
    /// Calculates the sum over the window frame.
    /// Translates to: sum(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">A numeric type.</typeparam>
    /// <param name="value">The value to sum.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Sum<T>(T value) => new("sum", value);

    /// <summary>
    /// Calculates the sum over the window frame.
    /// Translates to: sum(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">A numeric type.</typeparam>
    /// <param name="value">The value to sum.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The sum value.</returns>
    public static T? Sum<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Calculates the average over the window frame.
    /// Translates to: avg(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">A numeric type.</typeparam>
    /// <param name="value">The value to average.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<double> Avg<T>(T value) => new("avg", value);

    /// <summary>
    /// Calculates the average over the window frame.
    /// Translates to: avg(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">A numeric type.</typeparam>
    /// <param name="value">The value to average.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The average value.</returns>
    public static double? Avg<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Counts the non-null values in the window frame.
    /// Translates to: count(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to count (non-null values).</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<long> Count<T>(T value) => new("count", value);

    /// <summary>
    /// Counts the non-null values in the window frame.
    /// Translates to: count(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to count (non-null values).</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The count value.</returns>
    public static long Count<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Counts all rows in the window frame.
    /// Translates to: count() OVER (...)
    /// </summary>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<long> Count() => new("count");

    /// <summary>
    /// Counts all rows in the window frame.
    /// Translates to: count() OVER (...)
    /// </summary>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The count value.</returns>
    public static long Count(Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Finds the minimum value in the window frame.
    /// Translates to: min(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to find the minimum of.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Min<T>(T value) => new("min", value);

    /// <summary>
    /// Finds the minimum value in the window frame.
    /// Translates to: min(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to find the minimum of.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The minimum value.</returns>
    public static T? Min<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    /// <summary>
    /// Finds the maximum value in the window frame.
    /// Translates to: max(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to find the maximum of.</param>
    /// <returns>A builder for constructing the OVER clause. Call .Value to get the result.</returns>
    public static WindowBuilder<T> Max<T>(T value) => new("max", value);

    /// <summary>
    /// Finds the maximum value in the window frame.
    /// Translates to: max(value) OVER (...)
    /// </summary>
    /// <typeparam name="T">The type of the value.</typeparam>
    /// <param name="value">The value to find the maximum of.</param>
    /// <param name="over">Lambda to configure the OVER clause (partition, order, frame).</param>
    /// <returns>The maximum value.</returns>
    public static T? Max<T>(T value, Func<WindowSpec, WindowSpec> over) => default;

    #endregion
}
