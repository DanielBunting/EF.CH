namespace EF.CH.Extensions;

/// <summary>
/// Provides ClickHouse-specific aggregate functions for use in projections, materialized views, and LINQ queries.
/// These methods are translated to their corresponding ClickHouse SQL functions and should not be invoked directly.
/// </summary>
public static class ClickHouseAggregates
{
    #region Phase 1 - Uniqueness

    /// <summary>
    /// Calculates the approximate number of unique values. Translates to uniq(column).
    /// Much faster than UniqExact but may have slight variance (~2%).
    /// </summary>
    public static ulong Uniq<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<ulong>();

    /// <summary>
    /// Calculates the exact number of unique values. Translates to uniqExact(column).
    /// More resource-intensive than Uniq but provides exact counts.
    /// </summary>
    public static ulong UniqExact<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<ulong>();

    #endregion

    #region Phase 1 - ArgMax/ArgMin

    /// <summary>
    /// Returns the value of the arg column at the row where val is maximum.
    /// Translates to argMax(arg, val).
    /// </summary>
    /// <example>
    /// // Get the price at the most recent timestamp
    /// ClickHouseAggregates.ArgMax(g, o => o.Price, o => o.Timestamp)
    /// </example>
    public static TArg ArgMax<TSource, TArg, TVal>(
        this IEnumerable<TSource> source,
        Func<TSource, TArg> argSelector,
        Func<TSource, TVal> valSelector) => Throw<TArg>();

    /// <summary>
    /// Returns the value of the arg column at the row where val is minimum.
    /// Translates to argMin(arg, val).
    /// </summary>
    public static TArg ArgMin<TSource, TArg, TVal>(
        this IEnumerable<TSource> source,
        Func<TSource, TArg> argSelector,
        Func<TSource, TVal> valSelector) => Throw<TArg>();

    #endregion

    #region Phase 1 - AnyValue

    /// <summary>
    /// Returns any value from the group (first encountered). Translates to any(column).
    /// Useful for getting a representative value from a group.
    /// Named AnyValue to avoid conflict with LINQ's Any() method.
    /// </summary>
    public static TValue AnyValue<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<TValue>();

    /// <summary>
    /// Returns the last value from the group. Translates to anyLast(column).
    /// </summary>
    public static TValue AnyLastValue<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<TValue>();

    #endregion

    #region Phase 2 - Quantiles

    /// <summary>
    /// Computes an approximate quantile using the t-digest algorithm.
    /// Translates to quantile(level)(column).
    /// </summary>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile)</param>
    public static double Quantile<TSource>(
        this IEnumerable<TSource> source,
        double level,
        Func<TSource, double> selector) => Throw<double>();

    /// <summary>
    /// Computes the median (50th percentile). Translates to median(column).
    /// </summary>
    public static double Median<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector) => Throw<double>();

    #endregion

    #region Phase 2 - Statistics

    /// <summary>
    /// Calculates the population standard deviation. Translates to stddevPop(column).
    /// </summary>
    public static double StddevPop<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector) => Throw<double>();

    /// <summary>
    /// Calculates the sample standard deviation. Translates to stddevSamp(column).
    /// </summary>
    public static double StddevSamp<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector) => Throw<double>();

    /// <summary>
    /// Calculates the population variance. Translates to varPop(column).
    /// </summary>
    public static double VarPop<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector) => Throw<double>();

    /// <summary>
    /// Calculates the sample variance. Translates to varSamp(column).
    /// </summary>
    public static double VarSamp<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector) => Throw<double>();

    #endregion

    #region Phase 3 - Arrays

    /// <summary>
    /// Collects all values into an array. Translates to groupArray(column).
    /// </summary>
    public static TValue[] GroupArray<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Collects up to maxSize values into an array. Translates to groupArray(maxSize)(column).
    /// </summary>
    public static TValue[] GroupArray<TSource, TValue>(
        this IEnumerable<TSource> source,
        int maxSize,
        Func<TSource, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Collects all unique values into an array. Translates to groupUniqArray(column).
    /// </summary>
    public static TValue[] GroupUniqArray<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Returns the top K most frequent values. Translates to topK(k)(column).
    /// </summary>
    public static TValue[] TopK<TSource, TValue>(
        this IEnumerable<TSource> source,
        int k,
        Func<TSource, TValue> selector) => Throw<TValue[]>();

    #endregion

    private static T Throw<T>() => throw new InvalidOperationException(
        "This method is for LINQ translation only and should not be invoked directly. " +
        "Use it within projections, materialized views, or LINQ queries against ClickHouse.");
}
