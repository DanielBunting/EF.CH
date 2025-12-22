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
    /// Collects up to maxSize unique values into an array. Translates to groupUniqArray(maxSize)(column).
    /// </summary>
    public static TValue[] GroupUniqArray<TSource, TValue>(
        this IEnumerable<TSource> source,
        int maxSize,
        Func<TSource, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Returns the top K most frequent values. Translates to topK(k)(column).
    /// </summary>
    public static TValue[] TopK<TSource, TValue>(
        this IEnumerable<TSource> source,
        int k,
        Func<TSource, TValue> selector) => Throw<TValue[]>();

    #endregion

    #region State Combinators

    /// <summary>
    /// Returns the intermediate state of count() for AggregatingMergeTree storage.
    /// Translates to countState().
    /// </summary>
    /// <remarks>
    /// State combinators store opaque binary data that can be merged later using the
    /// corresponding -Merge combinator (CountMerge). Use with AggregatingMergeTree tables.
    /// </remarks>
    public static byte[] CountState<TSource>(this IEnumerable<TSource> source)
        => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of sum() for AggregatingMergeTree storage.
    /// Translates to sumState(column).
    /// </summary>
    public static byte[] SumState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of avg() for AggregatingMergeTree storage.
    /// Translates to avgState(column).
    /// </summary>
    public static byte[] AvgState<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of min() for AggregatingMergeTree storage.
    /// Translates to minState(column).
    /// </summary>
    public static byte[] MinState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of max() for AggregatingMergeTree storage.
    /// Translates to maxState(column).
    /// </summary>
    public static byte[] MaxState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of uniq() for AggregatingMergeTree storage.
    /// Translates to uniqState(column).
    /// </summary>
    public static byte[] UniqState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of uniqExact() for AggregatingMergeTree storage.
    /// Translates to uniqExactState(column).
    /// </summary>
    public static byte[] UniqExactState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of quantile() for AggregatingMergeTree storage.
    /// Translates to quantileState(level)(column).
    /// </summary>
    /// <param name="source">The source enumerable (group).</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    public static byte[] QuantileState<TSource>(
        this IEnumerable<TSource> source,
        double level,
        Func<TSource, double> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of any() for AggregatingMergeTree storage.
    /// Translates to anyState(column).
    /// </summary>
    public static byte[] AnyState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of anyLast() for AggregatingMergeTree storage.
    /// Translates to anyLastState(column).
    /// </summary>
    public static byte[] AnyLastState<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector) => Throw<byte[]>();

    #endregion

    #region Merge Combinators

    /// <summary>
    /// Merges countState() values into final count.
    /// Translates to countMerge(stateColumn).
    /// </summary>
    /// <remarks>
    /// Use this to read from AggregatingMergeTree tables that store state columns
    /// populated via countState().
    /// </remarks>
    public static long CountMerge<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<long>();

    /// <summary>
    /// Merges sumState() values into final sum.
    /// Translates to sumMerge(stateColumn).
    /// </summary>
    public static TValue SumMerge<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges avgState() values into final average.
    /// Translates to avgMerge(stateColumn).
    /// </summary>
    public static double AvgMerge<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<double>();

    /// <summary>
    /// Merges minState() values into final minimum.
    /// Translates to minMerge(stateColumn).
    /// </summary>
    public static TValue MinMerge<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges maxState() values into final maximum.
    /// Translates to maxMerge(stateColumn).
    /// </summary>
    public static TValue MaxMerge<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges uniqState() values into final unique count.
    /// Translates to uniqMerge(stateColumn).
    /// </summary>
    public static ulong UniqMerge<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<ulong>();

    /// <summary>
    /// Merges uniqExactState() values into final exact unique count.
    /// Translates to uniqExactMerge(stateColumn).
    /// </summary>
    public static ulong UniqExactMerge<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<ulong>();

    /// <summary>
    /// Merges quantileState() values into final quantile value.
    /// Translates to quantileMerge(level)(stateColumn).
    /// </summary>
    /// <param name="source">The source enumerable (group).</param>
    /// <param name="level">Quantile level from 0 to 1 (must match the level used in QuantileState).</param>
    /// <param name="stateSelector">The state column selector.</param>
    public static double QuantileMerge<TSource>(
        this IEnumerable<TSource> source,
        double level,
        Func<TSource, byte[]> stateSelector) => Throw<double>();

    /// <summary>
    /// Merges anyState() values into final value.
    /// Translates to anyMerge(stateColumn).
    /// </summary>
    public static TValue AnyMerge<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges anyLastState() values into final value.
    /// Translates to anyLastMerge(stateColumn).
    /// </summary>
    public static TValue AnyLastMerge<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, byte[]> stateSelector) => Throw<TValue>();

    #endregion

    #region Array Combinators

    /// <summary>
    /// Returns the sum of array elements. Translates to arraySum(column).
    /// </summary>
    /// <remarks>
    /// This operates on array columns directly (not IGrouping).
    /// For grouping aggregates, use the regular Sum() or GroupArray().
    /// </remarks>
    public static T ArraySum<T>(this T[] array) where T : struct
        => Throw<T>();

    /// <summary>
    /// Returns the sum of array elements. Translates to arraySum(column).
    /// </summary>
    public static T ArraySum<T>(this IEnumerable<T> array) where T : struct
        => Throw<T>();

    /// <summary>
    /// Returns the average of array elements. Translates to arrayAvg(column).
    /// </summary>
    public static double ArrayAvg<T>(this T[] array) where T : struct
        => Throw<double>();

    /// <summary>
    /// Returns the average of array elements. Translates to arrayAvg(column).
    /// </summary>
    public static double ArrayAvg<T>(this IEnumerable<T> array) where T : struct
        => Throw<double>();

    /// <summary>
    /// Returns the minimum array element. Translates to arrayMin(column).
    /// </summary>
    public static T ArrayMin<T>(this T[] array)
        => Throw<T>();

    /// <summary>
    /// Returns the minimum array element. Translates to arrayMin(column).
    /// </summary>
    public static T ArrayMin<T>(this IEnumerable<T> array)
        => Throw<T>();

    /// <summary>
    /// Returns the maximum array element. Translates to arrayMax(column).
    /// </summary>
    public static T ArrayMax<T>(this T[] array)
        => Throw<T>();

    /// <summary>
    /// Returns the maximum array element. Translates to arrayMax(column).
    /// </summary>
    public static T ArrayMax<T>(this IEnumerable<T> array)
        => Throw<T>();

    /// <summary>
    /// Returns the count of array elements. Translates to length(column).
    /// </summary>
    public static int ArrayCount<T>(this T[] array)
        => Throw<int>();

    /// <summary>
    /// Returns the count of array elements matching the predicate.
    /// Translates to arrayCount(x -> predicate, column).
    /// </summary>
    public static int ArrayCount<T>(this T[] array, Func<T, bool> predicate)
        => Throw<int>();

    /// <summary>
    /// Returns the count of array elements. Translates to length(column).
    /// </summary>
    public static int ArrayCount<T>(this IEnumerable<T> array)
        => Throw<int>();

    /// <summary>
    /// Returns the count of array elements matching the predicate.
    /// Translates to arrayCount(x -> predicate, column).
    /// </summary>
    public static int ArrayCount<T>(this IEnumerable<T> array, Func<T, bool> predicate)
        => Throw<int>();

    #endregion

    #region If Combinators

    /// <summary>
    /// Counts rows matching the predicate. Translates to countIf(condition).
    /// </summary>
    /// <remarks>
    /// This is an explicit -If combinator. LINQ's Count(predicate) also works
    /// but this provides direct access to ClickHouse's countIf function.
    /// </remarks>
    public static long CountIf<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, bool> predicate) => Throw<long>();

    /// <summary>
    /// Sums values where predicate is true. Translates to sumIf(column, condition).
    /// </summary>
    public static TValue SumIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Averages values where predicate is true. Translates to avgIf(column, condition).
    /// </summary>
    public static double AvgIf<TSource>(
        this IEnumerable<TSource> source,
        Func<TSource, double> selector,
        Func<TSource, bool> predicate) => Throw<double>();

    /// <summary>
    /// Returns minimum value where predicate is true. Translates to minIf(column, condition).
    /// </summary>
    public static TValue MinIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Returns maximum value where predicate is true. Translates to maxIf(column, condition).
    /// </summary>
    public static TValue MaxIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Counts unique values where predicate is true. Translates to uniqIf(column, condition).
    /// </summary>
    public static ulong UniqIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Counts exact unique values where predicate is true. Translates to uniqExactIf(column, condition).
    /// </summary>
    public static ulong UniqExactIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Returns any value where predicate is true. Translates to anyIf(column, condition).
    /// </summary>
    public static TValue AnyIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Returns the last value where predicate is true. Translates to anyLastIf(column, condition).
    /// </summary>
    public static TValue AnyLastIf<TSource, TValue>(
        this IEnumerable<TSource> source,
        Func<TSource, TValue> selector,
        Func<TSource, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Computes an approximate quantile where predicate is true.
    /// Translates to quantileIf(level)(column, condition).
    /// </summary>
    /// <param name="source">The source enumerable (group).</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    /// <param name="predicate">The filter predicate.</param>
    public static double QuantileIf<TSource>(
        this IEnumerable<TSource> source,
        double level,
        Func<TSource, double> selector,
        Func<TSource, bool> predicate) => Throw<double>();

    #endregion

    private static T Throw<T>() => throw new InvalidOperationException(
        "This method is for LINQ translation only and should not be invoked directly. " +
        "Use it within projections, materialized views, or LINQ queries against ClickHouse.");
}
