namespace EF.CH.Extensions;

/// <summary>
/// Provides ClickHouse-specific aggregate functions as extension methods on
/// <see cref="IGrouping{TKey, TElement}"/> for use in materialized-view definitions, projections,
/// and runtime LINQ <c>GroupBy(...).Select(...)</c> queries against ClickHouse.
/// These methods are translation stubs — they throw if invoked directly, and are converted to
/// ClickHouse SQL by the projection-DDL translator and the runtime aggregate translator.
/// </summary>
/// <remarks>
/// Idiomatic dotted-form usage:
/// <code>
/// var stats = ctx.Orders
///     .GroupBy(o => o.ProductId)
///     .Select(g => new {
///         g.Key,
///         Errors    = g.CountIf(x => x.Status >= 500),
///         Revenue   = g.SumIf(x => x.Amount, x => x.Status == "paid"),
///         UniqUsers = g.UniqCombined(x => x.UserId),
///     });
/// </code>
/// </remarks>
public static class ClickHouseAggregates
{
    #region Phase 1 - Uniqueness

    /// <summary>
    /// Calculates the approximate number of unique values. Translates to uniq(column).
    /// Much faster than UniqExact but may have slight variance (~2%).
    /// </summary>
    public static ulong Uniq<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

    /// <summary>
    /// Calculates the exact number of unique values. Translates to uniqExact(column).
    /// More resource-intensive than Uniq but provides exact counts.
    /// </summary>
    public static ulong UniqExact<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

    #endregion

    #region Phase 1 - ArgMax/ArgMin

    /// <summary>
    /// Returns the value of the arg column at the row where val is maximum.
    /// Translates to argMax(arg, val).
    /// </summary>
    /// <example>
    /// // Get the price at the most recent timestamp
    /// g.ArgMax(o => o.Price, o => o.Timestamp)
    /// </example>
    public static TArg ArgMax<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector) => Throw<TArg>();

    /// <summary>
    /// Returns the value of the arg column at the row where val is minimum.
    /// Translates to argMin(arg, val).
    /// </summary>
    public static TArg ArgMin<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector) => Throw<TArg>();

    #endregion

    #region Phase 1 - AnyValue

    /// <summary>
    /// Returns any value from the group (first encountered). Translates to any(column).
    /// Useful for getting a representative value from a group.
    /// Named AnyValue to avoid conflict with LINQ's Any() method.
    /// </summary>
    public static TValue AnyValue<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<TValue>();

    /// <summary>
    /// Returns the last value from the group. Translates to anyLast(column).
    /// </summary>
    public static TValue AnyLastValue<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<TValue>();

    #endregion

    #region Phase 2 - Quantiles

    /// <summary>
    /// Computes an approximate quantile using the t-digest algorithm.
    /// Translates to quantile(level)(column).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile)</param>
    /// <param name="selector">The value selector.</param>
    public static double Quantile<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Computes the median (50th percentile). Translates to median(column).
    /// </summary>
    public static double Median<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<double>();

    #endregion

    #region Phase 2 - Statistics

    /// <summary>
    /// Calculates the population standard deviation. Translates to stddevPop(column).
    /// </summary>
    public static double StddevPop<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Calculates the sample standard deviation. Translates to stddevSamp(column).
    /// </summary>
    public static double StddevSamp<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Calculates the population variance. Translates to varPop(column).
    /// </summary>
    public static double VarPop<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Calculates the sample variance. Translates to varSamp(column).
    /// </summary>
    public static double VarSamp<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<double>();

    #endregion

    #region Approximate Count Distinct

    /// <summary>
    /// Calculates the approximate number of unique values using the Combined algorithm.
    /// Translates to uniqCombined(column).
    /// Uses a combination of HyperLogLog, hash table and error correction.
    /// </summary>
    public static ulong UniqCombined<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

    /// <summary>
    /// Calculates the approximate number of unique values using the Combined64 algorithm.
    /// Translates to uniqCombined64(column).
    /// Same as UniqCombined but uses 64-bit hash for all data types.
    /// </summary>
    public static ulong UniqCombined64<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

    /// <summary>
    /// Calculates the approximate number of unique values using HyperLogLog.
    /// Translates to uniqHLL12(column).
    /// Uses HyperLogLog algorithm with 2^12 cells.
    /// </summary>
    public static ulong UniqHLL12<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

    /// <summary>
    /// Calculates the approximate number of unique values using Theta Sketch.
    /// Translates to uniqTheta(column).
    /// Uses Theta Sketch algorithm for set operations support.
    /// </summary>
    public static ulong UniqTheta<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

    #endregion

    #region Quantile Variants

    /// <summary>
    /// Computes an approximate quantile using the t-digest algorithm.
    /// Translates to quantileTDigest(level)(column).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    public static double QuantileTDigest<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Computes an approximate quantile using the DD (DDSketch) algorithm.
    /// Translates to quantileDD(relative_accuracy, level)(column).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="relativeAccuracy">Relative accuracy of the DD algorithm (e.g., 0.01 for 1%).</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    public static double QuantileDD<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double relativeAccuracy,
        double level,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Computes the exact quantile value. Translates to quantileExact(level)(column).
    /// More resource-intensive than approximate variants but provides exact results.
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    public static double QuantileExact<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<double>();

    /// <summary>
    /// Computes an approximate quantile optimized for timing data.
    /// Translates to quantileTiming(level)(column).
    /// Optimized for sequences that describe distributions such as loading times.
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    public static double QuantileTiming<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<double>();

    #endregion

    #region Multi-Quantile

    /// <summary>
    /// Computes multiple approximate quantiles in a single pass.
    /// Translates to quantiles(level1, level2, ...)(column).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="levels">Array of quantile levels from 0 to 1.</param>
    /// <param name="selector">The value selector.</param>
    public static double[] Quantiles<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector) => Throw<double[]>();

    /// <summary>
    /// Computes multiple approximate quantiles using the t-digest algorithm in a single pass.
    /// Translates to quantilesTDigest(level1, level2, ...)(column).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="levels">Array of quantile levels from 0 to 1.</param>
    /// <param name="selector">The value selector.</param>
    public static double[] QuantilesTDigest<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector) => Throw<double[]>();

    #endregion

    #region Weighted Top K

    /// <summary>
    /// Returns the top K most frequent values weighted by a weight column.
    /// Translates to topKWeighted(k)(column, weight).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="k">The number of top elements to return.</param>
    /// <param name="selector">The value selector.</param>
    /// <param name="weightSelector">The weight selector.</param>
    public static TValue[] TopKWeighted<TKey, TElement, TValue, TWeight>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector,
        Func<TElement, TWeight> weightSelector) => Throw<TValue[]>();

    #endregion

    #region Phase 3 - Arrays

    /// <summary>
    /// Collects all values into an array. Translates to groupArray(column).
    /// </summary>
    public static TValue[] GroupArray<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Collects up to maxSize values into an array. Translates to groupArray(maxSize)(column).
    /// </summary>
    public static TValue[] GroupArray<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Collects all unique values into an array. Translates to groupUniqArray(column).
    /// </summary>
    public static TValue[] GroupUniqArray<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Collects up to maxSize unique values into an array. Translates to groupUniqArray(maxSize)(column).
    /// </summary>
    public static TValue[] GroupUniqArray<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector) => Throw<TValue[]>();

    /// <summary>
    /// Returns the top K most frequent values. Translates to topK(k)(column).
    /// </summary>
    public static TValue[] TopK<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector) => Throw<TValue[]>();

    #endregion

    #region Typed-return Aggregates

    /// <summary>
    /// Counts rows in the group. Translates to <c>count()</c> and returns the native ClickHouse
    /// <see cref="ulong"/> result without the <c>(ulong)g.Count()</c> cast tax.
    /// </summary>
    /// <remarks>
    /// ClickHouse's <c>count()</c> aggregate natively returns <c>UInt64</c>; this overload simply
    /// surfaces that as <see cref="ulong"/>. Use this when you want a <see cref="ulong"/>-typed
    /// projection column instead of <see cref="int"/> from LINQ's <c>g.Count()</c>.
    /// </remarks>
    public static ulong CountUInt64<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping) => Throw<ulong>();

    /// <summary>
    /// Sums values in the group, returning a <see cref="ulong"/>. Translates to <c>sum(column)</c>.
    /// Use this when the aggregated values fit in an unsigned 64-bit integer and you want a
    /// <see cref="ulong"/> projection column without the <c>(ulong)g.Sum(...)</c> cast tax.
    /// </summary>
    public static ulong SumUInt64<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<ulong>();

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
    public static byte[] CountState<TKey, TElement>(this IGrouping<TKey, TElement> grouping)
        => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of sum() for AggregatingMergeTree storage.
    /// Translates to sumState(column).
    /// </summary>
    public static byte[] SumState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of avg() for AggregatingMergeTree storage.
    /// Translates to avgState(column).
    /// </summary>
    public static byte[] AvgState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of min() for AggregatingMergeTree storage.
    /// Translates to minState(column).
    /// </summary>
    public static byte[] MinState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of max() for AggregatingMergeTree storage.
    /// Translates to maxState(column).
    /// </summary>
    public static byte[] MaxState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of uniq() for AggregatingMergeTree storage.
    /// Translates to uniqState(column).
    /// </summary>
    public static byte[] UniqState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of uniqExact() for AggregatingMergeTree storage.
    /// Translates to uniqExactState(column).
    /// </summary>
    public static byte[] UniqExactState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of quantile() for AggregatingMergeTree storage.
    /// Translates to quantileState(level)(column).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    public static byte[] QuantileState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of any() for AggregatingMergeTree storage.
    /// Translates to anyState(column).
    /// </summary>
    public static byte[] AnyState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>
    /// Returns the intermediate state of anyLast() for AggregatingMergeTree storage.
    /// Translates to anyLastState(column).
    /// </summary>
    public static byte[] AnyLastState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    #endregion

    #region MergeState Combinators

    // `-MergeState` combines `-Merge` and `-State`: reads an AMT state column,
    // merges it, and re-emits the result as a state blob for downstream
    // AggregatingMergeTrees. Used to chain source AMT → rollup AMT (e.g. hourly → daily).

    /// <summary>countMergeState(stateCol) — re-state of a merged count.</summary>
    public static byte[] CountMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>sumMergeState(stateCol).</summary>
    public static byte[] SumMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>avgMergeState(stateCol).</summary>
    public static byte[] AvgMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>minMergeState(stateCol).</summary>
    public static byte[] MinMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>maxMergeState(stateCol).</summary>
    public static byte[] MaxMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>uniqMergeState(stateCol).</summary>
    public static byte[] UniqMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>uniqExactMergeState(stateCol).</summary>
    public static byte[] UniqExactMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>anyMergeState(stateCol).</summary>
    public static byte[] AnyMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>anyLastMergeState(stateCol).</summary>
    public static byte[] AnyLastMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

    /// <summary>quantileMergeState(level)(stateCol).</summary>
    public static byte[] QuantileMergeState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, byte[]> stateSelector) => Throw<byte[]>();

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
    public static long CountMerge<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<long>();

    /// <summary>
    /// Merges sumState() values into final sum.
    /// Translates to sumMerge(stateColumn).
    /// </summary>
    public static TValue SumMerge<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges avgState() values into final average.
    /// Translates to avgMerge(stateColumn).
    /// </summary>
    public static double AvgMerge<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<double>();

    /// <summary>
    /// Merges minState() values into final minimum.
    /// Translates to minMerge(stateColumn).
    /// </summary>
    public static TValue MinMerge<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges maxState() values into final maximum.
    /// Translates to maxMerge(stateColumn).
    /// </summary>
    public static TValue MaxMerge<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges uniqState() values into final unique count.
    /// Translates to uniqMerge(stateColumn).
    /// </summary>
    public static ulong UniqMerge<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<ulong>();

    /// <summary>
    /// Merges uniqExactState() values into final exact unique count.
    /// Translates to uniqExactMerge(stateColumn).
    /// </summary>
    public static ulong UniqExactMerge<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<ulong>();

    /// <summary>
    /// Merges quantileState() values into final quantile value.
    /// Translates to quantileMerge(level)(stateColumn).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (must match the level used in QuantileState).</param>
    /// <param name="stateSelector">The state column selector.</param>
    public static double QuantileMerge<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, byte[]> stateSelector) => Throw<double>();

    /// <summary>
    /// Merges anyState() values into final value.
    /// Translates to anyMerge(stateColumn).
    /// </summary>
    public static TValue AnyMerge<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<TValue>();

    /// <summary>
    /// Merges anyLastState() values into final value.
    /// Translates to anyLastMerge(stateColumn).
    /// </summary>
    public static TValue AnyLastMerge<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, byte[]> stateSelector) => Throw<TValue>();

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
    /// but this provides direct access to ClickHouse's countIf function and a
    /// <see cref="long"/> return type.
    /// </remarks>
    public static long CountIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, bool> predicate) => Throw<long>();

    /// <summary>
    /// Sums values where predicate is true. Translates to sumIf(column, condition).
    /// </summary>
    public static TValue SumIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Averages values where predicate is true. Translates to avgIf(column, condition).
    /// </summary>
    public static double AvgIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Returns minimum value where predicate is true. Translates to minIf(column, condition).
    /// </summary>
    public static TValue MinIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Returns maximum value where predicate is true. Translates to maxIf(column, condition).
    /// </summary>
    public static TValue MaxIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Counts unique values where predicate is true. Translates to uniqIf(column, condition).
    /// </summary>
    public static ulong UniqIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Counts exact unique values where predicate is true. Translates to uniqExactIf(column, condition).
    /// </summary>
    public static ulong UniqExactIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Returns any value where predicate is true. Translates to anyIf(column, condition).
    /// </summary>
    public static TValue AnyIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Returns the last value where predicate is true. Translates to anyLastIf(column, condition).
    /// </summary>
    public static TValue AnyLastIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue>();

    /// <summary>
    /// Computes an approximate quantile where predicate is true.
    /// Translates to quantileIf(level)(column, condition).
    /// </summary>
    /// <param name="grouping">The grouping the aggregate is computed over.</param>
    /// <param name="level">Quantile level from 0 to 1 (e.g., 0.95 for 95th percentile).</param>
    /// <param name="selector">The value selector.</param>
    /// <param name="predicate">The filter predicate.</param>
    public static double QuantileIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Returns the arg value at the row where val is maximum, restricted to rows matching the predicate.
    /// Translates to argMaxIf(arg, val, condition).
    /// </summary>
    public static TArg ArgMaxIf<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector,
        Func<TElement, bool> predicate) => Throw<TArg>();

    /// <summary>
    /// Returns the arg value at the row where val is minimum, restricted to rows matching the predicate.
    /// Translates to argMinIf(arg, val, condition).
    /// </summary>
    public static TArg ArgMinIf<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector,
        Func<TElement, bool> predicate) => Throw<TArg>();

    /// <summary>
    /// Returns the top K most frequent values where predicate is true. Translates to topKIf(k)(column, condition).
    /// </summary>
    public static TValue[] TopKIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue[]>();

    /// <summary>
    /// Returns the top K most frequent values weighted by a weight column, restricted to rows matching the predicate.
    /// Translates to topKWeightedIf(k)(column, weight, condition).
    /// </summary>
    public static TValue[] TopKWeightedIf<TKey, TElement, TValue, TWeight>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector,
        Func<TElement, TWeight> weightSelector,
        Func<TElement, bool> predicate) => Throw<TValue[]>();

    /// <summary>
    /// Collects values matching the predicate into an array. Translates to groupArrayIf(column, condition).
    /// </summary>
    public static TValue[] GroupArrayIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue[]>();

    /// <summary>
    /// Collects up to maxSize values matching the predicate into an array.
    /// Translates to groupArrayIf(maxSize)(column, condition).
    /// </summary>
    public static TValue[] GroupArrayIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue[]>();

    /// <summary>
    /// Collects unique values matching the predicate into an array. Translates to groupUniqArrayIf(column, condition).
    /// </summary>
    public static TValue[] GroupUniqArrayIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue[]>();

    /// <summary>
    /// Collects up to maxSize unique values matching the predicate into an array.
    /// Translates to groupUniqArrayIf(maxSize)(column, condition).
    /// </summary>
    public static TValue[] GroupUniqArrayIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<TValue[]>();

    /// <summary>
    /// Computes the median where predicate is true. Translates to medianIf(column, condition).
    /// </summary>
    public static double MedianIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Calculates the population standard deviation where predicate is true.
    /// Translates to stddevPopIf(column, condition).
    /// </summary>
    public static double StddevPopIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Calculates the sample standard deviation where predicate is true.
    /// Translates to stddevSampIf(column, condition).
    /// </summary>
    public static double StddevSampIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Calculates the population variance where predicate is true. Translates to varPopIf(column, condition).
    /// </summary>
    public static double VarPopIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Calculates the sample variance where predicate is true. Translates to varSampIf(column, condition).
    /// </summary>
    public static double VarSampIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Approximate distinct count where predicate is true using the Combined algorithm.
    /// Translates to uniqCombinedIf(column, condition).
    /// </summary>
    public static ulong UniqCombinedIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Approximate distinct count where predicate is true using the Combined64 algorithm.
    /// Translates to uniqCombined64If(column, condition).
    /// </summary>
    public static ulong UniqCombined64If<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Approximate distinct count where predicate is true using HyperLogLog.
    /// Translates to uniqHLL12If(column, condition).
    /// </summary>
    public static ulong UniqHLL12If<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Approximate distinct count where predicate is true using Theta Sketch.
    /// Translates to uniqThetaIf(column, condition).
    /// </summary>
    public static ulong UniqThetaIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<ulong>();

    /// <summary>
    /// Approximate quantile using the t-digest algorithm, restricted to rows matching the predicate.
    /// Translates to quantileTDigestIf(level)(column, condition).
    /// </summary>
    public static double QuantileTDigestIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Exact quantile, restricted to rows matching the predicate.
    /// Translates to quantileExactIf(level)(column, condition).
    /// </summary>
    public static double QuantileExactIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Approximate quantile optimized for timing data, restricted to rows matching the predicate.
    /// Translates to quantileTimingIf(level)(column, condition).
    /// </summary>
    public static double QuantileTimingIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Approximate quantile using the DD (DDSketch) algorithm, restricted to rows matching the predicate.
    /// Translates to quantileDDIf(relative_accuracy, level)(column, condition).
    /// </summary>
    public static double QuantileDDIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double relativeAccuracy,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double>();

    /// <summary>
    /// Computes multiple approximate quantiles in a single pass, restricted to rows matching the predicate.
    /// Translates to quantilesIf(level1, level2, ...)(column, condition).
    /// </summary>
    public static double[] QuantilesIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double[]>();

    /// <summary>
    /// Computes multiple approximate quantiles using t-digest in a single pass, restricted to rows matching the predicate.
    /// Translates to quantilesTDigestIf(level1, level2, ...)(column, condition).
    /// </summary>
    public static double[] QuantilesTDigestIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<double[]>();

    #endregion

    #region State Combinators - broader family

    // These complete the -State surface so every aggregate recognised by
    // MaterializedViewSqlTranslator has a matching -State variant. They all
    // translate to their ClickHouse equivalents inside AsMaterializedView(...)
    // LINQ expressions; direct invocation throws.

    /// <summary>Intermediate state of median(). Translates to medianState(column).</summary>
    public static byte[] MedianState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of stddevPop(). Translates to stddevPopState(column).</summary>
    public static byte[] StddevPopState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of stddevSamp(). Translates to stddevSampState(column).</summary>
    public static byte[] StddevSampState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of varPop(). Translates to varPopState(column).</summary>
    public static byte[] VarPopState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of varSamp(). Translates to varSampState(column).</summary>
    public static byte[] VarSampState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of uniqCombined(). Translates to uniqCombinedState(column).</summary>
    public static byte[] UniqCombinedState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of uniqCombined64(). Translates to uniqCombined64State(column).</summary>
    public static byte[] UniqCombined64State<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of uniqHLL12(). Translates to uniqHLL12State(column).</summary>
    public static byte[] UniqHLL12State<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of uniqTheta(). Translates to uniqThetaState(column).</summary>
    public static byte[] UniqThetaState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of argMax(). Translates to argMaxState(arg, val).</summary>
    public static byte[] ArgMaxState<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector) => Throw<byte[]>();

    /// <summary>Intermediate state of argMin(). Translates to argMinState(arg, val).</summary>
    public static byte[] ArgMinState<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector) => Throw<byte[]>();

    /// <summary>Intermediate state of quantileTDigest(). Translates to quantileTDigestState(level)(column).</summary>
    public static byte[] QuantileTDigestState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of quantileExact(). Translates to quantileExactState(level)(column).</summary>
    public static byte[] QuantileExactState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of quantileTiming(). Translates to quantileTimingState(level)(column).</summary>
    public static byte[] QuantileTimingState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of quantileDD(). Translates to quantileDDState(acc, level)(column).</summary>
    public static byte[] QuantileDDState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double relativeAccuracy,
        double level,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of quantiles(). Translates to quantilesState(level1, level2, ...)(column).</summary>
    public static byte[] QuantilesState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of quantilesTDigest(). Translates to quantilesTDigestState(level1, ...)(column).</summary>
    public static byte[] QuantilesTDigestState<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of groupArray(). Translates to groupArrayState(column).</summary>
    public static byte[] GroupArrayState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of groupArray(max). Translates to groupArrayState(max)(column).</summary>
    public static byte[] GroupArrayState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of groupUniqArray(). Translates to groupUniqArrayState(column).</summary>
    public static byte[] GroupUniqArrayState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of groupUniqArray(max). Translates to groupUniqArrayState(max)(column).</summary>
    public static byte[] GroupUniqArrayState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of topK(k). Translates to topKState(k)(column).</summary>
    public static byte[] TopKState<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector) => Throw<byte[]>();

    /// <summary>Intermediate state of topKWeighted(k). Translates to topKWeightedState(k)(column, weight).</summary>
    public static byte[] TopKWeightedState<TKey, TElement, TValue, TWeight>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector,
        Func<TElement, TWeight> weightSelector) => Throw<byte[]>();

    #endregion

    #region StateIf Combinators

    // -StateIf = -State + -If. Store a partial aggregate restricted to rows
    // matching a predicate — used inside AsMaterializedView to land conditional
    // rollups into AggregatingMergeTree targets.

    /// <summary>countStateIf(condition).</summary>
    public static byte[] CountStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>sumStateIf(column, condition).</summary>
    public static byte[] SumStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>avgStateIf(column, condition).</summary>
    public static byte[] AvgStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>minStateIf(column, condition).</summary>
    public static byte[] MinStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>maxStateIf(column, condition).</summary>
    public static byte[] MaxStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>uniqStateIf(column, condition).</summary>
    public static byte[] UniqStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>uniqExactStateIf(column, condition).</summary>
    public static byte[] UniqExactStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>anyStateIf(column, condition).</summary>
    public static byte[] AnyStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>anyLastStateIf(column, condition).</summary>
    public static byte[] AnyLastStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantileStateIf(level)(column, condition).</summary>
    public static byte[] QuantileStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>medianStateIf(column, condition).</summary>
    public static byte[] MedianStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>stddevPopStateIf(column, condition).</summary>
    public static byte[] StddevPopStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>stddevSampStateIf(column, condition).</summary>
    public static byte[] StddevSampStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>varPopStateIf(column, condition).</summary>
    public static byte[] VarPopStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>varSampStateIf(column, condition).</summary>
    public static byte[] VarSampStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>uniqCombinedStateIf(column, condition).</summary>
    public static byte[] UniqCombinedStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>uniqCombined64StateIf(column, condition).</summary>
    public static byte[] UniqCombined64StateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>uniqHLL12StateIf(column, condition).</summary>
    public static byte[] UniqHLL12StateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>uniqThetaStateIf(column, condition).</summary>
    public static byte[] UniqThetaStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>argMaxStateIf(arg, val, condition).</summary>
    public static byte[] ArgMaxStateIf<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>argMinStateIf(arg, val, condition).</summary>
    public static byte[] ArgMinStateIf<TKey, TElement, TArg, TVal>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TArg> argSelector,
        Func<TElement, TVal> valSelector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantileTDigestStateIf(level)(column, condition).</summary>
    public static byte[] QuantileTDigestStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantileExactStateIf(level)(column, condition).</summary>
    public static byte[] QuantileExactStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantileTimingStateIf(level)(column, condition).</summary>
    public static byte[] QuantileTimingStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantileDDStateIf(acc, level)(column, condition).</summary>
    public static byte[] QuantileDDStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double relativeAccuracy,
        double level,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantilesStateIf(levels)(column, condition).</summary>
    public static byte[] QuantilesStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>quantilesTDigestStateIf(levels)(column, condition).</summary>
    public static byte[] QuantilesTDigestStateIf<TKey, TElement>(
        this IGrouping<TKey, TElement> grouping,
        double[] levels,
        Func<TElement, double> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>groupArrayStateIf(column, condition).</summary>
    public static byte[] GroupArrayStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>groupArrayStateIf(max)(column, condition).</summary>
    public static byte[] GroupArrayStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>groupUniqArrayStateIf(column, condition).</summary>
    public static byte[] GroupUniqArrayStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>groupUniqArrayStateIf(max)(column, condition).</summary>
    public static byte[] GroupUniqArrayStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int maxSize,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>topKStateIf(k)(column, condition).</summary>
    public static byte[] TopKStateIf<TKey, TElement, TValue>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    /// <summary>topKWeightedStateIf(k)(column, weight, condition).</summary>
    public static byte[] TopKWeightedStateIf<TKey, TElement, TValue, TWeight>(
        this IGrouping<TKey, TElement> grouping,
        int k,
        Func<TElement, TValue> selector,
        Func<TElement, TWeight> weightSelector,
        Func<TElement, bool> predicate) => Throw<byte[]>();

    #endregion

    private static T Throw<T>() => throw new InvalidOperationException(
        "This method is for LINQ translation only and should not be invoked directly. " +
        "Use it within projections, materialized views, or LINQ queries against ClickHouse.");
}
