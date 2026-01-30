using System.Linq.Expressions;
using System.Reflection;

namespace EF.CH.Extensions;

/// <summary>
/// Provides ClickHouse-specific LINQ extension methods.
/// </summary>
public static class ClickHouseQueryableExtensions
{
    /// <summary>
    /// Wraps a constant value in an EF.Constant() call to prevent EF Core from parameterizing it.
    /// The ClickHouseEvaluatableExpressionFilterPlugin will prevent evaluation of EF.Constant() calls,
    /// and the translator will unwrap them to get the actual value.
    /// </summary>
    private static Expression WrapInEfConstant<T>(T value)
    {
        return Expression.Call(
            typeof(Microsoft.EntityFrameworkCore.EF).GetMethod(nameof(Microsoft.EntityFrameworkCore.EF.Constant))!.MakeGenericMethod(typeof(T)),
            Expression.Constant(value, typeof(T)));
    }

    /// <summary>
    /// Applies the FINAL modifier to force deduplication for ReplacingMergeTree tables.
    /// </summary>
    /// <remarks>
    /// FINAL causes ClickHouse to merge rows on-the-fly during the query, ensuring
    /// you see the latest version of each row. This has performance implications
    /// and should be used judiciously.
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <returns>A queryable with FINAL applied.</returns>
    public static IQueryable<TEntity> Final<TEntity>(this IQueryable<TEntity> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                FinalMethodInfo.MakeGenericMethod(typeof(TEntity)),
                source.Expression));
    }

    /// <summary>
    /// Applies probabilistic SAMPLE to the query for approximate results on large datasets.
    /// </summary>
    /// <remarks>
    /// SAMPLE provides fast approximate results by reading only a fraction of the data.
    /// Useful for exploratory analytics on very large tables.
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="fraction">The fraction of rows to sample (0.0 to 1.0).</param>
    /// <returns>A queryable with SAMPLE applied.</returns>
    public static IQueryable<TEntity> Sample<TEntity>(this IQueryable<TEntity> source, double fraction)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (fraction <= 0 || fraction > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(fraction),
                "Sample fraction must be between 0 (exclusive) and 1 (inclusive).");
        }

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                SampleMethodInfo.MakeGenericMethod(typeof(TEntity)),
                source.Expression,
                WrapInEfConstant(fraction)));
    }

    /// <summary>
    /// Applies probabilistic SAMPLE with a seed offset for reproducible sampling.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="fraction">The fraction of rows to sample (0.0 to 1.0).</param>
    /// <param name="offset">The offset for reproducible sampling.</param>
    /// <returns>A queryable with SAMPLE applied.</returns>
    public static IQueryable<TEntity> Sample<TEntity>(this IQueryable<TEntity> source, double fraction, double offset)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (fraction <= 0 || fraction > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(fraction),
                "Sample fraction must be between 0 (exclusive) and 1 (inclusive).");
        }

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                SampleWithOffsetMethodInfo.MakeGenericMethod(typeof(TEntity)),
                source.Expression,
                WrapInEfConstant(fraction),
                WrapInEfConstant(offset)));
    }

    internal static readonly MethodInfo FinalMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethod(
            nameof(Final),
            BindingFlags.Public | BindingFlags.Static)!;

    internal static readonly MethodInfo SampleMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(Sample) && m.GetParameters().Length == 2);

    internal static readonly MethodInfo SampleWithOffsetMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(Sample) && m.GetParameters().Length == 3);

    /// <summary>
    /// Applies ClickHouse query settings to the query.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Settings are appended to the query as a SETTINGS clause, e.g.:
    /// SELECT ... FROM ... SETTINGS max_threads = 4, optimize_read_in_order = 1
    /// </para>
    /// <para>
    /// Common settings include:
    /// - max_threads: Maximum number of threads for query execution
    /// - optimize_read_in_order: Optimize reading in ORDER BY key order
    /// - max_block_size: Maximum block size for reading
    /// - max_rows_to_read: Limit rows read (query fails if exceeded)
    /// - max_execution_time: Query timeout in seconds
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="settings">Dictionary of setting names and values.</param>
    /// <returns>A queryable with SETTINGS applied.</returns>
    public static IQueryable<TEntity> WithSettings<TEntity>(
        this IQueryable<TEntity> source,
        IDictionary<string, object> settings)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(settings);

        if (settings.Count == 0)
        {
            return source;
        }

        // Create a copy of the settings dictionary to avoid mutations
        var settingsCopy = new Dictionary<string, object>(settings);

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                WithSettingsMethodInfo.MakeGenericMethod(typeof(TEntity)),
                source.Expression,
                WrapInEfConstant(settingsCopy)));
    }

    /// <summary>
    /// Applies a single ClickHouse query setting to the query.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="name">The setting name.</param>
    /// <param name="value">The setting value.</param>
    /// <returns>A queryable with the setting applied.</returns>
    public static IQueryable<TEntity> WithSetting<TEntity>(
        this IQueryable<TEntity> source,
        string name,
        object value)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(name);

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                WithSettingMethodInfo.MakeGenericMethod(typeof(TEntity)),
                source.Expression,
                WrapInEfConstant(name),
                WrapInEfConstant(value)));
    }

    internal static readonly MethodInfo WithSettingsMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(WithSettings));

    internal static readonly MethodInfo WithSettingMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(WithSetting));

    /// <summary>
    /// Applies PREWHERE clause for optimized filtering before column reads.
    /// </summary>
    /// <remarks>
    /// <para>
    /// PREWHERE is a ClickHouse-specific optimization that filters rows before reading
    /// all columns. It reads only the filter columns first, then reads remaining columns
    /// for matching rows, reducing I/O for large tables with selective filters.
    /// </para>
    /// <para>
    /// Best suited for filtering on indexed/sorted columns (ORDER BY key columns)
    /// with highly selective predicates that eliminate most rows.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="predicate">The filter predicate to apply as PREWHERE.</param>
    /// <returns>A queryable with PREWHERE applied.</returns>
    public static IQueryable<TEntity> PreWhere<TEntity>(
        this IQueryable<TEntity> source,
        Expression<Func<TEntity, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(predicate);

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                PreWhereMethodInfo.MakeGenericMethod(typeof(TEntity)),
                source.Expression,
                Expression.Quote(predicate)));
    }

    internal static readonly MethodInfo PreWhereMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(PreWhere) && m.GetParameters().Length == 2);

    /// <summary>
    /// Applies LIMIT BY clause to return top N rows per group based on the specified key.
    /// </summary>
    /// <remarks>
    /// <para>
    /// LIMIT BY is a ClickHouse-specific clause that limits the number of rows per group.
    /// This is useful for "top N per category" queries without needing window functions.
    /// </para>
    /// <para>
    /// The key selector defines the grouping columns. Use OrderBy/OrderByDescending before
    /// LimitBy to control which rows are kept within each group.
    /// </para>
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TKey">The key type (single column or anonymous type for compound keys).</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="limit">The maximum number of rows to return per group.</param>
    /// <param name="keySelector">Expression selecting the grouping key column(s).</param>
    /// <returns>A queryable with LIMIT BY applied.</returns>
    /// <example>
    /// <code>
    /// // Top 5 events per category
    /// var results = context.Events
    ///     .OrderByDescending(e => e.Score)
    ///     .LimitBy(5, e => e.Category)
    ///     .ToList();
    ///
    /// // Compound key: top 3 per category and region
    /// var results = context.Events
    ///     .OrderByDescending(e => e.Score)
    ///     .LimitBy(3, e => new { e.Category, e.Region })
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TEntity> LimitBy<TEntity, TKey>(
        this IQueryable<TEntity> source,
        int limit,
        Expression<Func<TEntity, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);

        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit),
                "Limit must be a positive integer.");
        }

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                LimitByMethodInfo.MakeGenericMethod(typeof(TEntity), typeof(TKey)),
                source.Expression,
                WrapInEfConstant(limit),
                Expression.Quote(keySelector)));
    }

    /// <summary>
    /// Applies LIMIT BY clause with an offset to skip rows before taking top N per group.
    /// </summary>
    /// <remarks>
    /// LIMIT BY with offset skips the first N rows in each group before returning the limit.
    /// This is useful for pagination within groups.
    /// </remarks>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TKey">The key type (single column or anonymous type for compound keys).</typeparam>
    /// <param name="source">The source queryable.</param>
    /// <param name="offset">The number of rows to skip per group.</param>
    /// <param name="limit">The maximum number of rows to return per group after skipping.</param>
    /// <param name="keySelector">Expression selecting the grouping key column(s).</param>
    /// <returns>A queryable with LIMIT offset, limit BY applied.</returns>
    /// <example>
    /// <code>
    /// // Skip 2, take 5 per user (rows 3-7 per user)
    /// var results = context.Events
    ///     .OrderByDescending(e => e.CreatedAt)
    ///     .LimitBy(2, 5, e => e.UserId)
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TEntity> LimitBy<TEntity, TKey>(
        this IQueryable<TEntity> source,
        int offset,
        int limit,
        Expression<Func<TEntity, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);

        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset),
                "Offset must be non-negative.");
        }

        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit),
                "Limit must be a positive integer.");
        }

        return source.Provider.CreateQuery<TEntity>(
            Expression.Call(
                null,
                LimitByWithOffsetMethodInfo.MakeGenericMethod(typeof(TEntity), typeof(TKey)),
                source.Expression,
                WrapInEfConstant(offset),
                WrapInEfConstant(limit),
                Expression.Quote(keySelector)));
    }

    internal static readonly MethodInfo LimitByMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(LimitBy) && m.GetParameters().Length == 3);

    internal static readonly MethodInfo LimitByWithOffsetMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(LimitBy) && m.GetParameters().Length == 4);
}
