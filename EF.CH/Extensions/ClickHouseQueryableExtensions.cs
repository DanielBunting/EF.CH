using System.Linq.Expressions;
using System.Reflection;

namespace EF.CH.Extensions;

/// <summary>
/// Provides ClickHouse-specific LINQ extension methods.
/// </summary>
public static class ClickHouseQueryableExtensions
{
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
    /// <para>
    /// SAMPLE provides fast approximate results by reading only a fraction of the data.
    /// Useful for exploratory analytics on very large tables.
    /// </para>
    /// <para>
    /// Note: Due to EF Core's parameterization, ToQueryString() may not show the SAMPLE clause correctly.
    /// The feature works correctly when the query is actually executed against ClickHouse.
    /// </para>
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
                Expression.Constant(fraction)));
    }

    /// <summary>
    /// Applies probabilistic SAMPLE with a seed offset for reproducible sampling.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Note: Due to EF Core's parameterization, ToQueryString() may not show the SAMPLE clause correctly.
    /// The feature works correctly when the query is actually executed against ClickHouse.
    /// </para>
    /// </remarks>
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
                Expression.Constant(fraction),
                Expression.Constant(offset)));
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
                Expression.Constant(settingsCopy)));
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
                Expression.Constant(name),
                Expression.Constant(value, typeof(object))));
    }

    internal static readonly MethodInfo WithSettingsMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(WithSettings));

    internal static readonly MethodInfo WithSettingMethodInfo =
        typeof(ClickHouseQueryableExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(WithSetting));
}
