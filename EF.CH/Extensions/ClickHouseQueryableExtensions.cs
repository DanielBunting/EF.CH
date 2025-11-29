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
                Expression.Constant(fraction)));
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
}
