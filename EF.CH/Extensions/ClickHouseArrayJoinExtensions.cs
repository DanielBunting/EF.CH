using System.Linq.Expressions;
using System.Reflection;

namespace EF.CH.Extensions;

/// <summary>
/// Provides ClickHouse ARRAY JOIN extension methods for LINQ queries.
/// ARRAY JOIN "explodes" array columns into separate rows, similar to UNNEST in PostgreSQL.
/// </summary>
public static class ClickHouseArrayJoinExtensions
{
    /// <summary>
    /// Performs an ARRAY JOIN, exploding an array column into separate rows.
    /// Rows with empty arrays are excluded from the result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ARRAY JOIN creates one row for each element in the array column.
    /// This is useful for log analysis with tags, event data with nested attributes, etc.
    /// </para>
    /// <para>
    /// Example SQL generated:
    /// <code>
    /// SELECT e.Id, tag
    /// FROM Events AS e
    /// ARRAY JOIN e.Tags AS tag
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TElement">The type of elements in the array.</typeparam>
    /// <typeparam name="TResult">The type of the result elements.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="arraySelector">A function to select the array property to explode.</param>
    /// <param name="resultSelector">A function to create a result element from the source and array element.</param>
    /// <returns>An IQueryable containing one row per array element.</returns>
    /// <example>
    /// <code>
    /// var exploded = context.Events
    ///     .ArrayJoin(
    ///         e => e.Tags,
    ///         (e, tag) => new { e.Name, Tag = tag })
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TResult> ArrayJoin<TSource, TElement, TResult>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, IEnumerable<TElement>>> arraySelector,
        Expression<Func<TSource, TElement, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(arraySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                ArrayJoinMethodInfo.MakeGenericMethod(typeof(TSource), typeof(TElement), typeof(TResult)),
                source.Expression,
                Expression.Quote(arraySelector),
                Expression.Quote(resultSelector)));
    }

    /// <summary>
    /// Performs a LEFT ARRAY JOIN, exploding an array column into separate rows.
    /// Rows with empty arrays are preserved with null element values.
    /// </summary>
    /// <remarks>
    /// <para>
    /// LEFT ARRAY JOIN is similar to ARRAY JOIN but preserves rows where the array is empty.
    /// The array element will be null (or default value) for such rows.
    /// </para>
    /// <para>
    /// Example SQL generated:
    /// <code>
    /// SELECT e.Id, tag
    /// FROM Events AS e
    /// LEFT ARRAY JOIN e.Tags AS tag
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TElement">The type of elements in the array.</typeparam>
    /// <typeparam name="TResult">The type of the result elements.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="arraySelector">A function to select the array property to explode.</param>
    /// <param name="resultSelector">A function to create a result element. The element parameter may be default/null for empty arrays.</param>
    /// <returns>An IQueryable containing one row per array element, plus rows for empty arrays.</returns>
    /// <example>
    /// <code>
    /// var exploded = context.Events
    ///     .LeftArrayJoin(
    ///         e => e.Tags,
    ///         (e, tag) => new { e.Name, Tag = tag ?? "(no tag)" })
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TResult> LeftArrayJoin<TSource, TElement, TResult>(
        this IQueryable<TSource> source,
        Expression<Func<TSource, IEnumerable<TElement>>> arraySelector,
        Expression<Func<TSource, TElement?, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(arraySelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                LeftArrayJoinMethodInfo.MakeGenericMethod(typeof(TSource), typeof(TElement), typeof(TResult)),
                source.Expression,
                Expression.Quote(arraySelector),
                Expression.Quote(resultSelector)));
    }

    internal static readonly MethodInfo ArrayJoinMethodInfo =
        typeof(ClickHouseArrayJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(ArrayJoin) && m.GetParameters().Length == 3);

    internal static readonly MethodInfo LeftArrayJoinMethodInfo =
        typeof(ClickHouseArrayJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(LeftArrayJoin) && m.GetParameters().Length == 3);
}
