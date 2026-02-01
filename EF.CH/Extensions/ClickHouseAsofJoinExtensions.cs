using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Query;

namespace EF.CH.Extensions;

/// <summary>
/// Provides ClickHouse ASOF JOIN extension methods for LINQ queries.
/// ASOF JOIN matches rows based on the closest timestamp rather than exact equality,
/// making it ideal for time-series data like financial trades and quotes.
/// </summary>
public static class ClickHouseAsofJoinExtensions
{
    /// <summary>
    /// Performs an ASOF JOIN between two sequences, matching each outer element to the
    /// inner element with the closest ASOF key value that satisfies the comparison condition.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ASOF JOIN is similar to a regular inner join but instead of requiring exact equality
    /// on the ASOF column, it finds the closest match. This is essential for time-series data.
    /// </para>
    /// <para>
    /// Example SQL generated:
    /// <code>
    /// SELECT t.*, q.BidPrice
    /// FROM Trades AS t
    /// ASOF JOIN Quotes AS q
    /// ON t.Symbol = q.Symbol AND t.TradeTime >= q.QuoteTime
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="TOuter">The type of elements in the outer sequence.</typeparam>
    /// <typeparam name="TInner">The type of elements in the inner sequence.</typeparam>
    /// <typeparam name="TKey">The type of the equality key.</typeparam>
    /// <typeparam name="TAsofKey">The type of the ASOF key (must be comparable, typically DateTime or numeric).</typeparam>
    /// <typeparam name="TResult">The type of the result elements.</typeparam>
    /// <param name="outer">The outer sequence.</param>
    /// <param name="inner">The inner sequence to join with.</param>
    /// <param name="outerKeySelector">A function to extract the equality key from each outer element.</param>
    /// <param name="innerKeySelector">A function to extract the equality key from each inner element.</param>
    /// <param name="outerAsofSelector">A function to extract the ASOF key from each outer element.</param>
    /// <param name="innerAsofSelector">A function to extract the ASOF key from each inner element.</param>
    /// <param name="asofType">The comparison type for the ASOF condition.</param>
    /// <param name="resultSelector">A function to create a result element from an outer and inner element.</param>
    /// <returns>An IQueryable containing the ASOF JOIN results.</returns>
    /// <example>
    /// <code>
    /// var tradesWithQuotes = context.Trades
    ///     .AsofJoin(
    ///         context.Quotes,
    ///         t => t.Symbol,           // Equality key (outer)
    ///         q => q.Symbol,           // Equality key (inner)
    ///         t => t.TradeTime,        // ASOF key (outer)
    ///         q => q.QuoteTime,        // ASOF key (inner)
    ///         AsofJoinType.GreaterOrEqual,
    ///         (t, q) => new { t.TradeId, t.Price, QuotePrice = q.BidPrice })
    ///     .ToList();
    /// </code>
    /// </example>
    public static IQueryable<TResult> AsofJoin<TOuter, TInner, TKey, TAsofKey, TResult>(
        this IQueryable<TOuter> outer,
        IQueryable<TInner> inner,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TOuter, TAsofKey>> outerAsofSelector,
        Expression<Func<TInner, TAsofKey>> innerAsofSelector,
        AsofJoinType asofType,
        Expression<Func<TOuter, TInner, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(outer);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(outerKeySelector);
        ArgumentNullException.ThrowIfNull(innerKeySelector);
        ArgumentNullException.ThrowIfNull(outerAsofSelector);
        ArgumentNullException.ThrowIfNull(innerAsofSelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return outer.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                AsofJoinMethodInfo.MakeGenericMethod(
                    typeof(TOuter), typeof(TInner), typeof(TKey), typeof(TAsofKey), typeof(TResult)),
                outer.Expression,
                inner.Expression,
                Expression.Quote(outerKeySelector),
                Expression.Quote(innerKeySelector),
                Expression.Quote(outerAsofSelector),
                Expression.Quote(innerAsofSelector),
                WrapInEfConstant(asofType),
                Expression.Quote(resultSelector)));
    }

    /// <summary>
    /// Performs an ASOF LEFT JOIN between two sequences. Similar to AsofJoin but preserves
    /// all outer elements even when no matching inner element is found.
    /// </summary>
    /// <remarks>
    /// <para>
    /// In an ASOF LEFT JOIN, outer elements without a matching inner element will have
    /// null values for the inner element in the result selector.
    /// </para>
    /// <para>
    /// Example SQL generated:
    /// <code>
    /// SELECT t.*, q.BidPrice
    /// FROM Trades AS t
    /// ASOF LEFT JOIN Quotes AS q
    /// ON t.Symbol = q.Symbol AND t.TradeTime >= q.QuoteTime
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="TOuter">The type of elements in the outer sequence.</typeparam>
    /// <typeparam name="TInner">The type of elements in the inner sequence (must be a reference type).</typeparam>
    /// <typeparam name="TKey">The type of the equality key.</typeparam>
    /// <typeparam name="TAsofKey">The type of the ASOF key.</typeparam>
    /// <typeparam name="TResult">The type of the result elements.</typeparam>
    /// <param name="outer">The outer sequence.</param>
    /// <param name="inner">The inner sequence to join with.</param>
    /// <param name="outerKeySelector">A function to extract the equality key from each outer element.</param>
    /// <param name="innerKeySelector">A function to extract the equality key from each inner element.</param>
    /// <param name="outerAsofSelector">A function to extract the ASOF key from each outer element.</param>
    /// <param name="innerAsofSelector">A function to extract the ASOF key from each inner element.</param>
    /// <param name="asofType">The comparison type for the ASOF condition.</param>
    /// <param name="resultSelector">A function to create a result element. The inner parameter may be null.</param>
    /// <returns>An IQueryable containing the ASOF LEFT JOIN results.</returns>
    public static IQueryable<TResult> AsofLeftJoin<TOuter, TInner, TKey, TAsofKey, TResult>(
        this IQueryable<TOuter> outer,
        IQueryable<TInner> inner,
        Expression<Func<TOuter, TKey>> outerKeySelector,
        Expression<Func<TInner, TKey>> innerKeySelector,
        Expression<Func<TOuter, TAsofKey>> outerAsofSelector,
        Expression<Func<TInner, TAsofKey>> innerAsofSelector,
        AsofJoinType asofType,
        Expression<Func<TOuter, TInner?, TResult>> resultSelector)
        where TInner : class
    {
        ArgumentNullException.ThrowIfNull(outer);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(outerKeySelector);
        ArgumentNullException.ThrowIfNull(innerKeySelector);
        ArgumentNullException.ThrowIfNull(outerAsofSelector);
        ArgumentNullException.ThrowIfNull(innerAsofSelector);
        ArgumentNullException.ThrowIfNull(resultSelector);

        return outer.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                AsofLeftJoinMethodInfo.MakeGenericMethod(
                    typeof(TOuter), typeof(TInner), typeof(TKey), typeof(TAsofKey), typeof(TResult)),
                outer.Expression,
                inner.Expression,
                Expression.Quote(outerKeySelector),
                Expression.Quote(innerKeySelector),
                Expression.Quote(outerAsofSelector),
                Expression.Quote(innerAsofSelector),
                WrapInEfConstant(asofType),
                Expression.Quote(resultSelector)));
    }

    /// <summary>
    /// Wraps a constant value in an EF.Constant() call to prevent EF Core from parameterizing it.
    /// </summary>
    private static Expression WrapInEfConstant<T>(T value)
    {
        return Expression.Call(
            typeof(Microsoft.EntityFrameworkCore.EF).GetMethod(nameof(Microsoft.EntityFrameworkCore.EF.Constant))!.MakeGenericMethod(typeof(T)),
            Expression.Constant(value, typeof(T)));
    }

    internal static readonly MethodInfo AsofJoinMethodInfo =
        typeof(ClickHouseAsofJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(AsofJoin) && m.GetParameters().Length == 8);

    internal static readonly MethodInfo AsofLeftJoinMethodInfo =
        typeof(ClickHouseAsofJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(AsofLeftJoin) && m.GetParameters().Length == 8);
}
