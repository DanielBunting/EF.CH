using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Extensions;

/// <summary>
/// Convenience extension methods on <see cref="IQueryable{T}"/> that wrap the
/// <see cref="ClickHouseTextSearchDbFunctionsExtensions"/> DbFunctions for common text search patterns.
/// </summary>
public static class ClickHouseTextSearchQueryExtensions
{
    private static readonly MethodInfo HasTokenMethod =
        typeof(ClickHouseTextSearchDbFunctionsExtensions).GetMethod(
            nameof(ClickHouseTextSearchDbFunctionsExtensions.HasToken),
            new[] { typeof(DbFunctions), typeof(string), typeof(string) })!;

    private static readonly MethodInfo HasTokenCaseInsensitiveMethod =
        typeof(ClickHouseTextSearchDbFunctionsExtensions).GetMethod(
            nameof(ClickHouseTextSearchDbFunctionsExtensions.HasTokenCaseInsensitive),
            new[] { typeof(DbFunctions), typeof(string), typeof(string) })!;

    private static readonly MethodInfo MultiSearchAnyMethod =
        typeof(ClickHouseTextSearchDbFunctionsExtensions).GetMethod(
            nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchAny),
            new[] { typeof(DbFunctions), typeof(string), typeof(string[]) })!;

    private static readonly MethodInfo MultiSearchAnyCaseInsensitiveMethod =
        typeof(ClickHouseTextSearchDbFunctionsExtensions).GetMethod(
            nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchAnyCaseInsensitive),
            new[] { typeof(DbFunctions), typeof(string), typeof(string[]) })!;

    private static readonly MethodInfo NgramSearchMethod =
        typeof(ClickHouseTextSearchDbFunctionsExtensions).GetMethod(
            nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramSearch),
            new[] { typeof(DbFunctions), typeof(string), typeof(string) })!;

    private static readonly MethodInfo NgramSearchCaseInsensitiveMethod =
        typeof(ClickHouseTextSearchDbFunctionsExtensions).GetMethod(
            nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramSearchCaseInsensitive),
            new[] { typeof(DbFunctions), typeof(string), typeof(string) })!;

    /// <summary>
    /// Filters the query to rows where the selected text column contains the given token.
    /// Translates to <c>WHERE hasToken(column, token)</c> or <c>WHERE hasTokenCaseInsensitive(column, token)</c>.
    /// </summary>
    public static IQueryable<T> ContainsToken<T>(
        this IQueryable<T> source,
        Expression<Func<T, string>> textSelector,
        string token,
        bool caseInsensitive = false)
    {
        var param = textSelector.Parameters[0];
        var method = caseInsensitive ? HasTokenCaseInsensitiveMethod : HasTokenMethod;

        var call = Expression.Call(
            method,
            Expression.Property(null, typeof(EfClass), nameof(EfClass.Functions)),
            textSelector.Body,
            Expression.Constant(token));

        var predicate = Expression.Lambda<Func<T, bool>>(call, param);
        return source.Where(predicate);
    }

    /// <summary>
    /// Filters the query to rows where the selected text column contains any of the given terms.
    /// Translates to <c>WHERE multiSearchAny(column, [terms])</c>.
    /// </summary>
    public static IQueryable<T> ContainsAny<T>(
        this IQueryable<T> source,
        Expression<Func<T, string>> textSelector,
        string[] terms,
        bool caseInsensitive = false)
    {
        var param = textSelector.Parameters[0];
        var method = caseInsensitive ? MultiSearchAnyCaseInsensitiveMethod : MultiSearchAnyMethod;

        var call = Expression.Call(
            method,
            Expression.Property(null, typeof(EfClass), nameof(EfClass.Functions)),
            textSelector.Body,
            Expression.Constant(terms));

        var predicate = Expression.Lambda<Func<T, bool>>(call, param);
        return source.Where(predicate);
    }

    /// <summary>
    /// Filters the query to rows where the n-gram similarity score exceeds the threshold,
    /// ordered by descending score. Translates to
    /// <c>WHERE ngramSearch(column, query) &gt; threshold ORDER BY ngramSearch(column, query) DESC</c>.
    /// </summary>
    public static IOrderedQueryable<T> FuzzyMatch<T>(
        this IQueryable<T> source,
        Expression<Func<T, string>> textSelector,
        string query,
        float threshold = 0.3f,
        bool caseInsensitive = false)
    {
        var param = textSelector.Parameters[0];
        var method = caseInsensitive ? NgramSearchCaseInsensitiveMethod : NgramSearchMethod;

        var scoreCall = Expression.Call(
            method,
            Expression.Property(null, typeof(EfClass), nameof(EfClass.Functions)),
            textSelector.Body,
            Expression.Constant(query));

        // WHERE ngramSearch(...) > threshold
        var filterPredicate = Expression.Lambda<Func<T, bool>>(
            Expression.GreaterThan(scoreCall, Expression.Constant(threshold)),
            param);

        // ORDER BY ngramSearch(...) DESC
        var scoreSelector = Expression.Lambda<Func<T, float>>(scoreCall, param);

        return source.Where(filterPredicate).OrderByDescending(scoreSelector);
    }
}
