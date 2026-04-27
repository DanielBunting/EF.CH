using System.Linq.Expressions;
using System.Reflection;

namespace EF.CH.Extensions;

/// <summary>
/// Internal expression-tree shapes for the multi-array ARRAY JOIN forms (3, 4, 5).
/// User code reaches these through <see cref="ArrayJoinBuilder{TEntity}"/> /
/// <see cref="ClickHouseQueryableExtensions.ArrayJoin{TEntity}"/>.
/// </summary>
public static class ClickHouseArrayJoinExtensions
{
    public static IQueryable<TResult> ArrayJoin3<TEntity, T1, T2, T3, TResult>(
        IQueryable<TEntity> source,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2,
        Expression<Func<TEntity, IEnumerable<T3>>> array3,
        Expression<Func<TEntity, T1, T2, T3, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                ArrayJoin3MethodInfo.MakeGenericMethod(typeof(TEntity), typeof(T1), typeof(T2), typeof(T3), typeof(TResult)),
                source.Expression,
                Expression.Quote(array1),
                Expression.Quote(array2),
                Expression.Quote(array3),
                Expression.Quote(resultSelector)));
    }

    public static IQueryable<TResult> ArrayJoin4<TEntity, T1, T2, T3, T4, TResult>(
        IQueryable<TEntity> source,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2,
        Expression<Func<TEntity, IEnumerable<T3>>> array3,
        Expression<Func<TEntity, IEnumerable<T4>>> array4,
        Expression<Func<TEntity, T1, T2, T3, T4, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                ArrayJoin4MethodInfo.MakeGenericMethod(typeof(TEntity), typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(TResult)),
                source.Expression,
                Expression.Quote(array1),
                Expression.Quote(array2),
                Expression.Quote(array3),
                Expression.Quote(array4),
                Expression.Quote(resultSelector)));
    }

    public static IQueryable<TResult> ArrayJoin5<TEntity, T1, T2, T3, T4, T5, TResult>(
        IQueryable<TEntity> source,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2,
        Expression<Func<TEntity, IEnumerable<T3>>> array3,
        Expression<Func<TEntity, IEnumerable<T4>>> array4,
        Expression<Func<TEntity, IEnumerable<T5>>> array5,
        Expression<Func<TEntity, T1, T2, T3, T4, T5, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.Provider.CreateQuery<TResult>(
            Expression.Call(
                null,
                ArrayJoin5MethodInfo.MakeGenericMethod(typeof(TEntity), typeof(T1), typeof(T2), typeof(T3), typeof(T4), typeof(T5), typeof(TResult)),
                source.Expression,
                Expression.Quote(array1),
                Expression.Quote(array2),
                Expression.Quote(array3),
                Expression.Quote(array4),
                Expression.Quote(array5),
                Expression.Quote(resultSelector)));
    }

    internal static readonly MethodInfo ArrayJoin3MethodInfo =
        typeof(ClickHouseArrayJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(ArrayJoin3));

    internal static readonly MethodInfo ArrayJoin4MethodInfo =
        typeof(ClickHouseArrayJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(ArrayJoin4));

    internal static readonly MethodInfo ArrayJoin5MethodInfo =
        typeof(ClickHouseArrayJoinExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(ArrayJoin5));
}
