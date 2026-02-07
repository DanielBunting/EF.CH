namespace EF.CH.Extensions;

/// <summary>
/// Provides convenience extension methods for ClickHouse set operations (UNION ALL, UNION DISTINCT, INTERSECT, EXCEPT).
/// </summary>
/// <remarks>
/// <para>
/// EF Core's built-in set operations map to ClickHouse as follows:
/// - <c>Concat()</c> → UNION ALL (keeps duplicates)
/// - <c>Union()</c> → UNION DISTINCT (removes duplicates)
/// - <c>Intersect()</c> → INTERSECT DISTINCT
/// - <c>Except()</c> → EXCEPT DISTINCT
/// </para>
/// <para>
/// These extension methods add convenience for chaining multiple set operations
/// and provide a fluent builder API.
/// </para>
/// </remarks>
public static class ClickHouseSetOperationExtensions
{
    /// <summary>
    /// Combines multiple queries using UNION ALL (keeps duplicates).
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="source">The first query.</param>
    /// <param name="queries">Additional queries to union.</param>
    /// <returns>A combined queryable using UNION ALL.</returns>
    public static IQueryable<T> UnionAll<T>(this IQueryable<T> source, params IQueryable<T>[] queries)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(queries);

        var result = source;
        foreach (var query in queries)
        {
            result = result.Concat(query);
        }
        return result;
    }

    /// <summary>
    /// Combines multiple queries using UNION DISTINCT (removes duplicates).
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="source">The first query.</param>
    /// <param name="queries">Additional queries to union.</param>
    /// <returns>A combined queryable using UNION DISTINCT.</returns>
    public static IQueryable<T> UnionDistinct<T>(this IQueryable<T> source, params IQueryable<T>[] queries)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(queries);

        var result = source;
        foreach (var query in queries)
        {
            result = result.Union(query);
        }
        return result;
    }

    /// <summary>
    /// Creates a fluent set operation builder for chaining multiple set operations.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="source">The initial query.</param>
    /// <returns>A <see cref="SetOperationBuilder{T}"/> for fluent chaining.</returns>
    public static SetOperationBuilder<T> AsSetOperation<T>(this IQueryable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new SetOperationBuilder<T>(source);
    }
}

/// <summary>
/// Fluent builder for chaining ClickHouse set operations.
/// </summary>
/// <typeparam name="T">The entity type.</typeparam>
public class SetOperationBuilder<T>
{
    private IQueryable<T> _query;

    internal SetOperationBuilder(IQueryable<T> query)
    {
        _query = query;
    }

    /// <summary>
    /// Appends a query using UNION ALL (keeps duplicates).
    /// </summary>
    public SetOperationBuilder<T> UnionAll(IQueryable<T> other)
    {
        _query = _query.Concat(other);
        return this;
    }

    /// <summary>
    /// Appends a query using UNION DISTINCT (removes duplicates).
    /// </summary>
    public SetOperationBuilder<T> UnionDistinct(IQueryable<T> other)
    {
        _query = _query.Union(other);
        return this;
    }

    /// <summary>
    /// Applies INTERSECT with another query.
    /// </summary>
    public SetOperationBuilder<T> Intersect(IQueryable<T> other)
    {
        _query = _query.Intersect(other);
        return this;
    }

    /// <summary>
    /// Applies EXCEPT with another query.
    /// </summary>
    public SetOperationBuilder<T> Except(IQueryable<T> other)
    {
        _query = _query.Except(other);
        return this;
    }

    /// <summary>
    /// Returns the underlying <see cref="IQueryable{T}"/> for further LINQ operations.
    /// </summary>
    public IQueryable<T> Build() => _query;
}
