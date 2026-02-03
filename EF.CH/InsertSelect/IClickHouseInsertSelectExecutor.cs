using System.Linq.Expressions;

namespace EF.CH.InsertSelect;

/// <summary>
/// Interface for executing server-side INSERT ... SELECT operations in ClickHouse.
/// Enables efficient data movement without client round-trips.
/// </summary>
public interface IClickHouseInsertSelectExecutor
{
    /// <summary>
    /// Executes an INSERT ... SELECT operation where source and target types are the same.
    /// The source query is executed on the server and results are inserted directly into the target table.
    /// </summary>
    /// <typeparam name="TTarget">The target entity type (also the source type).</typeparam>
    /// <param name="sourceQuery">The source query to select data from.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the insert operation.</returns>
    Task<ClickHouseInsertSelectResult> ExecuteAsync<TTarget>(
        IQueryable<TTarget> sourceQuery,
        CancellationToken cancellationToken = default) where TTarget : class;

    /// <summary>
    /// Executes an INSERT ... SELECT operation with a mapping expression to transform source data into target entities.
    /// The source query is executed on the server and results are inserted directly into the target table.
    /// </summary>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <typeparam name="TTarget">The target entity type.</typeparam>
    /// <param name="sourceQuery">The source query to select data from.</param>
    /// <param name="mapping">Expression to map source entities to target entities.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the insert operation.</returns>
    Task<ClickHouseInsertSelectResult> ExecuteAsync<TSource, TTarget>(
        IQueryable<TSource> sourceQuery,
        Expression<Func<TSource, TTarget>> mapping,
        CancellationToken cancellationToken = default)
        where TSource : class
        where TTarget : class;
}
