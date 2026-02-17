namespace EF.CH.BulkInsert;

/// <summary>
/// Interface for performing bulk insert operations in ClickHouse.
/// Bypasses EF Core change tracking for maximum insert performance.
/// </summary>
public interface IClickHouseBulkInserter
{
    /// <summary>
    /// Bulk inserts a collection of entities into ClickHouse.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="entities">The entities to insert.</param>
    /// <param name="configure">Optional action to configure insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the bulk insert operation.</returns>
    Task<ClickHouseBulkInsertResult> InsertAsync<TEntity>(
        IEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class;

    /// <summary>
    /// Bulk inserts an async stream of entities into ClickHouse.
    /// Uses streaming to handle large datasets without loading everything into memory.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="entities">The async stream of entities to insert.</param>
    /// <param name="configure">Optional action to configure insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the bulk insert operation.</returns>
    Task<ClickHouseBulkInsertResult> InsertStreamingAsync<TEntity>(
        IAsyncEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class;
}
