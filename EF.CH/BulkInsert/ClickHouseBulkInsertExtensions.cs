using EF.CH.BulkInsert;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for bulk insert operations on DbContext and DbSet.
/// </summary>
public static class ClickHouseBulkInsertExtensions
{
    /// <summary>
    /// Bulk inserts a collection of entities into ClickHouse.
    /// Bypasses EF Core change tracking for maximum insert performance.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="entities">The entities to insert.</param>
    /// <param name="configure">Optional action to configure insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the bulk insert operation.</returns>
    public static Task<ClickHouseBulkInsertResult> BulkInsertAsync<TEntity>(
        this DbContext context,
        IEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var bulkInserter = context.GetService<IClickHouseBulkInserter>();
        return bulkInserter.InsertAsync(entities, configure, cancellationToken);
    }

    /// <summary>
    /// Bulk inserts an async stream of entities into ClickHouse.
    /// Uses streaming to handle large datasets without loading everything into memory.
    /// Bypasses EF Core change tracking for maximum insert performance.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="entities">The async stream of entities to insert.</param>
    /// <param name="configure">Optional action to configure insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the bulk insert operation.</returns>
    public static Task<ClickHouseBulkInsertResult> BulkInsertStreamingAsync<TEntity>(
        this DbContext context,
        IAsyncEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var bulkInserter = context.GetService<IClickHouseBulkInserter>();
        return bulkInserter.InsertStreamingAsync(entities, configure, cancellationToken);
    }

    /// <summary>
    /// Bulk inserts a collection of entities into ClickHouse.
    /// Bypasses EF Core change tracking for maximum insert performance.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="dbSet">The DbSet.</param>
    /// <param name="entities">The entities to insert.</param>
    /// <param name="configure">Optional action to configure insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the bulk insert operation.</returns>
    public static Task<ClickHouseBulkInsertResult> BulkInsertAsync<TEntity>(
        this DbSet<TEntity> dbSet,
        IEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var context = dbSet.GetService<ICurrentDbContext>().Context;
        var bulkInserter = context.GetService<IClickHouseBulkInserter>();
        return bulkInserter.InsertAsync(entities, configure, cancellationToken);
    }

    /// <summary>
    /// Bulk inserts an async stream of entities into ClickHouse.
    /// Uses streaming to handle large datasets without loading everything into memory.
    /// Bypasses EF Core change tracking for maximum insert performance.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="dbSet">The DbSet.</param>
    /// <param name="entities">The async stream of entities to insert.</param>
    /// <param name="configure">Optional action to configure insert options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the bulk insert operation.</returns>
    public static Task<ClickHouseBulkInsertResult> BulkInsertStreamingAsync<TEntity>(
        this DbSet<TEntity> dbSet,
        IAsyncEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var context = dbSet.GetService<ICurrentDbContext>().Context;
        var bulkInserter = context.GetService<IClickHouseBulkInserter>();
        return bulkInserter.InsertStreamingAsync(entities, configure, cancellationToken);
    }
}
