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

    /// <summary>
    /// Upserts a collection of entities. For engines that treat INSERT as an
    /// upsert (<c>KeeperMap</c>, <c>ReplacingMergeTree</c>), this is identical
    /// to <see cref="BulkInsertAsync{TEntity}(DbSet{TEntity}, IEnumerable{TEntity}, Action{ClickHouseBulkInsertOptions}?, CancellationToken)"/>
    /// but the name documents the intent. Bypasses EF's change tracker so
    /// duplicate keys don't trip <c>InvalidOperationException</c>.
    /// </summary>
    public static Task<ClickHouseBulkInsertResult> UpsertRangeAsync<TEntity>(
        this DbSet<TEntity> dbSet,
        IEnumerable<TEntity> entities,
        CancellationToken cancellationToken = default) where TEntity : class
        => dbSet.BulkInsertAsync(entities, configure: null, cancellationToken);

    /// <summary>
    /// Issues <c>INSERT INTO target SELECT …</c> from the given source
    /// <see cref="IQueryable{TEntity}"/>. Stays server-side — no rows flow
    /// through the client. Use for AMT→AMT chaining where
    /// <see cref="ClickHouseAggregates.CountMergeState{TSource}"/> and friends
    /// re-state merged values for the downstream target.
    /// </summary>
    public static Task<int> InsertFromQueryAsync<TEntity>(
        this DbSet<TEntity> targetSet,
        IQueryable<TEntity> sourceQuery,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(sourceQuery);
        var context = targetSet.GetService<ICurrentDbContext>().Context;
        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"Entity type '{typeof(TEntity).Name}' is not in the model.");
        var tableName = entityType.GetTableName()
            ?? throw new InvalidOperationException($"Entity '{typeof(TEntity).Name}' has no table name.");

        var selectSql = sourceQuery.ToQueryString();
        var helper = context.GetService<Microsoft.EntityFrameworkCore.Storage.ISqlGenerationHelper>();
        var insertSql = $"INSERT INTO {helper.DelimitIdentifier(tableName)} {selectSql}";
        return context.Database.ExecuteSqlRawAsync(insertSql, cancellationToken);
    }
}
