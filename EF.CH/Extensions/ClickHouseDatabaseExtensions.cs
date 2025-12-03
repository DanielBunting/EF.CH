using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for ClickHouse database operations on DatabaseFacade.
/// </summary>
public static class ClickHouseDatabaseExtensions
{
    #region Generic Entity Type Methods

    /// <summary>
    /// Optimizes the table for the specified entity type, triggering a merge of data parts.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTableAsync<TEntity>(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        return OptimizeTableAsync<TEntity>(database, configure: null, cancellationToken);
    }

    /// <summary>
    /// Optimizes the table with FINAL modifier, forcing a complete merge.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTableFinalAsync<TEntity>(
        this DatabaseFacade database,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        return OptimizeTableAsync<TEntity>(database, o => o.WithFinal(), cancellationToken);
    }

    /// <summary>
    /// Optimizes a specific partition of the table.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="partitionId">The partition ID (e.g., "202401" for monthly, "20240115" for daily).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTablePartitionAsync<TEntity>(
        this DatabaseFacade database,
        string partitionId,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        return OptimizeTableAsync<TEntity>(database, o => o.WithPartition(partitionId), cancellationToken);
    }

    /// <summary>
    /// Optimizes a specific partition with FINAL modifier.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="partitionId">The partition ID (e.g., "202401" for monthly, "20240115" for daily).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTablePartitionFinalAsync<TEntity>(
        this DatabaseFacade database,
        string partitionId,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        return OptimizeTableAsync<TEntity>(database, o => o.WithPartition(partitionId).WithFinal(), cancellationToken);
    }

    /// <summary>
    /// Optimizes the table with custom options.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="database">The database facade.</param>
    /// <param name="configure">An action to configure optimization options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <example>
    /// <code>
    /// await context.Database.OptimizeTableAsync&lt;Event&gt;(o => o
    ///     .WithPartition("202401")
    ///     .WithFinal()
    ///     .WithDeduplicate());
    /// </code>
    /// </example>
    public static Task<int> OptimizeTableAsync<TEntity>(
        this DatabaseFacade database,
        Action<OptimizeTableOptions>? configure,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var tableName = GetTableName<TEntity>(database);
        return OptimizeTableCoreAsync(database, tableName, configure, cancellationToken);
    }

    #endregion

    #region String Table Name Methods

    /// <summary>
    /// Optimizes a table by name.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTableAsync(
        this DatabaseFacade database,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        return OptimizeTableCoreAsync(database, tableName, configure: null, cancellationToken);
    }

    /// <summary>
    /// Optimizes a table by name with FINAL modifier.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTableFinalAsync(
        this DatabaseFacade database,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        return OptimizeTableCoreAsync(database, tableName, o => o.WithFinal(), cancellationToken);
    }

    /// <summary>
    /// Optimizes a specific partition of a table by name.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="partitionId">The partition ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTablePartitionAsync(
        this DatabaseFacade database,
        string tableName,
        string partitionId,
        CancellationToken cancellationToken = default)
    {
        return OptimizeTableCoreAsync(database, tableName, o => o.WithPartition(partitionId), cancellationToken);
    }

    /// <summary>
    /// Optimizes a specific partition of a table by name with FINAL modifier.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="partitionId">The partition ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTablePartitionFinalAsync(
        this DatabaseFacade database,
        string tableName,
        string partitionId,
        CancellationToken cancellationToken = default)
    {
        return OptimizeTableCoreAsync(database, tableName, o => o.WithPartition(partitionId).WithFinal(), cancellationToken);
    }

    /// <summary>
    /// Optimizes a table by name with custom options.
    /// </summary>
    /// <param name="database">The database facade.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="configure">An action to configure optimization options.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public static Task<int> OptimizeTableAsync(
        this DatabaseFacade database,
        string tableName,
        Action<OptimizeTableOptions>? configure,
        CancellationToken cancellationToken = default)
    {
        return OptimizeTableCoreAsync(database, tableName, configure, cancellationToken);
    }

    #endregion

    #region Private Helpers

    private static Task<int> OptimizeTableCoreAsync(
        DatabaseFacade database,
        string tableName,
        Action<OptimizeTableOptions>? configure,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var options = new OptimizeTableOptions();
        configure?.Invoke(options);

        var sql = BuildOptimizeSql(database, tableName, options);
        return database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    private static string BuildOptimizeSql(
        DatabaseFacade database,
        string tableName,
        OptimizeTableOptions options)
    {
        var sqlHelper = database.GetService<ISqlGenerationHelper>();
        var quotedTable = sqlHelper.DelimitIdentifier(tableName);

        var sql = $"OPTIMIZE TABLE {quotedTable}";

        if (!string.IsNullOrEmpty(options.Partition))
        {
            // Partition IDs are passed as string literals
            sql += $" PARTITION '{options.Partition}'";
        }

        if (options.Final)
        {
            sql += " FINAL";
        }

        if (options.Deduplicate)
        {
            sql += " DEDUPLICATE";
            if (options.DeduplicateBy?.Length > 0)
            {
                var columns = string.Join(", ",
                    options.DeduplicateBy.Select(c => sqlHelper.DelimitIdentifier(c)));
                sql += $" BY {columns}";
            }
        }

        return sql;
    }

    private static string GetTableName<TEntity>(DatabaseFacade database) where TEntity : class
    {
        var context = database.GetService<ICurrentDbContext>().Context;
        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' is not part of the model.");

        return entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TEntity).Name}' does not have a table name.");
    }

    #endregion
}
