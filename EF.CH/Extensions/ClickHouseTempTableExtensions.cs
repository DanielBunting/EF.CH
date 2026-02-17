using EF.CH.TempTable;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for ClickHouse temporary table operations.
/// </summary>
public static class ClickHouseTempTableExtensions
{
    /// <summary>
    /// Creates an empty temporary table with the schema of the specified entity type.
    /// The table is session-scoped and dropped when the returned handle is disposed.
    /// </summary>
    /// <typeparam name="T">The entity type whose schema defines the temp table columns.</typeparam>
    /// <param name="context">The DbContext.</param>
    /// <param name="tableName">Optional table name. If not provided, a unique name is auto-generated.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handle for inserting, querying, and disposing the temp table.</returns>
    public static Task<TempTableHandle<T>> CreateTempTableAsync<T>(
        this DbContext context,
        string? tableName = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var manager = context.GetService<IClickHouseTempTableManager>();
        return manager.CreateAsync<T>(tableName, cancellationToken);
    }

    /// <summary>
    /// Creates a <see cref="TempTableScope"/> for managing multiple temporary tables.
    /// All tables created within the scope are dropped when the scope is disposed.
    /// </summary>
    /// <param name="context">The DbContext.</param>
    /// <returns>A scope that tracks and disposes temp tables.</returns>
    public static TempTableScope BeginTempTableScope(this DbContext context)
    {
        var manager = context.GetService<IClickHouseTempTableManager>();
        return new TempTableScope(manager);
    }

    /// <summary>
    /// Creates a temporary table and populates it with the results of this query.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The source query.</param>
    /// <param name="context">The DbContext.</param>
    /// <param name="tableName">Optional table name. If not provided, a unique name is auto-generated.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handle for querying and disposing the temp table.</returns>
    public static Task<TempTableHandle<T>> ToTempTableAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        string? tableName = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var manager = context.GetService<IClickHouseTempTableManager>();
        return manager.CreateFromQueryAsync(query, tableName, cancellationToken);
    }

    /// <summary>
    /// Inserts the results of this query into the specified temporary table.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="query">The source query.</param>
    /// <param name="handle">The temp table handle to insert into.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static Task InsertIntoTempTableAsync<T>(
        this IQueryable<T> query,
        TempTableHandle<T> handle,
        CancellationToken cancellationToken = default) where T : class
    {
        return handle.InsertFromQueryAsync(query, cancellationToken);
    }
}
