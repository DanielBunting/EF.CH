namespace EF.CH.TempTable;

/// <summary>
/// Service for creating and managing ClickHouse temporary tables.
/// Tables use the Memory engine with unique names and are automatically dropped when disposed.
/// </summary>
public interface IClickHouseTempTableManager
{
    /// <summary>
    /// Creates an empty temporary table with the schema of the specified entity type.
    /// </summary>
    /// <typeparam name="T">The entity type whose schema defines the temp table columns.</typeparam>
    /// <param name="tableName">Optional table name. If not provided, a unique name is auto-generated.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handle for inserting, querying, and disposing the temp table.</returns>
    Task<TempTableHandle<T>> CreateAsync<T>(string? tableName = null, CancellationToken cancellationToken = default) where T : class;

    /// <summary>
    /// Creates a temporary table and populates it with the results of a query.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="sourceQuery">The query whose results populate the temp table.</param>
    /// <param name="tableName">Optional table name. If not provided, a unique name is auto-generated.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handle for inserting, querying, and disposing the temp table.</returns>
    Task<TempTableHandle<T>> CreateFromQueryAsync<T>(IQueryable<T> sourceQuery, string? tableName = null, CancellationToken cancellationToken = default) where T : class;
}
