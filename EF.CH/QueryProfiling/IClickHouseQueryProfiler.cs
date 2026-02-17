namespace EF.CH.QueryProfiling;

/// <summary>
/// Service interface for ClickHouse query profiling operations.
/// Provides EXPLAIN queries and query statistics retrieval.
/// </summary>
public interface IClickHouseQueryProfiler
{
    /// <summary>
    /// Executes an EXPLAIN query for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="configure">Optional action to configure explain options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN result containing query plan information.</returns>
    Task<ExplainResult> ExplainAsync<T>(
        IQueryable<T> query,
        Action<ExplainOptions>? configure = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes an EXPLAIN query for raw SQL.
    /// </summary>
    /// <param name="sql">The SQL query to explain.</param>
    /// <param name="configure">Optional action to configure explain options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN result containing query plan information.</returns>
    Task<ExplainResult> ExplainSqlAsync(
        string sql,
        Action<ExplainOptions>? configure = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a query and returns results along with execution statistics.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to execute.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Query results with execution statistics.</returns>
    /// <remarks>
    /// Statistics are retrieved from system.query_log after query execution.
    /// Due to ClickHouse's asynchronous logging, statistics may not be immediately
    /// available and retrieval is best-effort.
    /// </remarks>
    Task<QueryResultWithStats<T>> ToListWithStatsAsync<T>(
        IQueryable<T> query,
        CancellationToken cancellationToken = default);
}
