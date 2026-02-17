namespace EF.CH.QueryProfiling;

/// <summary>
/// Represents query results along with execution statistics.
/// </summary>
/// <typeparam name="T">The type of elements in the result set.</typeparam>
public sealed class QueryResultWithStats<T>
{
    /// <summary>
    /// Gets the query results.
    /// </summary>
    public IReadOnlyList<T> Results { get; init; } = Array.Empty<T>();

    /// <summary>
    /// Gets the execution statistics for the query.
    /// May be null if statistics could not be retrieved.
    /// </summary>
    public QueryStatistics? Statistics { get; init; }

    /// <summary>
    /// Gets the SQL query that was executed.
    /// </summary>
    public string Sql { get; init; } = string.Empty;

    /// <summary>
    /// Gets the total time elapsed for query execution and statistics retrieval.
    /// </summary>
    public TimeSpan Elapsed { get; init; }

    /// <summary>
    /// Gets the number of results returned.
    /// </summary>
    public int Count => Results.Count;
}
