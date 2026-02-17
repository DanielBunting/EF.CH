using System.Diagnostics;

namespace EF.CH.InsertSelect;

/// <summary>
/// Contains the results of an INSERT ... SELECT operation.
/// </summary>
public class ClickHouseInsertSelectResult
{
    /// <summary>
    /// Gets the number of rows affected by the operation.
    /// Note: ClickHouse may not always return accurate row counts for INSERT ... SELECT.
    /// </summary>
    public long RowsAffected { get; init; }

    /// <summary>
    /// Gets the total elapsed time for the operation.
    /// </summary>
    public TimeSpan Elapsed { get; init; }

    /// <summary>
    /// Gets the generated SQL statement (useful for debugging).
    /// </summary>
    public string Sql { get; init; } = string.Empty;

    /// <summary>
    /// Creates an empty result (no rows affected).
    /// </summary>
    public static ClickHouseInsertSelectResult Empty { get; } = new()
    {
        RowsAffected = 0,
        Elapsed = TimeSpan.Zero,
        Sql = string.Empty
    };

    /// <summary>
    /// Creates a result from a stopwatch and row count.
    /// </summary>
    internal static ClickHouseInsertSelectResult Create(Stopwatch stopwatch, long rowsAffected, string sql)
    {
        stopwatch.Stop();
        return new ClickHouseInsertSelectResult
        {
            RowsAffected = rowsAffected,
            Elapsed = stopwatch.Elapsed,
            Sql = sql
        };
    }
}
