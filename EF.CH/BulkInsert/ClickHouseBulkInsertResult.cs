using System.Diagnostics;

namespace EF.CH.BulkInsert;

/// <summary>
/// Contains the results of a bulk insert operation.
/// </summary>
public class ClickHouseBulkInsertResult
{
    /// <summary>
    /// Gets the total number of rows inserted.
    /// </summary>
    public long RowsInserted { get; init; }

    /// <summary>
    /// Gets the number of batches executed.
    /// </summary>
    public int BatchesExecuted { get; init; }

    /// <summary>
    /// Gets the total elapsed time for the operation.
    /// </summary>
    public TimeSpan Elapsed { get; init; }

    /// <summary>
    /// Gets the rows per second throughput.
    /// </summary>
    public double RowsPerSecond => Elapsed.TotalSeconds > 0
        ? RowsInserted / Elapsed.TotalSeconds
        : 0;

    /// <summary>
    /// Creates an empty result (no rows inserted).
    /// </summary>
    public static ClickHouseBulkInsertResult Empty { get; } = new()
    {
        RowsInserted = 0,
        BatchesExecuted = 0,
        Elapsed = TimeSpan.Zero
    };

    /// <summary>
    /// Creates a result from a stopwatch and counts.
    /// </summary>
    internal static ClickHouseBulkInsertResult Create(Stopwatch stopwatch, long rowsInserted, int batchesExecuted)
    {
        stopwatch.Stop();
        return new ClickHouseBulkInsertResult
        {
            RowsInserted = rowsInserted,
            BatchesExecuted = batchesExecuted,
            Elapsed = stopwatch.Elapsed
        };
    }
}
