namespace EF.CH.Infrastructure;

/// <summary>
/// Options for configuring OPTIMIZE TABLE operations.
/// </summary>
public class OptimizeTableOptions
{
    /// <summary>
    /// Gets whether the FINAL modifier should be applied.
    /// FINAL forces a complete merge even if not strictly necessary.
    /// </summary>
    public bool Final { get; private set; }

    /// <summary>
    /// Gets the partition ID to optimize. If null, optimizes the entire table.
    /// </summary>
    public string? Partition { get; private set; }

    /// <summary>
    /// Gets whether deduplication should be performed.
    /// </summary>
    public bool Deduplicate { get; private set; }

    /// <summary>
    /// Gets the columns to deduplicate by. If null or empty, deduplicates by all columns.
    /// </summary>
    public string[]? DeduplicateBy { get; private set; }

    /// <summary>
    /// Applies the FINAL modifier to force a complete merge.
    /// </summary>
    /// <param name="final">Whether to apply FINAL. Defaults to true.</param>
    /// <returns>This options instance for chaining.</returns>
    public OptimizeTableOptions WithFinal(bool final = true)
    {
        Final = final;
        return this;
    }

    /// <summary>
    /// Specifies a partition to optimize instead of the entire table.
    /// </summary>
    /// <param name="partitionId">The partition ID (e.g., "202401" for monthly, "20240115" for daily).</param>
    /// <returns>This options instance for chaining.</returns>
    public OptimizeTableOptions WithPartition(string partitionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(partitionId);
        Partition = partitionId;
        return this;
    }

    /// <summary>
    /// Enables deduplication during optimization.
    /// </summary>
    /// <param name="columns">Optional columns to deduplicate by. If empty, deduplicates by all columns.</param>
    /// <returns>This options instance for chaining.</returns>
    public OptimizeTableOptions WithDeduplicate(params string[] columns)
    {
        Deduplicate = true;
        if (columns.Length > 0)
        {
            DeduplicateBy = columns;
        }
        return this;
    }
}
