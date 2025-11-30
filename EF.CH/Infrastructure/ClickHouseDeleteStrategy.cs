namespace EF.CH.Infrastructure;

/// <summary>
/// Specifies the DELETE strategy to use for ClickHouse operations.
/// </summary>
public enum ClickHouseDeleteStrategy
{
    /// <summary>
    /// Uses lightweight DELETE FROM syntax (default).
    /// Marks rows as deleted immediately; physical deletion during background merges.
    /// Returns affected row count. Recommended for normal operations.
    /// </summary>
    Lightweight,

    /// <summary>
    /// Uses ALTER TABLE DELETE mutation syntax.
    /// Asynchronous operation that rewrites data parts. Does not return affected row count.
    /// Recommended for bulk maintenance operations only.
    /// </summary>
    Mutation
}
