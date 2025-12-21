namespace EF.CH.Query.Internal.WithFill;

/// <summary>
/// Specification for a single ORDER BY column's WITH FILL configuration.
/// </summary>
internal sealed class WithFillColumnSpec
{
    /// <summary>
    /// The column name to match against ORDER BY columns.
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// Start of fill range. Null means use minimum value from data.
    /// </summary>
    public object? From { get; init; }

    /// <summary>
    /// End of fill range. Null means use maximum value from data.
    /// </summary>
    public object? To { get; init; }

    /// <summary>
    /// Step interval. Can be numeric, TimeSpan, or ClickHouseInterval.
    /// </summary>
    public object? Step { get; init; }

    /// <summary>
    /// Maximum gap size before stopping fill (ClickHouse 25.3+).
    /// </summary>
    public object? Staleness { get; init; }
}
