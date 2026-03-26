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
    /// May be a DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    public object? From { get; set; }

    /// <summary>
    /// End of fill range. Null means use maximum value from data.
    /// May be a DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    public object? To { get; set; }

    /// <summary>
    /// Step interval. Can be numeric, TimeSpan, ClickHouseInterval, or DeferredParameter.
    /// </summary>
    public object? Step { get; set; }

    /// <summary>
    /// Maximum gap size before stopping fill (ClickHouse 25.3+).
    /// May be a DeferredParameter (resolved via ResolveDeferredParameters).
    /// </summary>
    public object? Staleness { get; set; }
}
