namespace EF.CH.Query.Internal.Expressions;

/// <summary>
/// Specifies the type of window frame (ROWS or RANGE).
/// </summary>
public enum WindowFrameType
{
    /// <summary>
    /// Frame is defined in terms of physical rows.
    /// </summary>
    Rows,

    /// <summary>
    /// Frame is defined in terms of logical value ranges.
    /// </summary>
    Range
}

/// <summary>
/// Specifies the boundary of a window frame.
/// </summary>
public enum WindowFrameBound
{
    /// <summary>
    /// UNBOUNDED PRECEDING - all rows from the start of the partition.
    /// </summary>
    UnboundedPreceding,

    /// <summary>
    /// N PRECEDING - N rows before the current row.
    /// </summary>
    Preceding,

    /// <summary>
    /// CURRENT ROW - the current row.
    /// </summary>
    CurrentRow,

    /// <summary>
    /// N FOLLOWING - N rows after the current row.
    /// </summary>
    Following,

    /// <summary>
    /// UNBOUNDED FOLLOWING - all rows to the end of the partition.
    /// </summary>
    UnboundedFollowing
}

/// <summary>
/// Represents a window frame specification for ClickHouse window functions.
/// </summary>
/// <param name="Type">The frame type (ROWS or RANGE).</param>
/// <param name="StartBound">The start boundary of the frame.</param>
/// <param name="StartOffset">The offset for PRECEDING/FOLLOWING start bounds.</param>
/// <param name="EndBound">The end boundary of the frame.</param>
/// <param name="EndOffset">The offset for PRECEDING/FOLLOWING end bounds.</param>
public sealed record WindowFrame(
    WindowFrameType Type,
    WindowFrameBound StartBound,
    int? StartOffset,
    WindowFrameBound EndBound,
    int? EndOffset)
{
    /// <summary>
    /// Creates a frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
    /// This is the default frame when ORDER BY is specified.
    /// </summary>
    public static WindowFrame RowsUnboundedPrecedingToCurrentRow => new(
        WindowFrameType.Rows,
        WindowFrameBound.UnboundedPreceding,
        null,
        WindowFrameBound.CurrentRow,
        null);

    /// <summary>
    /// Creates a frame: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
    /// This frame includes all rows in the partition.
    /// Required for lagInFrame/leadInFrame to work correctly.
    /// </summary>
    public static WindowFrame RowsUnboundedPrecedingToUnboundedFollowing => new(
        WindowFrameType.Rows,
        WindowFrameBound.UnboundedPreceding,
        null,
        WindowFrameBound.UnboundedFollowing,
        null);
}
