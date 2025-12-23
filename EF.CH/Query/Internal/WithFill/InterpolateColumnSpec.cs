using EF.CH.Extensions;

namespace EF.CH.Query.Internal.WithFill;

/// <summary>
/// Specification for how a non-ORDER BY column is interpolated during fill.
/// </summary>
internal sealed class InterpolateColumnSpec
{
    /// <summary>
    /// The column name to interpolate.
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// Interpolation mode (Prev, Default).
    /// </summary>
    public InterpolateMode Mode { get; init; } = InterpolateMode.Default;

    /// <summary>
    /// Constant value for constant-fill mode.
    /// </summary>
    public object? ConstantValue { get; init; }

    /// <summary>
    /// Whether this uses a constant value fill.
    /// </summary>
    public bool IsConstant { get; init; }
}
