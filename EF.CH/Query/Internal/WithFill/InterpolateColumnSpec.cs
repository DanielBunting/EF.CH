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
    public InterpolateMode Mode { get; set; } = InterpolateMode.Default;

    /// <summary>
    /// Raw mode value that may be a DeferredParameter (for EF Core 9+ compatibility).
    /// If set, takes precedence over Mode during resolution.
    /// </summary>
    public object? ModeValue { get; set; }

    /// <summary>
    /// Constant value for constant-fill mode.
    /// May be a DeferredParameter (for EF Core 9+ compatibility).
    /// </summary>
    public object? ConstantValue { get; set; }

    /// <summary>
    /// Whether this uses a constant value fill.
    /// </summary>
    public bool IsConstant { get; init; }
}
