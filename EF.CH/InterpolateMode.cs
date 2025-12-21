namespace EF.CH;

/// <summary>
/// Specifies how non-ORDER BY columns are filled when using WITH FILL.
/// </summary>
public enum InterpolateMode
{
    /// <summary>
    /// Use the default value for the column type (0, empty string, NULL, etc.).
    /// This is the default behavior when no INTERPOLATE is specified.
    /// </summary>
    Default,

    /// <summary>
    /// Forward-fill: use the value from the previous non-filled row.
    /// Translates to: INTERPOLATE (column AS column)
    /// </summary>
    Prev
}
