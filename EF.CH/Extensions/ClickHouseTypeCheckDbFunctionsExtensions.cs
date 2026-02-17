using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse type-checking functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseTypeCheckDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>isNaN(value)</c>.
    /// Returns true if the value is Not-a-Number.
    /// </summary>
    public static bool IsNaN(this DbFunctions _, double value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isFinite(value)</c>.
    /// Returns true if the value is not infinite and not NaN.
    /// </summary>
    public static bool IsFinite(this DbFunctions _, double value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isInfinite(value)</c>.
    /// Returns true if the value is infinite.
    /// </summary>
    public static bool IsInfinite(this DbFunctions _, double value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
