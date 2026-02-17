using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse UUID functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseUuidDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>generateUUIDv7()</c>.
    /// Generates a time-sortable UUID v7 value.
    /// </summary>
    public static Guid NewGuidV7(this DbFunctions _)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
