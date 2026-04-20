using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse Keeper-backed
/// scalar functions. These are LINQ translation stubs — calling them outside
/// of a query will throw.
/// </summary>
public static class ClickHouseKeeperDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>generateSerialID(counter_name)</c>.
    /// Atomically increments a Keeper-persisted counter and returns a
    /// monotonically increasing UInt64. Requires a ClickHouse cluster with
    /// Keeper configured.
    /// </summary>
    public static ulong GenerateSerialID(this DbFunctions _, string counterName)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
