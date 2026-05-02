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

    /// <summary>
    /// Returns the resolved root path for the given Keeper-relative
    /// <paramref name="path"/>, applying any configured ZooKeeper base path
    /// macros. Errors when no Keeper is configured for the server.
    /// </summary>
    public static string ZooKeeperPath(this DbFunctions _, string path)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Returns true when the connected ClickHouse server has a ZooKeeper /
    /// Keeper configuration available — used as a guard inside MV definitions
    /// or queries that depend on Keeper-backed features.
    /// </summary>
    public static bool HasZooKeeperConfig(this DbFunctions _)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
