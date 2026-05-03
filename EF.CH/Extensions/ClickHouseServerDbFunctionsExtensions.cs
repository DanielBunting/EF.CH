using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse server /
/// session metadata functions. These are LINQ translation stubs — calling
/// them outside of a query will throw.
/// </summary>
public static class ClickHouseServerDbFunctionsExtensions
{
    /// <summary>Translates to <c>version()</c>. Returns the server version string.</summary>
    public static string Version(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>hostName()</c>. Returns the host name on which the query is executing.</summary>
    public static string HostName(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>currentDatabase()</c>.</summary>
    public static string CurrentDatabase(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>currentUser()</c>.</summary>
    public static string CurrentUser(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>serverTimezone()</c>.</summary>
    public static string ServerTimezone(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>serverUUID()</c>.</summary>
    public static Guid ServerUUID(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>uptime()</c>. Returns the server uptime in seconds.</summary>
    public static uint Uptime(this DbFunctions _) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
