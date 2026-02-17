using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse IP address functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseIpDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>IPv4NumToString(ip)</c>.
    /// Converts a UInt32 IP address to its dotted-decimal string representation.
    /// </summary>
    public static string IPv4NumToString(this DbFunctions _, uint ip)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>IPv4StringToNum(s)</c>.
    /// Converts a dotted-decimal string to a UInt32 IP address.
    /// </summary>
    public static uint IPv4StringToNum(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isIPAddressInRange(address, cidr)</c>.
    /// Returns true if the IP address is within the specified CIDR range.
    /// </summary>
    public static bool IsIPAddressInRange(this DbFunctions _, string address, string cidr)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isIPv4String(s)</c>.
    /// Returns true if the string is a valid IPv4 address.
    /// </summary>
    public static bool IsIPv4String(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isIPv6String(s)</c>.
    /// Returns true if the string is a valid IPv6 address.
    /// </summary>
    public static bool IsIPv6String(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
