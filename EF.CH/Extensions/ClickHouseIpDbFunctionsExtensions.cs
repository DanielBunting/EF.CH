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

    /// <summary>Translates to <c>IPv6StringToNum(s)</c>.</summary>
    public static byte[] IPv6StringToNum(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>Translates to <c>IPv6NumToString(b)</c>.</summary>
    public static string IPv6NumToString(this DbFunctions _, byte[] b)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>Translates to <c>IPv4CIDRToRange(ip, prefix)</c>.</summary>
    public static object IPv4CIDRToRange(this DbFunctions _, uint ip, byte prefix)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>Translates to <c>IPv6CIDRToRange(ip, prefix)</c>.</summary>
    public static object IPv6CIDRToRange(this DbFunctions _, byte[] ip, byte prefix)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>Translates to <c>IPv4ToIPv6(ip)</c>.</summary>
    public static byte[] IPv4ToIPv6(this DbFunctions _, uint ip)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to <c>cutIPv6(ip, bytesToCutForIPv6, bytesToCutForIPv4)</c>.
    /// Truncates the trailing bytes of an IPv6 address for log anonymisation.
    /// </summary>
    public static string CutIPv6(this DbFunctions _, byte[] ip, int bytesToCutForIPv6, int bytesToCutForIPv4)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>Translates to <c>toIPv4(s)</c>.</summary>
    public static uint ToIPv4(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>Translates to <c>toIPv6(s)</c>.</summary>
    public static byte[] ToIPv6(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
