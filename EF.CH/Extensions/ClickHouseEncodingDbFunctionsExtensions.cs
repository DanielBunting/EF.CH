using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse encoding/decoding functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseEncodingDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>base64Encode(s)</c>.
    /// Encodes a string as Base64.
    /// </summary>
    public static string Base64Encode(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>base64Decode(s)</c>.
    /// Decodes a Base64-encoded string.
    /// </summary>
    public static string Base64Decode(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hex(value)</c>.
    /// Returns the hexadecimal representation of the value.
    /// </summary>
    public static string Hex<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>unhex(s)</c>.
    /// Converts a hex string back to bytes (returned as a String).
    /// </summary>
    public static string Unhex(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
