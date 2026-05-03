using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse string splitting and joining functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseStringSplitDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>splitByChar(separator, s)</c>.
    /// Splits a string by a single character separator.
    /// </summary>
    public static string[] SplitByChar(this DbFunctions _, string separator, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>splitByString(separator, s)</c>.
    /// Splits a string by a multi-character separator.
    /// </summary>
    public static string[] SplitByString(this DbFunctions _, string separator, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>arrayStringConcat(arr)</c>.
    /// Concatenates array elements into a single string with no separator.
    /// </summary>
    public static string ArrayStringConcat(this DbFunctions _, string[] arr)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>arrayStringConcat(arr, separator)</c>.
    /// Concatenates array elements into a single string with the specified separator.
    /// </summary>
    public static string ArrayStringConcat(this DbFunctions _, string[] arr, string separator)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>splitByRegexp(pattern, s)</c>.
    /// Splits a string by a regular-expression separator.
    /// </summary>
    public static string[] SplitByRegexp(this DbFunctions _, string pattern, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>alphaTokens(s)</c>.
    /// Returns the maximal substrings of consecutive alphabetic characters in
    /// <paramref name="s"/>.
    /// </summary>
    public static string[] AlphaTokens(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
