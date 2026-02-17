using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse string distance/similarity functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseStringDistanceDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>levenshteinDistance(s1, s2)</c>.
    /// Returns the minimum number of single-character edits to transform one string into the other.
    /// </summary>
    public static ulong LevenshteinDistance(this DbFunctions _, string s1, string s2)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>levenshteinDistanceUTF8(s1, s2)</c>.
    /// UTF-8 aware version of <see cref="LevenshteinDistance"/>.
    /// </summary>
    public static ulong LevenshteinDistanceUTF8(this DbFunctions _, string s1, string s2)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>damerauLevenshteinDistance(s1, s2)</c>.
    /// Like Levenshtein but also allows transpositions of adjacent characters.
    /// </summary>
    public static ulong DamerauLevenshteinDistance(this DbFunctions _, string s1, string s2)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>jaroSimilarity(s1, s2)</c>.
    /// Returns the Jaro similarity (0..1) between two strings.
    /// </summary>
    public static double JaroSimilarity(this DbFunctions _, string s1, string s2)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>jaroWinklerSimilarity(s1, s2)</c>.
    /// Returns the Jaro-Winkler similarity (0..1) between two strings.
    /// </summary>
    public static double JaroWinklerSimilarity(this DbFunctions _, string s1, string s2)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
