using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse full-text search functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseTextSearchDbFunctionsExtensions
{
    // --- Token functions ---

    /// <summary>
    /// Translates to ClickHouse <c>hasToken(haystack, token)</c>.
    /// Checks whether a token appears in the string. A token is a maximal-length substring
    /// between two non-alphanumeric ASCII characters (or boundaries).
    /// </summary>
    public static bool HasToken(this DbFunctions _, string haystack, string token)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hasTokenCaseInsensitive(haystack, token)</c>.
    /// Case-insensitive version of <see cref="HasToken"/>.
    /// </summary>
    public static bool HasTokenCaseInsensitive(this DbFunctions _, string haystack, string token)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hasAnyToken(haystack, tokens)</c>.
    /// Returns true if any of the given tokens appear in the string.
    /// </summary>
    public static bool HasAnyToken(this DbFunctions _, string haystack, string[] tokens)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hasAllTokens(haystack, tokens)</c>.
    /// Returns true if all of the given tokens appear in the string.
    /// </summary>
    public static bool HasAllTokens(this DbFunctions _, string haystack, string[] tokens)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    // --- Multi-search functions ---

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchAny(haystack, [needles])</c>.
    /// Returns 1 if any of the needle substrings appear in the haystack.
    /// </summary>
    public static bool MultiSearchAny(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchAnyCaseInsensitive(haystack, [needles])</c>.
    /// Case-insensitive version of <see cref="MultiSearchAny"/>.
    /// </summary>
    public static bool MultiSearchAnyCaseInsensitive(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchAllPositions(haystack, [needles])</c>.
    /// Returns an array of positions of each needle in the haystack (0 if not found).
    /// </summary>
    public static bool MultiSearchAll(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchAllPositionsCaseInsensitive(haystack, [needles])</c>.
    /// Case-insensitive version of <see cref="MultiSearchAll"/>.
    /// </summary>
    public static bool MultiSearchAllCaseInsensitive(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchFirstPosition(haystack, [needles])</c>.
    /// Returns the leftmost offset of the earliest matching needle in the haystack (0 if none found).
    /// </summary>
    public static ulong MultiSearchFirstPosition(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchFirstPositionCaseInsensitive(haystack, [needles])</c>.
    /// Case-insensitive version of <see cref="MultiSearchFirstPosition"/>.
    /// </summary>
    public static ulong MultiSearchFirstPositionCaseInsensitive(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchFirstIndex(haystack, [needles])</c>.
    /// Returns the 1-based index of the first matching needle (0 if none found).
    /// </summary>
    public static ulong MultiSearchFirstIndex(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiSearchFirstIndexCaseInsensitive(haystack, [needles])</c>.
    /// Case-insensitive version of <see cref="MultiSearchFirstIndex"/>.
    /// </summary>
    public static ulong MultiSearchFirstIndexCaseInsensitive(this DbFunctions _, string haystack, string[] needles)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    // --- N-gram functions ---

    /// <summary>
    /// Translates to ClickHouse <c>ngramSearch(haystack, needle)</c>.
    /// Returns the similarity score (0..1) between the haystack and the needle using 3-gram comparison.
    /// </summary>
    public static float NgramSearch(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramSearchCaseInsensitive(haystack, needle)</c>.
    /// Case-insensitive version of <see cref="NgramSearch"/>.
    /// </summary>
    public static float NgramSearchCaseInsensitive(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramDistance(haystack, needle)</c>.
    /// Returns the distance (0..1) between the haystack and the needle using 3-gram comparison.
    /// </summary>
    public static float NgramDistance(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramDistanceCaseInsensitive(haystack, needle)</c>.
    /// Case-insensitive version of <see cref="NgramDistance"/>.
    /// </summary>
    public static float NgramDistanceCaseInsensitive(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramSearchUTF8(haystack, needle)</c>.
    /// UTF-8 aware version of <see cref="NgramSearch"/>.
    /// </summary>
    public static float NgramSearchUTF8(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramSearchCaseInsensitiveUTF8(haystack, needle)</c>.
    /// UTF-8 aware, case-insensitive version of <see cref="NgramSearch"/>.
    /// </summary>
    public static float NgramSearchCaseInsensitiveUTF8(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramDistanceUTF8(haystack, needle)</c>.
    /// UTF-8 aware version of <see cref="NgramDistance"/>.
    /// </summary>
    public static float NgramDistanceUTF8(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>ngramDistanceCaseInsensitiveUTF8(haystack, needle)</c>.
    /// UTF-8 aware, case-insensitive version of <see cref="NgramDistance"/>.
    /// </summary>
    public static float NgramDistanceCaseInsensitiveUTF8(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    // --- Subsequence functions ---

    /// <summary>
    /// Translates to ClickHouse <c>hasSubsequence(haystack, subsequence)</c>.
    /// Checks whether the subsequence characters appear in the haystack in order (not necessarily contiguous).
    /// </summary>
    public static bool HasSubsequence(this DbFunctions _, string haystack, string subsequence)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hasSubsequenceCaseInsensitive(haystack, subsequence)</c>.
    /// Case-insensitive version of <see cref="HasSubsequence"/>.
    /// </summary>
    public static bool HasSubsequenceCaseInsensitive(this DbFunctions _, string haystack, string subsequence)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    // --- Substring counting ---

    /// <summary>
    /// Translates to ClickHouse <c>countSubstrings(haystack, needle)</c>.
    /// Returns the number of non-overlapping occurrences of the needle in the haystack.
    /// </summary>
    public static ulong CountSubstrings(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>countSubstringsCaseInsensitive(haystack, needle)</c>.
    /// Case-insensitive version of <see cref="CountSubstrings"/>.
    /// </summary>
    public static ulong CountSubstringsCaseInsensitive(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    // --- Multi-match (regex via Hyperscan) ---

    /// <summary>
    /// Translates to ClickHouse <c>multiMatchAny(haystack, [patterns])</c>.
    /// Returns 1 if any of the regex patterns match the haystack. Uses Hyperscan.
    /// </summary>
    public static bool MultiMatchAny(this DbFunctions _, string haystack, string[] patterns)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiMatchAnyIndex(haystack, [patterns])</c>.
    /// Returns the 1-based index of the first matching regex pattern (0 if none match).
    /// </summary>
    public static ulong MultiMatchAnyIndex(this DbFunctions _, string haystack, string[] patterns)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiMatchAllIndices(haystack, [patterns])</c>.
    /// Returns an array of 1-based indices of all matching regex patterns.
    /// </summary>
    public static ulong[] MultiMatchAllIndices(this DbFunctions _, string haystack, string[] patterns)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    // --- Extract functions ---

    /// <summary>
    /// Translates to ClickHouse <c>extractAll(haystack, pattern)</c>.
    /// Extracts all fragments matching a regular expression.
    /// </summary>
    public static string[] ExtractAll(this DbFunctions _, string haystack, string pattern)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>splitByNonAlpha(s)</c>.
    /// Splits the string by non-alphanumeric ASCII characters.
    /// </summary>
    public static string[] SplitByNonAlpha(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
