using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse string pattern matching
/// (LIKE / ILIKE / NOT LIKE, regex match and replace, position lookup).
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseStringPatternDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>ilike(text, pattern)</c>. Case-insensitive LIKE.
    /// Returns true when <paramref name="text"/> matches <paramref name="pattern"/>
    /// using SQL LIKE wildcards (<c>%</c>, <c>_</c>) ignoring case.
    /// </summary>
    public static bool ILike(this DbFunctions _, string text, string pattern)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>notLike(text, pattern)</c>. The negation of LIKE.
    /// </summary>
    public static bool NotLike(this DbFunctions _, string text, string pattern)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>notILike(text, pattern)</c>. The negation of ILIKE.
    /// </summary>
    public static bool NotILike(this DbFunctions _, string text, string pattern)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>match(text, pattern)</c>. Tests whether
    /// <paramref name="text"/> matches the re2 regular expression
    /// <paramref name="pattern"/>. Anchors (<c>^</c>, <c>$</c>) are not implicit.
    /// </summary>
    public static bool Match(this DbFunctions _, string text, string pattern)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>replaceRegexpOne(text, pattern, replacement)</c>.
    /// Replaces the first occurrence of the re2 <paramref name="pattern"/> in
    /// <paramref name="text"/> with <paramref name="replacement"/>.
    /// </summary>
    public static string ReplaceRegex(this DbFunctions _, string text, string pattern, string replacement)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>replaceRegexpAll(text, pattern, replacement)</c>.
    /// Replaces every occurrence of the re2 <paramref name="pattern"/> in
    /// <paramref name="text"/> with <paramref name="replacement"/>.
    /// </summary>
    public static string ReplaceRegexAll(this DbFunctions _, string text, string pattern, string replacement)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>extractAllGroupsHorizontal(text, pattern)</c>.
    /// Returns one inner array per capturing group; each inner array contains
    /// every match of that group across <paramref name="text"/>.
    /// </summary>
    public static string[][] MatchAll(this DbFunctions _, string text, string pattern)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>position(haystack, needle)</c>. Returns the
    /// 1-based index of the first occurrence of <paramref name="needle"/> in
    /// <paramref name="haystack"/>, or 0 if not found.
    /// </summary>
    public static int Position(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>positionCaseInsensitiveUTF8(haystack, needle)</c>.
    /// Case-insensitive 1-based position lookup.
    /// </summary>
    public static int PositionCaseInsensitive(this DbFunctions _, string haystack, string needle)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
