using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse string
/// operations that don't have direct .NET BCL equivalents — left/right
/// substring extraction, padding, repetition, reversal, title-casing,
/// space generation. UTF8-aware variants are used wherever the BCL
/// counterpart would produce different results on multi-byte characters.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseStringExtraDbFunctionsExtensions
{
    /// <summary>Translates to <c>leftUTF8(s, n)</c>.</summary>
    public static string Left(this DbFunctions _, string s, int n) => throw NotSupported();

    /// <summary>Translates to <c>rightUTF8(s, n)</c>.</summary>
    public static string Right(this DbFunctions _, string s, int n) => throw NotSupported();

    /// <summary>Translates to <c>leftPad(s, length, pad)</c>.</summary>
    public static string LeftPad(this DbFunctions _, string s, int length, string pad) => throw NotSupported();

    /// <summary>Translates to <c>rightPad(s, length, pad)</c>.</summary>
    public static string RightPad(this DbFunctions _, string s, int length, string pad) => throw NotSupported();

    /// <summary>Translates to <c>repeat(s, n)</c>.</summary>
    public static string Repeat(this DbFunctions _, string s, int n) => throw NotSupported();

    /// <summary>Translates to <c>reverseUTF8(s)</c>.</summary>
    public static string Reverse(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>initcapUTF8(s)</c> — title-cases each whitespace-delimited word.</summary>
    public static string InitCap(this DbFunctions _, string s) => throw NotSupported();

    /// <summary>Translates to <c>space(n)</c> — returns a string of <paramref name="n"/> spaces.</summary>
    public static string Space(this DbFunctions _, int n) => throw NotSupported();

    /// <summary>Translates to <c>concatWithSeparator(separator, parts...)</c>.</summary>
    public static string ConcatWithSeparator(this DbFunctions _, string separator, params string[] parts) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
