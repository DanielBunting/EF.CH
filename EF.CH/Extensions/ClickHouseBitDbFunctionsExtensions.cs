using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse bit-manipulation functions.
/// C# bitwise operators (<c>&amp;</c>, <c>|</c>, <c>^</c>, <c>~</c>, <c>&lt;&lt;</c>, <c>&gt;&gt;</c>) on
/// integer columns already lower to <c>bitAnd</c> / <c>bitOr</c> / <c>bitXor</c> /
/// <c>bitNot</c> / <c>bitShiftLeft</c> / <c>bitShiftRight</c> via EF Core's
/// standard translation. The methods here expose the explicit named surface
/// plus rotates, population count, individual-bit testing, slicing and
/// Hamming distance — none of which have a C#-operator equivalent.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseBitDbFunctionsExtensions
{
    /// <summary>Translates to <c>bitAnd(a, b)</c>.</summary>
    public static long BitAnd(this DbFunctions _, long a, long b) => throw NotSupported();
    /// <summary>Translates to <c>bitOr(a, b)</c>.</summary>
    public static long BitOr(this DbFunctions _, long a, long b) => throw NotSupported();
    /// <summary>Translates to <c>bitXor(a, b)</c>.</summary>
    public static long BitXor(this DbFunctions _, long a, long b) => throw NotSupported();
    /// <summary>Translates to <c>bitNot(a)</c>.</summary>
    public static long BitNot(this DbFunctions _, long a) => throw NotSupported();

    /// <summary>Translates to <c>bitShiftLeft(a, n)</c>.</summary>
    public static long BitShiftLeft(this DbFunctions _, long a, int n) => throw NotSupported();
    /// <summary>Translates to <c>bitShiftRight(a, n)</c>.</summary>
    public static long BitShiftRight(this DbFunctions _, long a, int n) => throw NotSupported();

    /// <summary>Translates to <c>bitRotateLeft(a, n)</c>.</summary>
    public static long BitRotateLeft(this DbFunctions _, long a, int n) => throw NotSupported();
    /// <summary>Translates to <c>bitRotateRight(a, n)</c>.</summary>
    public static long BitRotateRight(this DbFunctions _, long a, int n) => throw NotSupported();

    /// <summary>
    /// Translates to <c>bitCount(a)</c>. Returns the number of bits set to 1
    /// in <paramref name="a"/> (population count / Hamming weight).
    /// </summary>
    public static long BitCount(this DbFunctions _, long a) => throw NotSupported();

    /// <summary>
    /// Translates to <c>bitTest(a, position)</c>. Returns the bit at
    /// <paramref name="position"/> (0-based, LSB first) as 0 or 1.
    /// </summary>
    public static byte BitTest(this DbFunctions _, long a, int position) => throw NotSupported();

    /// <summary>Translates to <c>bitTestAll(a, positions...)</c>. True iff every named bit is set.</summary>
    public static bool BitTestAll(this DbFunctions _, long a, params int[] positions) => throw NotSupported();

    /// <summary>Translates to <c>bitTestAny(a, positions...)</c>. True iff any named bit is set.</summary>
    public static bool BitTestAny(this DbFunctions _, long a, params int[] positions) => throw NotSupported();

    /// <summary>
    /// Translates to <c>bitSlice(a, offset, length)</c>. Extracts
    /// <paramref name="length"/> bits starting at <paramref name="offset"/>.
    /// </summary>
    public static string BitSlice(this DbFunctions _, long a, int offset, int length) => throw NotSupported();

    /// <summary>
    /// Translates to <c>bitHammingDistance(a, b)</c>. Returns the count of
    /// differing bits between <paramref name="a"/> and <paramref name="b"/>.
    /// </summary>
    public static long BitHammingDistance(this DbFunctions _, long a, long b) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
