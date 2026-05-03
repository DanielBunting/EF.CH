using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse math
/// functions that don't have a clean .NET <see cref="Math"/> equivalent —
/// constants, angle conversion, special functions, factorial, banker's
/// rounding, histogram bucketing, hypotenuse. These are LINQ translation
/// stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseMathDbFunctionsExtensions
{
    /// <summary>Translates to <c>pi()</c>.</summary>
    public static double Pi(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>e()</c>. Euler's number.</summary>
    public static double E(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>degrees(rad)</c>.</summary>
    public static double Degrees(this DbFunctions _, double rad) => throw NotSupported();

    /// <summary>Translates to <c>radians(deg)</c>.</summary>
    public static double Radians(this DbFunctions _, double deg) => throw NotSupported();

    /// <summary>Translates to <c>factorial(n)</c>.</summary>
    public static long Factorial(this DbFunctions _, int n) => throw NotSupported();

    /// <summary>Translates to <c>erf(x)</c> — the error function.</summary>
    public static double Erf(this DbFunctions _, double x) => throw NotSupported();

    /// <summary>Translates to <c>erfc(x)</c> — the complementary error function.</summary>
    public static double Erfc(this DbFunctions _, double x) => throw NotSupported();

    /// <summary>Translates to <c>lgamma(x)</c> — log-gamma.</summary>
    public static double Lgamma(this DbFunctions _, double x) => throw NotSupported();

    /// <summary>Translates to <c>tgamma(x)</c> — gamma.</summary>
    public static double Tgamma(this DbFunctions _, double x) => throw NotSupported();

    /// <summary>Translates to <c>roundBankers(x, n)</c> — banker's rounding (round-half-to-even).</summary>
    public static double RoundBankers(this DbFunctions _, double x, int n) => throw NotSupported();

    /// <summary>
    /// Translates to <c>widthBucket(value, low, high, count)</c>. Returns
    /// the 1-based histogram bucket for <paramref name="value"/> over
    /// <paramref name="count"/> equal-width buckets between
    /// <paramref name="low"/> and <paramref name="high"/>.
    /// </summary>
    public static int WidthBucket(this DbFunctions _, double value, double low, double high, int count) => throw NotSupported();

    /// <summary>Translates to <c>hypot(x, y)</c>. Hypotenuse without overflow / underflow.</summary>
    public static double Hypot(this DbFunctions _, double x, double y) => throw NotSupported();

    /// <summary>Translates to <c>log1p(x)</c> — accurate <c>log(1 + x)</c> near zero.</summary>
    public static double Log1P(this DbFunctions _, double x) => throw NotSupported();

    /// <summary>Translates to <c>sigmoid(x)</c>.</summary>
    public static double Sigmoid(this DbFunctions _, double x) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
