using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse's
/// non-deterministic random-value functions. All entries are
/// non-deterministic — the <see cref="Query.Internal.ClickHouseEvaluatableExpressionFilterPlugin"/>
/// keeps every call as a tree node so the server (not the client) computes
/// a fresh value per row. These are LINQ translation stubs — calling them
/// outside of a query will throw.
/// </summary>
public static class ClickHouseRandomDbFunctionsExtensions
{
    /// <summary>Translates to <c>rand()</c>. Returns a random UInt32.</summary>
    public static uint Rand(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>rand64()</c>. Returns a random UInt64.</summary>
    public static ulong Rand64(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>randCanonical()</c>. Returns a random Float64 in [0, 1).</summary>
    public static double RandCanonical(this DbFunctions _) => throw NotSupported();

    /// <summary>Translates to <c>randomString(length)</c>.</summary>
    public static string RandomString(this DbFunctions _, int length) => throw NotSupported();

    /// <summary>Translates to <c>randomFixedString(length)</c>.</summary>
    public static string RandomFixedString(this DbFunctions _, int length) => throw NotSupported();

    /// <summary>Translates to <c>randomPrintableASCII(length)</c>.</summary>
    public static string RandomPrintableASCII(this DbFunctions _, int length) => throw NotSupported();

    /// <summary>Translates to <c>randUniform(min, max)</c>. Returns a Float64 in [min, max).</summary>
    public static double RandUniform(this DbFunctions _, double min, double max) => throw NotSupported();

    /// <summary>Translates to <c>randNormal(mean, stddev)</c>.</summary>
    public static double RandNormal(this DbFunctions _, double mean, double stddev) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
