using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse's typed
/// <c>JSONExtract*</c> functions and JSON introspection helpers. Use these
/// when you have a String/JSON column whose typed access shape is known at
/// query time and want to avoid the row-shaped <c>JSON</c> column type.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseJsonExtractDbFunctionsExtensions
{
    /// <summary>Translates to <c>JSONExtractInt(json, path)</c>.</summary>
    public static long JSONExtractInt(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONExtractUInt(json, path)</c>.</summary>
    public static ulong JSONExtractUInt(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONExtractFloat(json, path)</c>.</summary>
    public static double JSONExtractFloat(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONExtractBool(json, path)</c>.</summary>
    public static bool JSONExtractBool(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONExtractString(json, path)</c>.</summary>
    public static string JSONExtractString(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONExtractRaw(json, path)</c>.</summary>
    public static string JSONExtractRaw(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONHas(json, path)</c>.</summary>
    public static bool JSONHas(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONLength(json, path)</c>. Returns the array length / object key count.</summary>
    public static long JSONLength(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>JSONType(json, path)</c>. Returns <c>'String'</c>, <c>'Int64'</c>, etc.</summary>
    public static string JSONType(this DbFunctions _, string json, string path) => throw NotSupported();

    /// <summary>Translates to <c>isValidJSON(s)</c>.</summary>
    public static bool IsValidJSON(this DbFunctions _, string s) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
