using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse null-handling functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseNullDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>ifNull(value, default)</c>.
    /// Returns <paramref name="defaultValue"/> if <paramref name="value"/> is NULL, otherwise returns <paramref name="value"/>.
    /// </summary>
    public static T IfNull<T>(this DbFunctions _, T? value, T defaultValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>nullIf(value, compareValue)</c>.
    /// Returns NULL if <paramref name="value"/> equals <paramref name="compareValue"/>, otherwise returns <paramref name="value"/>.
    /// </summary>
    public static T? NullIf<T>(this DbFunctions _, T value, T compareValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>assumeNotNull(value)</c>.
    /// Treats a Nullable column as non-Nullable, with undefined behavior if the value is actually NULL.
    /// </summary>
    public static T AssumeNotNull<T>(this DbFunctions _, T? value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>coalesce(a, b)</c>.
    /// Returns the first non-NULL argument.
    /// </summary>
    public static T Coalesce<T>(this DbFunctions _, T? a, T? b)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>coalesce(a, b, c)</c>.
    /// Returns the first non-NULL argument.
    /// </summary>
    public static T Coalesce<T>(this DbFunctions _, T? a, T? b, T? c)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isNull(value)</c>.
    /// Returns true if <paramref name="value"/> is NULL.
    /// </summary>
    public static bool IsNull<T>(this DbFunctions _, T? value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>isNotNull(value)</c>.
    /// Returns true if <paramref name="value"/> is not NULL.
    /// </summary>
    public static bool IsNotNull<T>(this DbFunctions _, T? value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
