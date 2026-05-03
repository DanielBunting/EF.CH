using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse conditional functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
/// <remarks>
/// <para>
/// EF Core already lowers C# ternary chains (<c>a ? b : c</c>) and switch
/// expressions to <c>CASE WHEN … END</c>, which ClickHouse parses correctly,
/// so a dedicated single-condition <c>If(cond, then, else)</c> would just
/// duplicate the standard surface. <see cref="MultiIf"/> is the value-add:
/// it emits ClickHouse's compact <c>multiIf(c1, v1, c2, v2, …, default)</c>
/// form, which the server can short-circuit and which reads more naturally
/// than nested ternaries for many-branch conditionals.
/// </para>
/// </remarks>
public static class ClickHouseConditionalDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>multiIf(c1, v1, defaultValue)</c>.
    /// </summary>
    public static T MultiIf<T>(this DbFunctions _, bool c1, T v1, T defaultValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiIf(c1, v1, c2, v2, defaultValue)</c>.
    /// </summary>
    public static T MultiIf<T>(this DbFunctions _, bool c1, T v1, bool c2, T v2, T defaultValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiIf(c1, v1, c2, v2, c3, v3, defaultValue)</c>.
    /// </summary>
    public static T MultiIf<T>(this DbFunctions _, bool c1, T v1, bool c2, T v2, bool c3, T v3, T defaultValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiIf(c1, v1, c2, v2, c3, v3, c4, v4, defaultValue)</c>.
    /// </summary>
    public static T MultiIf<T>(this DbFunctions _, bool c1, T v1, bool c2, T v2, bool c3, T v3, bool c4, T v4, T defaultValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>multiIf(c1, v1, c2, v2, c3, v3, c4, v4, c5, v5, defaultValue)</c>.
    /// </summary>
    public static T MultiIf<T>(this DbFunctions _, bool c1, T v1, bool c2, T v2, bool c3, T v3, bool c4, T v4, bool c5, T v5, T defaultValue)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
