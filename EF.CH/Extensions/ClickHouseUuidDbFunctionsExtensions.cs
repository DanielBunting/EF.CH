using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse UUID functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseUuidDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>generateUUIDv7()</c>.
    /// Generates a time-sortable UUID v7 value.
    /// </summary>
    /// <remarks>
    /// .NET's default <see cref="Guid.CompareTo(Guid)"/> does NOT preserve the
    /// time-order of v7 UUIDs because the first three groups are laid out
    /// little-endian, while the v7 timestamp prefix is big-endian. To time-order
    /// client-side, sort with <see cref="UuidV7Comparer.Instance"/>; to
    /// time-order server-side just <c>ORDER BY</c> the column (CH's UUID type
    /// sorts in big-endian byte order).
    /// </remarks>
    public static Guid NewGuidV7(this DbFunctions _)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>generateUUIDv4()</c>.
    /// Generates a random UUID v4 value.
    /// </summary>
    public static Guid NewGuidV4(this DbFunctions _)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>UUIDStringToNum(s)</c>.
    /// Converts a UUID hyphenated string to its 16-byte big-endian binary form.
    /// </summary>
    public static byte[] UUIDStringToNum(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>UUIDNumToString(b)</c>.
    /// Converts a 16-byte UUID binary to its hyphenated string form.
    /// </summary>
    public static string UUIDNumToString(this DbFunctions _, byte[] bytes)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>toUUIDOrNull(s)</c>.
    /// Parses a string as a UUID; returns null when the input is not a valid UUID.
    /// </summary>
    public static Guid? ToUUIDOrNull(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
