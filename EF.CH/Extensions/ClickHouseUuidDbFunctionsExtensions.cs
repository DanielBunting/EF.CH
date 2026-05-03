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

    /// <summary>Translates to <c>toUUIDOrZero(s)</c>. Returns <see cref="Guid.Empty"/> on failure.</summary>
    public static Guid ToUUIDOrZero(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Constructs a UUIDv7 whose 48-bit big-endian timestamp prefix matches
    /// <paramref name="dt"/>. The remainder of the UUID (sub-ms / random
    /// bits) is generated client-side via <see cref="Guid.CreateVersion7(DateTimeOffset)"/>;
    /// the resulting <see cref="Guid"/> is bound to the SQL as a parameter,
    /// so the server doesn't need to ship a <c>dateTimeToUUIDv7</c> function
    /// (which only exists as an experimental builder in CH 24.10+, and not
    /// in every stable release including 25.6).
    /// </summary>
    /// <remarks>
    /// Unlike the rest of this class — whose methods are pure translation
    /// stubs and throw if invoked outside a query — this one runs both at
    /// query-translation time (where EF Core evaluates the call into a
    /// constant <see cref="Guid"/>) AND in plain .NET code if you choose to
    /// call it directly.
    /// </remarks>
    public static Guid DateTimeToUUIDv7(this DbFunctions _, DateTime dt)
    {
        var instant = dt.Kind == DateTimeKind.Unspecified
            ? new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc), TimeSpan.Zero)
            : new DateTimeOffset(dt.ToUniversalTime(), TimeSpan.Zero);
        return Guid.CreateVersion7(instant);
    }

    /// <summary>
    /// Translates to <c>UUIDv7ToDateTime(uuid)</c>. Extracts the embedded
    /// 48-bit ms timestamp from a UUIDv7.
    /// </summary>
    public static DateTime UUIDv7ToDateTime(this DbFunctions _, Guid uuid)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
