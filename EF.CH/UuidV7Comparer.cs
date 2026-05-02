namespace EF.CH;

/// <summary>
/// Compares <see cref="Guid"/> values produced by ClickHouse <c>generateUUIDv7()</c>
/// in true time-order. .NET's default <see cref="Guid.CompareTo(Guid)"/> uses
/// mixed-endian byte layout for the first three integer groups, which loses
/// the big-endian 48-bit timestamp prefix of an RFC 9562 v7 UUID. This
/// comparer reads each Guid's canonical (RFC big-endian) byte sequence and
/// performs a lexicographic byte comparison, so two v7 UUIDs minted
/// milliseconds apart sort in mint order.
/// </summary>
public sealed class UuidV7Comparer : IComparer<Guid>
{
    public static readonly UuidV7Comparer Instance = new();

    public int Compare(Guid x, Guid y)
    {
        Span<byte> a = stackalloc byte[16];
        Span<byte> b = stackalloc byte[16];
        x.TryWriteBytes(a, bigEndian: true, out _);
        y.TryWriteBytes(b, bigEndian: true, out _);
        return a.SequenceCompareTo(b);
    }
}
