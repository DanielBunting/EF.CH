using EF.CH;
using Xunit;

namespace EF.CH.Tests.Functions;

public class UuidV7ComparerTests
{
    [Fact]
    public void Compare_OrdersByTimestampPrefix()
    {
        var earlier = MakeV7(timestampMs: 0x000001234567UL, tail: 0xAABBCCDDEEFF1122UL);
        var later   = MakeV7(timestampMs: 0x000001234568UL, tail: 0x0011223344556677UL);

        Assert.True(UuidV7Comparer.Instance.Compare(earlier, later) < 0);
        Assert.True(UuidV7Comparer.Instance.Compare(later, earlier) > 0);
        Assert.Equal(0, UuidV7Comparer.Instance.Compare(earlier, earlier));
    }

    [Fact]
    public void Compare_TimePrefixCarry_OrdersCorrectly_WhereDefaultGuidCompareToFails()
    {
        // Pick a timestamp pair where the byte ordering matters most: a low
        // byte rolls over (...00FF → ...0100). With .NET's mixed-endian
        // Guid.CompareTo (Data1 first, little-endian-int compared as int32),
        // these can sort in the wrong order. UuidV7Comparer must always pick
        // the right order regardless of where the carry lands.
        var earlier = MakeV7(timestampMs: 0x0000_0000_00FFUL, tail: 0);
        var later   = MakeV7(timestampMs: 0x0000_0000_0100UL, tail: 0);

        Assert.True(UuidV7Comparer.Instance.Compare(earlier, later) < 0);

        var arr = new[] { later, earlier };
        Array.Sort(arr, UuidV7Comparer.Instance);
        Assert.Equal(earlier, arr[0]);
        Assert.Equal(later, arr[1]);
    }

    /// <summary>
    /// Build a synthetic v7 Guid with a chosen 48-bit timestamp and 64-bit tail.
    /// Bytes are laid out per RFC 9562 (big-endian time prefix in bytes 0..5,
    /// version nibble in byte 6, variant nibble in byte 8) and constructed via
    /// the modern <see cref="Guid(System.ReadOnlySpan{byte}, bool)"/> overload.
    /// </summary>
    private static Guid MakeV7(ulong timestampMs, ulong tail)
    {
        Span<byte> rfc = stackalloc byte[16];
        rfc[0] = (byte)(timestampMs >> 40);
        rfc[1] = (byte)(timestampMs >> 32);
        rfc[2] = (byte)(timestampMs >> 24);
        rfc[3] = (byte)(timestampMs >> 16);
        rfc[4] = (byte)(timestampMs >> 8);
        rfc[5] = (byte)timestampMs;
        rfc[6] = (byte)(0x70 | ((tail >> 56) & 0x0F));
        rfc[7] = (byte)(tail >> 48);
        rfc[8] = (byte)(0x80 | ((tail >> 40) & 0x3F));
        rfc[9]  = (byte)(tail >> 32);
        rfc[10] = (byte)(tail >> 24);
        rfc[11] = (byte)(tail >> 16);
        rfc[12] = (byte)(tail >> 8);
        rfc[13] = (byte)tail;
        rfc[14] = 0xAB;
        rfc[15] = 0xCD;
        return new Guid(rfc, bigEndian: true);
    }
}
