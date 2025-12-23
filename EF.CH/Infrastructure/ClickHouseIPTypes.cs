using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

namespace EF.CH;

/// <summary>
/// Represents a ClickHouse IPv4 address.
/// Wraps System.Net.IPAddress but ensures IPv4 format.
/// </summary>
/// <remarks>
/// ClickHouse stores IPv4 as 4 bytes (UInt32).
/// Use this type for explicit IPv4 columns; for mixed IP storage, use IPAddress with IPv6 column.
/// </remarks>
public readonly struct ClickHouseIPv4 : IEquatable<ClickHouseIPv4>, IComparable<ClickHouseIPv4>
{
    private readonly uint _value;

    /// <summary>
    /// Creates a new ClickHouseIPv4 from the raw 32-bit value.
    /// </summary>
    public ClickHouseIPv4(uint value)
    {
        _value = value;
    }

    /// <summary>
    /// Creates a new ClickHouseIPv4 from an IPAddress.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown if the address is not IPv4.</exception>
    public ClickHouseIPv4(IPAddress address)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
        {
            throw new ArgumentException("Address must be IPv4.", nameof(address));
        }

        var bytes = address.GetAddressBytes();
        _value = (uint)((bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3]);
    }

    /// <summary>
    /// Gets the raw 32-bit value of the IP address.
    /// </summary>
    public uint Value => _value;

    /// <summary>
    /// Converts this IPv4 to a System.Net.IPAddress.
    /// </summary>
    public IPAddress ToIPAddress()
    {
        var bytes = new byte[]
        {
            (byte)((_value >> 24) & 0xFF),
            (byte)((_value >> 16) & 0xFF),
            (byte)((_value >> 8) & 0xFF),
            (byte)(_value & 0xFF)
        };
        return new IPAddress(bytes);
    }

    /// <summary>
    /// Parses an IPv4 address from a string.
    /// </summary>
    public static ClickHouseIPv4 Parse(string s)
    {
        var address = IPAddress.Parse(s);
        if (address.AddressFamily == AddressFamily.InterNetworkV6 && address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }
        return new ClickHouseIPv4(address);
    }

    /// <summary>
    /// Tries to parse an IPv4 address from a string.
    /// </summary>
    public static bool TryParse(string s, [NotNullWhen(true)] out ClickHouseIPv4 result)
    {
        if (IPAddress.TryParse(s, out var address))
        {
            if (address.AddressFamily == AddressFamily.InterNetwork)
            {
                result = new ClickHouseIPv4(address);
                return true;
            }
            if (address.AddressFamily == AddressFamily.InterNetworkV6 && address.IsIPv4MappedToIPv6)
            {
                result = new ClickHouseIPv4(address.MapToIPv4());
                return true;
            }
        }

        result = default;
        return false;
    }

    /// <summary>
    /// Returns the dotted-decimal string representation.
    /// </summary>
    public override string ToString() => ToIPAddress().ToString();

    public bool Equals(ClickHouseIPv4 other) => _value == other._value;
    public override bool Equals(object? obj) => obj is ClickHouseIPv4 other && Equals(other);
    public override int GetHashCode() => _value.GetHashCode();
    public int CompareTo(ClickHouseIPv4 other) => _value.CompareTo(other._value);

    public static bool operator ==(ClickHouseIPv4 left, ClickHouseIPv4 right) => left.Equals(right);
    public static bool operator !=(ClickHouseIPv4 left, ClickHouseIPv4 right) => !left.Equals(right);
    public static bool operator <(ClickHouseIPv4 left, ClickHouseIPv4 right) => left._value < right._value;
    public static bool operator >(ClickHouseIPv4 left, ClickHouseIPv4 right) => left._value > right._value;
    public static bool operator <=(ClickHouseIPv4 left, ClickHouseIPv4 right) => left._value <= right._value;
    public static bool operator >=(ClickHouseIPv4 left, ClickHouseIPv4 right) => left._value >= right._value;

    public static implicit operator ClickHouseIPv4(string s) => Parse(s);
    public static explicit operator IPAddress(ClickHouseIPv4 ip) => ip.ToIPAddress();
}

/// <summary>
/// Represents a ClickHouse IPv6 address.
/// Wraps System.Net.IPAddress but ensures IPv6 format.
/// </summary>
/// <remarks>
/// ClickHouse stores IPv6 as 16 bytes (UInt128) in big-endian format.
/// IPv4 addresses can be represented as IPv4-mapped IPv6 (::ffff:192.168.1.1).
/// </remarks>
public readonly struct ClickHouseIPv6 : IEquatable<ClickHouseIPv6>
{
    private readonly byte[] _bytes;

    /// <summary>
    /// Creates a new ClickHouseIPv6 from the raw 16-byte value.
    /// </summary>
    public ClickHouseIPv6(byte[] bytes)
    {
        if (bytes.Length != 16)
        {
            throw new ArgumentException("IPv6 address must be 16 bytes.", nameof(bytes));
        }
        _bytes = bytes;
    }

    /// <summary>
    /// Creates a new ClickHouseIPv6 from an IPAddress.
    /// IPv4 addresses are mapped to IPv6.
    /// </summary>
    public ClickHouseIPv6(IPAddress address)
    {
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            address = address.MapToIPv6();
        }
        _bytes = address.GetAddressBytes();
    }

    /// <summary>
    /// Gets the raw 16-byte value of the IP address.
    /// </summary>
    public byte[] GetBytes() => (byte[])(_bytes?.Clone() ?? new byte[16]);

    /// <summary>
    /// Converts this IPv6 to a System.Net.IPAddress.
    /// </summary>
    public IPAddress ToIPAddress() => new(_bytes ?? new byte[16]);

    /// <summary>
    /// Parses an IPv6 address from a string.
    /// IPv4 addresses are automatically mapped to IPv6.
    /// </summary>
    public static ClickHouseIPv6 Parse(string s)
    {
        var address = IPAddress.Parse(s);
        return new ClickHouseIPv6(address);
    }

    /// <summary>
    /// Tries to parse an IPv6 address from a string.
    /// </summary>
    public static bool TryParse(string s, [NotNullWhen(true)] out ClickHouseIPv6 result)
    {
        if (IPAddress.TryParse(s, out var address))
        {
            result = new ClickHouseIPv6(address);
            return true;
        }

        result = default;
        return false;
    }

    /// <summary>
    /// Returns the standard IPv6 string representation.
    /// </summary>
    public override string ToString() => ToIPAddress().ToString();

    public bool Equals(ClickHouseIPv6 other)
    {
        if (_bytes is null && other._bytes is null) return true;
        if (_bytes is null || other._bytes is null) return false;
        return _bytes.SequenceEqual(other._bytes);
    }

    public override bool Equals(object? obj) => obj is ClickHouseIPv6 other && Equals(other);

    public override int GetHashCode()
    {
        if (_bytes is null) return 0;
        var hash = new HashCode();
        foreach (var b in _bytes)
        {
            hash.Add(b);
        }
        return hash.ToHashCode();
    }

    public static bool operator ==(ClickHouseIPv6 left, ClickHouseIPv6 right) => left.Equals(right);
    public static bool operator !=(ClickHouseIPv6 left, ClickHouseIPv6 right) => !left.Equals(right);

    public static implicit operator ClickHouseIPv6(string s) => Parse(s);
    public static explicit operator IPAddress(ClickHouseIPv6 ip) => ip.ToIPAddress();
}
