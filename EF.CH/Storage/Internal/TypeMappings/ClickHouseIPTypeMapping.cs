using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse IPv4 type.
/// Stored as 4 bytes (UInt32) internally.
/// </summary>
/// <remarks>
/// ClickHouse IPv4 accepts string format: '192.168.1.1'
/// Literals are passed as toIPv4('address') for explicit typing.
/// </remarks>
public class ClickHouseIPv4TypeMapping : RelationalTypeMapping
{
    public ClickHouseIPv4TypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(ClickHouseIPv4),
                converter: new ClickHouseIPv4Converter()),
            "IPv4",
            StoreTypePostfix.None,
            System.Data.DbType.String))
    {
    }

    protected ClickHouseIPv4TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseIPv4TypeMapping(parameters);

    /// <summary>
    /// Generates a SQL literal for an IPv4 value.
    /// Uses toIPv4() function for explicit typing.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var ip = value switch
        {
            ClickHouseIPv4 chIp => chIp.ToString(),
            string s => s,
            _ => value.ToString()
        };
        return $"toIPv4('{ip}')";
    }
}

/// <summary>
/// Type mapping for ClickHouse IPv6 type.
/// Stored as 16 bytes (UInt128) in big-endian format.
/// </summary>
/// <remarks>
/// ClickHouse IPv6 accepts standard notation: '2001:db8::1'
/// Literals are passed as toIPv6('address') for explicit typing.
/// </remarks>
public class ClickHouseIPv6TypeMapping : RelationalTypeMapping
{
    public ClickHouseIPv6TypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(ClickHouseIPv6),
                converter: new ClickHouseIPv6Converter()),
            "IPv6",
            StoreTypePostfix.None,
            System.Data.DbType.String))
    {
    }

    protected ClickHouseIPv6TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseIPv6TypeMapping(parameters);

    /// <summary>
    /// Generates a SQL literal for an IPv6 value.
    /// Uses toIPv6() function for explicit typing.
    /// </summary>
    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var ip = value switch
        {
            ClickHouseIPv6 chIp => chIp.ToString(),
            string s => s,
            _ => value.ToString()
        };
        return $"toIPv6('{ip}')";
    }
}

/// <summary>
/// Type mapping for System.Net.IPAddress (maps to IPv6 for maximum compatibility).
/// </summary>
public class ClickHouseIPAddressTypeMapping : RelationalTypeMapping
{
    public ClickHouseIPAddressTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(IPAddress),
                converter: new IPAddressToStringConverter()),
            "IPv6",
            StoreTypePostfix.None,
            System.Data.DbType.String))
    {
    }

    protected ClickHouseIPAddressTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseIPAddressTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var ip = value switch
        {
            IPAddress addr => addr.ToString(),
            string s => s,
            _ => value.ToString()
        };
        return $"toIPv6('{ip}')";
    }
}

/// <summary>
/// Value converter for ClickHouseIPv4 to/from string.
/// </summary>
internal class ClickHouseIPv4Converter : ValueConverter<ClickHouseIPv4, string>
{
    public ClickHouseIPv4Converter()
        : base(
            ip => ip.ToString(),
            s => ClickHouseIPv4.Parse(s))
    {
    }
}

/// <summary>
/// Value converter for ClickHouseIPv6 to/from string.
/// </summary>
internal class ClickHouseIPv6Converter : ValueConverter<ClickHouseIPv6, string>
{
    public ClickHouseIPv6Converter()
        : base(
            ip => ip.ToString(),
            s => ClickHouseIPv6.Parse(s))
    {
    }
}

/// <summary>
/// Value converter for IPAddress to/from string.
/// </summary>
internal class IPAddressToStringConverter : ValueConverter<IPAddress, string>
{
    public IPAddressToStringConverter()
        : base(
            ip => ip.ToString(),
            s => IPAddress.Parse(s))
    {
    }
}
