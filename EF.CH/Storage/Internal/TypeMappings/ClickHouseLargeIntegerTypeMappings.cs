using System.Globalization;
using System.Numerics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Int128 type.
/// Maps to .NET Int128 (available in .NET 7+). The driver materialises Int128 columns as
/// BigInteger, so a converter bridges Int128 ↔ BigInteger.
/// </summary>
public class ClickHouseInt128TypeMapping : ClickHouseTypeMapping
{
    private static readonly Int128BigIntegerConverter BigIntegerConverter = new();

    public ClickHouseInt128TypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(Int128), BigIntegerConverter),
            "Int128"))
    {
    }

    protected ClickHouseInt128TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt128TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => value switch
        {
            Int128 i => i.ToString(CultureInfo.InvariantCulture),
            BigInteger b => b.ToString(CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException($"Unexpected value type for Int128: {value.GetType()}")
        };
}

/// <summary>
/// Type mapping for ClickHouse UInt128 type.
/// Maps to .NET UInt128 (available in .NET 7+). The driver materialises UInt128 columns as
/// BigInteger, so a converter bridges UInt128 ↔ BigInteger.
/// </summary>
public class ClickHouseUInt128TypeMapping : ClickHouseTypeMapping
{
    private static readonly UInt128BigIntegerConverter BigIntegerConverter = new();

    public ClickHouseUInt128TypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(UInt128), BigIntegerConverter),
            "UInt128"))
    {
    }

    protected ClickHouseUInt128TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt128TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => value switch
        {
            UInt128 u => u.ToString(CultureInfo.InvariantCulture),
            BigInteger b => b.ToString(CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException($"Unexpected value type for UInt128: {value.GetType()}")
        };
}

internal class Int128BigIntegerConverter : ValueConverter<Int128, BigInteger>
{
    public Int128BigIntegerConverter()
        : base(v => (BigInteger)v, v => (Int128)v)
    {
    }
}

internal class UInt128BigIntegerConverter : ValueConverter<UInt128, BigInteger>
{
    public UInt128BigIntegerConverter()
        : base(v => (BigInteger)v, v => (UInt128)v)
    {
    }
}

/// <summary>
/// Type mapping for ClickHouse Int256 type.
/// Maps to .NET BigInteger since there's no native 256-bit integer type.
/// </summary>
/// <remarks>
/// BigInteger can represent arbitrary precision integers, but when used with Int256
/// it should be constrained to values within the Int256 range:
/// Min: -2^255
/// Max: 2^255 - 1
/// </remarks>
public class ClickHouseInt256TypeMapping : ClickHouseTypeMapping
{
    /// <summary>
    /// Minimum value for Int256: -2^255
    /// </summary>
    public static readonly BigInteger MinValue = BigInteger.MinusOne << 255;

    /// <summary>
    /// Maximum value for Int256: 2^255 - 1
    /// </summary>
    public static readonly BigInteger MaxValue = (BigInteger.One << 255) - 1;

    public ClickHouseInt256TypeMapping()
        : base("Int256", typeof(BigInteger))
    {
    }

    protected ClickHouseInt256TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt256TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var bigInt = (BigInteger)value;
        return bigInt.ToString(CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// Type mapping for ClickHouse UInt256 type.
/// Maps to .NET BigInteger since there's no native 256-bit unsigned integer type.
/// </summary>
/// <remarks>
/// BigInteger can represent arbitrary precision integers, but when used with UInt256
/// it should be constrained to values within the UInt256 range:
/// Min: 0
/// Max: 2^256 - 1
/// </remarks>
public class ClickHouseUInt256TypeMapping : ClickHouseTypeMapping
{
    /// <summary>
    /// Maximum value for UInt256: 2^256 - 1
    /// </summary>
    public static readonly BigInteger MaxValue = (BigInteger.One << 256) - 1;

    public ClickHouseUInt256TypeMapping()
        : base("UInt256", typeof(BigInteger))
    {
    }

    protected ClickHouseUInt256TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt256TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var bigInt = (BigInteger)value;
        if (bigInt.Sign < 0)
        {
            throw new ArgumentException("UInt256 cannot represent negative values", nameof(value));
        }
        return bigInt.ToString(CultureInfo.InvariantCulture);
    }
}
