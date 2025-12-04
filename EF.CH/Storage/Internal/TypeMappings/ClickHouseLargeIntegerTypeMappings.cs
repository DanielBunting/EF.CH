using System.Globalization;
using System.Numerics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Int128 type.
/// Maps to .NET Int128 (available in .NET 7+).
/// </summary>
public class ClickHouseInt128TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseInt128TypeMapping()
        : base("Int128", typeof(Int128))
    {
    }

    protected ClickHouseInt128TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt128TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((Int128)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse UInt128 type.
/// Maps to .NET UInt128 (available in .NET 7+).
/// </summary>
public class ClickHouseUInt128TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseUInt128TypeMapping()
        : base("UInt128", typeof(UInt128))
    {
    }

    protected ClickHouseUInt128TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt128TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((UInt128)value).ToString(CultureInfo.InvariantCulture);
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
