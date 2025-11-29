using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse Decimal types.
/// ClickHouse supports Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S).
/// </summary>
public class ClickHouseDecimalTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// Total number of digits (precision).
    /// </summary>
    public int DecimalPrecision { get; }

    /// <summary>
    /// Number of digits after the decimal point (scale).
    /// </summary>
    public int DecimalScale { get; }

    /// <summary>
    /// Creates a Decimal type mapping with the specified precision and scale.
    /// </summary>
    /// <param name="precision">Total number of digits (1-76).</param>
    /// <param name="scale">Number of decimal places (0 to precision).</param>
    public ClickHouseDecimalTypeMapping(int precision = 18, int scale = 4)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(decimal)),
            $"Decimal({precision}, {scale})",
            StoreTypePostfix.None,
            System.Data.DbType.Decimal,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: precision,
            scale: scale))
    {
        DecimalPrecision = precision;
        DecimalScale = scale;
    }

    protected ClickHouseDecimalTypeMapping(
        RelationalTypeMappingParameters parameters,
        int precision,
        int scale)
        : base(parameters)
    {
        DecimalPrecision = precision;
        DecimalScale = scale;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDecimalTypeMapping(parameters, DecimalPrecision, DecimalScale);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var decimalValue = (decimal)value;
        return decimalValue.ToString(CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// Type mapping for ClickHouse Decimal32(S) - uses Int32 storage.
/// Precision 1-9 digits.
/// </summary>
public class ClickHouseDecimal32TypeMapping : RelationalTypeMapping
{
    public int DecimalScale { get; }

    public ClickHouseDecimal32TypeMapping(int scale = 4)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(decimal)),
            $"Decimal32({scale})",
            StoreTypePostfix.None,
            System.Data.DbType.Decimal,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: 9,
            scale: scale))
    {
        DecimalScale = scale;
    }

    protected ClickHouseDecimal32TypeMapping(RelationalTypeMappingParameters parameters, int scale)
        : base(parameters)
    {
        DecimalScale = scale;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDecimal32TypeMapping(parameters, DecimalScale);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((decimal)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse Decimal64(S) - uses Int64 storage.
/// Precision 1-18 digits.
/// </summary>
public class ClickHouseDecimal64TypeMapping : RelationalTypeMapping
{
    public int DecimalScale { get; }

    public ClickHouseDecimal64TypeMapping(int scale = 4)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(decimal)),
            $"Decimal64({scale})",
            StoreTypePostfix.None,
            System.Data.DbType.Decimal,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: 18,
            scale: scale))
    {
        DecimalScale = scale;
    }

    protected ClickHouseDecimal64TypeMapping(RelationalTypeMappingParameters parameters, int scale)
        : base(parameters)
    {
        DecimalScale = scale;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDecimal64TypeMapping(parameters, DecimalScale);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((decimal)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse Decimal128(S) - uses Int128 storage.
/// Precision 1-38 digits.
/// </summary>
public class ClickHouseDecimal128TypeMapping : RelationalTypeMapping
{
    public int DecimalScale { get; }

    public ClickHouseDecimal128TypeMapping(int scale = 4)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(decimal)),
            $"Decimal128({scale})",
            StoreTypePostfix.None,
            System.Data.DbType.Decimal,
            unicode: false,
            size: null,
            fixedLength: false,
            precision: 38,
            scale: scale))
    {
        DecimalScale = scale;
    }

    protected ClickHouseDecimal128TypeMapping(RelationalTypeMappingParameters parameters, int scale)
        : base(parameters)
    {
        DecimalScale = scale;
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseDecimal128TypeMapping(parameters, DecimalScale);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((decimal)value).ToString(CultureInfo.InvariantCulture);
}
