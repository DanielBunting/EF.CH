using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Type mapping for ClickHouse signed 8-bit integer (Int8 / sbyte).
/// </summary>
public class ClickHouseInt8TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseInt8TypeMapping()
        : base("Int8", typeof(sbyte), System.Data.DbType.SByte)
    {
    }

    protected ClickHouseInt8TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt8TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((sbyte)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse unsigned 8-bit integer (UInt8 / byte).
/// </summary>
public class ClickHouseUInt8TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseUInt8TypeMapping()
        : base("UInt8", typeof(byte), System.Data.DbType.Byte)
    {
    }

    protected ClickHouseUInt8TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt8TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((byte)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse signed 16-bit integer (Int16 / short).
/// </summary>
public class ClickHouseInt16TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseInt16TypeMapping()
        : base("Int16", typeof(short), System.Data.DbType.Int16)
    {
    }

    protected ClickHouseInt16TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt16TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((short)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse unsigned 16-bit integer (UInt16 / ushort).
/// </summary>
public class ClickHouseUInt16TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseUInt16TypeMapping()
        : base("UInt16", typeof(ushort), System.Data.DbType.UInt16)
    {
    }

    protected ClickHouseUInt16TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt16TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((ushort)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse signed 32-bit integer (Int32 / int).
/// </summary>
public class ClickHouseInt32TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseInt32TypeMapping()
        : base("Int32", typeof(int), System.Data.DbType.Int32)
    {
    }

    protected ClickHouseInt32TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt32TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((int)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse unsigned 32-bit integer (UInt32 / uint).
/// </summary>
public class ClickHouseUInt32TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseUInt32TypeMapping()
        : base("UInt32", typeof(uint), System.Data.DbType.UInt32)
    {
    }

    protected ClickHouseUInt32TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt32TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((uint)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse signed 64-bit integer (Int64 / long).
/// </summary>
public class ClickHouseInt64TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseInt64TypeMapping()
        : base("Int64", typeof(long), System.Data.DbType.Int64)
    {
    }

    protected ClickHouseInt64TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseInt64TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((long)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse unsigned 64-bit integer (UInt64 / ulong).
/// </summary>
public class ClickHouseUInt64TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseUInt64TypeMapping()
        : base("UInt64", typeof(ulong), System.Data.DbType.UInt64)
    {
    }

    protected ClickHouseUInt64TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseUInt64TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => ((ulong)value).ToString(CultureInfo.InvariantCulture);
}

/// <summary>
/// Type mapping for ClickHouse 32-bit float (Float32 / float).
/// </summary>
public class ClickHouseFloat32TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseFloat32TypeMapping()
        : base("Float32", typeof(float), System.Data.DbType.Single)
    {
    }

    protected ClickHouseFloat32TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseFloat32TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var floatValue = (float)value;

        if (float.IsNaN(floatValue))
            return "nan";
        if (float.IsPositiveInfinity(floatValue))
            return "inf";
        if (float.IsNegativeInfinity(floatValue))
            return "-inf";

        return floatValue.ToString("G9", CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// Type mapping for ClickHouse 64-bit float (Float64 / double).
/// </summary>
public class ClickHouseFloat64TypeMapping : ClickHouseTypeMapping
{
    public ClickHouseFloat64TypeMapping()
        : base("Float64", typeof(double), System.Data.DbType.Double)
    {
    }

    protected ClickHouseFloat64TypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseFloat64TypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var doubleValue = (double)value;

        if (double.IsNaN(doubleValue))
            return "nan";
        if (double.IsPositiveInfinity(doubleValue))
            return "inf";
        if (double.IsNegativeInfinity(doubleValue))
            return "-inf";

        return doubleValue.ToString("G17", CultureInfo.InvariantCulture);
    }
}
