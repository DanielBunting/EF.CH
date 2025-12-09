using System.Data.Common;
using ClickHouse.Driver.ADO.Parameters;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal.TypeMappings;

/// <summary>
/// Base type mapping for ClickHouse types.
/// </summary>
public class ClickHouseTypeMapping : RelationalTypeMapping
{
    /// <summary>
    /// Creates a new ClickHouse type mapping.
    /// </summary>
    /// <param name="storeType">The ClickHouse store type name (e.g., "Int32", "String").</param>
    /// <param name="clrType">The CLR type this mapping represents.</param>
    /// <param name="dbType">Optional DbType for ADO.NET compatibility.</param>
    public ClickHouseTypeMapping(
        string storeType,
        Type clrType,
        System.Data.DbType? dbType = null)
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(clrType),
            storeType,
            StoreTypePostfix.None,
            dbType))
    {
    }

    /// <summary>
    /// Creates a new ClickHouse type mapping from existing parameters.
    /// </summary>
    protected ClickHouseTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    /// <summary>
    /// Creates a copy of this mapping with the specified parameters.
    /// </summary>
    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseTypeMapping(parameters);

    /// <summary>
    /// Configures a DbParameter with ClickHouse-specific settings.
    /// Sets the ClickHouseType property required for parameter substitution.
    /// </summary>
    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        // Set the ClickHouse type string for parameter substitution
        if (parameter is ClickHouseDbParameter clickHouseParam)
        {
            clickHouseParam.ClickHouseType = StoreType;
        }
    }
}

/// <summary>
/// Type mapping for ClickHouse boolean type.
/// </summary>
public class ClickHouseBoolTypeMapping : ClickHouseTypeMapping
{
    public ClickHouseBoolTypeMapping()
        : base("Bool", typeof(bool), System.Data.DbType.Boolean)
    {
    }

    protected ClickHouseBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseBoolTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => (bool)value ? "true" : "false";
}

/// <summary>
/// Type mapping for ClickHouse UUID type.
/// </summary>
public class ClickHouseGuidTypeMapping : ClickHouseTypeMapping
{
    public ClickHouseGuidTypeMapping()
        : base("UUID", typeof(Guid), System.Data.DbType.Guid)
    {
    }

    protected ClickHouseGuidTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ClickHouseGuidTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"'{(Guid)value:D}'";
}
