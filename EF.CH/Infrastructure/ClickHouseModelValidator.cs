using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Infrastructure;

/// <summary>
/// Validates models for ClickHouse-specific requirements.
/// </summary>
public class ClickHouseModelValidator : RelationalModelValidator
{
    public ClickHouseModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        base.Validate(model, logger);
        ValidateNoIdentityColumns(model);
    }

    /// <summary>
    /// Validates that no properties are configured to use database-generated identity columns.
    /// ClickHouse doesn't support auto-increment/identity columns.
    /// </summary>
    private static void ValidateNoIdentityColumns(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            // Skip owned types and views
            if (entityType.IsOwned() || entityType.GetViewName() != null)
                continue;

            foreach (var property in entityType.GetProperties())
            {
                // Check if property expects database-generated value
                if (property.ValueGenerated.HasFlag(ValueGenerated.OnAdd))
                {
                    // Guid is OK - EF Core generates these client-side
                    var clrType = property.ClrType;
                    if (clrType == typeof(Guid) || clrType == typeof(Guid?))
                        continue;

                    // If there's a SQL default value expression, that's OK (database can generate the value)
                    // Note: GetDefaultValue() returns CLR default (0, false, etc.) even when not explicitly set,
                    // so we only check for SQL defaults here
                    if (!string.IsNullOrEmpty(property.GetDefaultValueSql()))
                        continue;

                    // If there's an explicit non-CLR-default value set, that's also OK
                    var defaultValue = property.GetDefaultValue();
                    if (defaultValue != null && !IsClrDefault(defaultValue, clrType))
                        continue;

                    // If there's a custom value generator factory, that's OK
                    if (property.GetValueGeneratorFactory() != null)
                        continue;

                    // If ValueGeneratedNever was explicitly set, skip (shouldn't have OnAdd flag anyway)
                    // Check if it's a numeric type that would typically expect identity
                    if (IsIntegerType(clrType))
                    {
                        var tableName = entityType.GetTableName() ?? entityType.Name;
                        var columnName = property.GetColumnName() ?? property.Name;

                        throw ClickHouseUnsupportedOperationException.Identity(tableName, columnName);
                    }
                }
            }
        }
    }

    private static bool IsIntegerType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        return underlyingType == typeof(int)
            || underlyingType == typeof(long)
            || underlyingType == typeof(short)
            || underlyingType == typeof(byte)
            || underlyingType == typeof(uint)
            || underlyingType == typeof(ulong)
            || underlyingType == typeof(ushort)
            || underlyingType == typeof(sbyte);
    }

    private static bool IsClrDefault(object value, Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        if (!underlyingType.IsValueType)
            return value == null;

        // Get the CLR default for the type (0 for numbers, false for bool, etc.)
        var defaultValue = Activator.CreateInstance(underlyingType);
        return Equals(value, defaultValue);
    }
}
