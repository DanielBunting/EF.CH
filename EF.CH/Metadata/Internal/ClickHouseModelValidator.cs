using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Metadata.Internal;

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
        ValidateComputedColumnExclusivity(model);
    }

    /// <summary>
    /// Enforces that a property uses at most one of the ClickHouse column
    /// modifiers (MATERIALIZED / ALIAS / DEFAULT / EPHEMERAL) and that
    /// EPHEMERAL columns don't carry a compression codec.
    /// </summary>
    private static void ValidateComputedColumnExclusivity(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.IsOwned() || entityType.GetViewName() != null)
                continue;

            foreach (var property in entityType.GetProperties())
            {
                var hasMaterialized = property.FindAnnotation(ClickHouseAnnotationNames.MaterializedExpression) != null;
                var hasAlias = property.FindAnnotation(ClickHouseAnnotationNames.AliasExpression) != null;
                var hasDefault = property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression) != null;
                var hasEphemeral = property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression) != null;

                var modeCount = (hasMaterialized ? 1 : 0) + (hasAlias ? 1 : 0)
                              + (hasDefault ? 1 : 0) + (hasEphemeral ? 1 : 0);

                if (modeCount > 1)
                {
                    var tableName = entityType.GetTableName() ?? entityType.Name;
                    var columnName = property.GetColumnName() ?? property.Name;
                    throw new InvalidOperationException(
                        $"Property '{columnName}' on '{tableName}' has multiple ClickHouse column " +
                        $"modifiers configured (MATERIALIZED / ALIAS / DEFAULT / EPHEMERAL). " +
                        $"These are mutually exclusive — pick one.");
                }

                if (hasEphemeral && property.FindAnnotation(ClickHouseAnnotationNames.CompressionCodec) != null)
                {
                    var tableName = entityType.GetTableName() ?? entityType.Name;
                    var columnName = property.GetColumnName() ?? property.Name;
                    throw new InvalidOperationException(
                        $"Property '{columnName}' on '{tableName}' is EPHEMERAL and cannot have " +
                        $"a compression codec — ephemeral columns have no storage.");
                }
            }
        }
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

                    // If this is a MATERIALIZED column, it's expected to have ValueGeneratedOnAdd
                    // since ClickHouse computes the value on INSERT
                    if (property.FindAnnotation(ClickHouseAnnotationNames.MaterializedExpression) != null)
                        continue;

                    // If this is a DEFAULT expression column, it has a ClickHouse default
                    if (property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression) != null)
                        continue;

                    // EPHEMERAL columns have ValueGenerated.Never — the identity-column
                    // check shouldn't fire for them, but guard defensively in case a
                    // downstream convention reintroduces OnAdd.
                    if (property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression) != null)
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
