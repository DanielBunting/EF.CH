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
        ValidateRefreshableMaterializedViews(model);
    }

    /// <summary>
    /// Cross-validates the refreshable-MV annotations: schedule must be present
    /// for any optional flag, POPULATE/REFRESH are mutually exclusive,
    /// APPEND/EMPTY are mutually exclusive, and DEPENDS ON entries must resolve
    /// to known entities.
    /// </summary>
    private static void ValidateRefreshableMaterializedViews(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            if (entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedView)?.Value as bool? != true)
                continue;

            var hasInterval = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshInterval) != null;

            if (!hasInterval)
            {
                // Non-refreshable MV: ensure refresh-only options aren't accidentally set.
                foreach (var ann in new[]
                {
                    ClickHouseAnnotationNames.MaterializedViewRefreshOffset,
                    ClickHouseAnnotationNames.MaterializedViewRefreshRandomizeFor,
                    ClickHouseAnnotationNames.MaterializedViewRefreshDependsOn,
                    ClickHouseAnnotationNames.MaterializedViewRefreshAppend,
                    ClickHouseAnnotationNames.MaterializedViewRefreshEmpty,
                    ClickHouseAnnotationNames.MaterializedViewRefreshSettings,
                    ClickHouseAnnotationNames.MaterializedViewRefreshTarget,
                })
                {
                    if (entityType.FindAnnotation(ann) != null)
                    {
                        var name = entityType.GetTableName() ?? entityType.Name;
                        throw new InvalidOperationException(
                            $"Materialized view '{name}' has refresh option '{ann}' set without a refresh schedule. " +
                            $"Call Every(...) or After(...) on the refresh builder.");
                    }
                }
                continue;
            }

            var name2 = entityType.GetTableName() ?? entityType.Name;

            var populate = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate)?.Value as bool? ?? false;
            if (populate)
            {
                throw new InvalidOperationException(
                    $"Refreshable materialized view '{name2}' cannot also use POPULATE. " +
                    $"Use the EMPTY option (skip initial refresh) instead.");
            }

            var append = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshAppend)?.Value as bool? ?? false;
            var empty = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshEmpty)?.Value as bool? ?? false;
            if (append && empty)
            {
                throw new InvalidOperationException(
                    $"Refreshable materialized view '{name2}' has both APPEND and EMPTY set; these are mutually exclusive.");
            }

            var dependsOn = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewRefreshDependsOn)?.Value as string[];
            if (dependsOn is { Length: > 0 })
            {
                foreach (var dep in dependsOn)
                {
                    var found = model.GetEntityTypes().Any(e =>
                        string.Equals(e.GetTableName(), dep, StringComparison.Ordinal) ||
                        string.Equals(e.ShortName(), dep, StringComparison.Ordinal) ||
                        string.Equals(e.Name, dep, StringComparison.Ordinal));
                    if (!found)
                    {
                        throw new InvalidOperationException(
                            $"Refreshable materialized view '{name2}' depends on unknown entity '{dep}'. " +
                            $"Add the entity to the model or use a fully qualified table name.");
                    }
                }
            }
        }
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

                    // ALIAS columns are computed on read (HasAliasExpression sets
                    // ValueGeneratedOnAddOrUpdate, so the OnAdd flag fires the check
                    // for integer types). The expression supplies the value — there's
                    // no identity behaviour involved.
                    if (property.FindAnnotation(ClickHouseAnnotationNames.AliasExpression) != null)
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
