using System.Reflection;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that discovers <see cref="ClickHouseTimeZoneAttribute"/> on DateTimeOffset properties
/// and stores the timezone as an annotation.
/// </summary>
/// <remarks>
/// This convention runs when properties are added to the model. It checks for
/// <see cref="ClickHouseTimeZoneAttribute"/> on <see cref="DateTimeOffset"/> properties
/// and stores the timezone as the <see cref="ClickHouseAnnotationNames.TimeZone"/> annotation.
/// <para>
/// The annotation is set with <c>fromDataAnnotation: true</c>, so explicit fluent API
/// configuration via <c>HasTimeZone()</c> will override the attribute value.
/// </para>
/// </remarks>
public class ClickHouseTimeZoneAttributeConvention : IPropertyAddedConvention
{
    /// <inheritdoc />
    public void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        IConventionContext<IConventionPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;
        var clrType = property.ClrType;
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        // Only process DateTimeOffset properties
        if (underlyingType != typeof(DateTimeOffset))
            return;

        var memberInfo = property.PropertyInfo ?? (MemberInfo?)property.FieldInfo;
        if (memberInfo == null)
            return;

        var timeZoneAttribute = memberInfo.GetCustomAttribute<ClickHouseTimeZoneAttribute>();
        if (timeZoneAttribute != null)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.TimeZone,
                timeZoneAttribute.TimeZone,
                fromDataAnnotation: true);
        }
    }
}

/// <summary>
/// Convention that applies column types to DateTimeOffset properties based on their timezone annotation.
/// </summary>
/// <remarks>
/// <para>
/// This convention runs during model finalization. It finds DateTimeOffset properties with the
/// <see cref="ClickHouseAnnotationNames.TimeZone"/> annotation and sets the appropriate
/// column type (e.g., "DateTime64(3, 'America/New_York')").
/// </para>
/// <para>
/// This two-phase approach (attribute discovery + column type application) is necessary because
/// <see cref="Microsoft.EntityFrameworkCore.Storage.RelationalTypeMappingInfo"/> doesn't have
/// access to property metadata during type mapping resolution.
/// </para>
/// </remarks>
public class ClickHouseTimeZoneColumnTypeConvention : IModelFinalizingConvention
{
    /// <inheritdoc />
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetDeclaredProperties())
            {
                var clrType = property.ClrType;
                var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

                // Only process DateTimeOffset properties
                if (underlyingType != typeof(DateTimeOffset))
                    continue;

                // Check for timezone annotation
                var timeZoneAnnotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);
                if (timeZoneAnnotation?.Value is not string timeZone)
                    continue;

                // Skip if column type is already explicitly set
                if (property.GetColumnType() != null)
                    continue;

                // Set the column type with timezone
                var isNullable = property.IsNullable;
                var columnType = isNullable
                    ? $"Nullable(DateTime64(3, '{timeZone}'))"
                    : $"DateTime64(3, '{timeZone}')";

                property.Builder.HasColumnType(columnType);
            }
        }
    }
}
