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
/// <see cref="ClickHouseTimeZoneAttribute"/> on DateTimeOffset properties and stores the
/// timezone as the <see cref="ClickHouseAnnotationNames.TimeZone"/> annotation.
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

        // Only applies to DateTimeOffset properties
        var clrType = property.ClrType;
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;
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
/// Convention that applies the timezone annotation to DateTimeOffset column types during model finalization.
/// </summary>
/// <remarks>
/// This convention runs during model finalization to set the column type for DateTimeOffset properties
/// that have a <see cref="ClickHouseAnnotationNames.TimeZone"/> annotation (set via attribute or fluent API).
/// <para>
/// Setting the column type to <c>DateTime64(3, 'timezone')</c> allows the type mapping source to
/// create the correct timezone-aware mapping without needing direct property access.
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
                // Only applies to DateTimeOffset properties
                var clrType = property.ClrType;
                var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;
                if (underlyingType != typeof(DateTimeOffset))
                    continue;

                // Check if a timezone annotation is set
                var timeZoneAnnotation = property.FindAnnotation(ClickHouseAnnotationNames.TimeZone);
                if (timeZoneAnnotation?.Value is not string timeZone)
                    continue;

                // Skip if the column type is already explicitly set
                var existingColumnType = property.GetColumnType();
                if (existingColumnType != null)
                    continue;

                // Set the column type with the timezone
                var isNullable = property.IsNullable;
                var columnType = isNullable
                    ? $"Nullable(DateTime64(3, '{timeZone}'))"
                    : $"DateTime64(3, '{timeZone}')";

                property.Builder.HasColumnType(columnType);
            }
        }
    }
}
