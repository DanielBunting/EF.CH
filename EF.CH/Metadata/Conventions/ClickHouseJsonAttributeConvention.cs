using System.Reflection;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that discovers <see cref="ClickHouseJsonAttribute"/> on properties
/// and stores the JSON configuration as annotations.
/// </summary>
/// <remarks>
/// This convention runs when properties are added to the model. It checks for
/// <see cref="ClickHouseJsonAttribute"/> and stores the configuration as annotations.
/// <para>
/// The annotations are set with <c>fromDataAnnotation: true</c>, so explicit fluent API
/// configuration will override the attribute values.
/// </para>
/// </remarks>
public class ClickHouseJsonAttributeConvention : IPropertyAddedConvention
{
    /// <inheritdoc />
    public void ProcessPropertyAdded(
        IConventionPropertyBuilder propertyBuilder,
        IConventionContext<IConventionPropertyBuilder> context)
    {
        var property = propertyBuilder.Metadata;
        var memberInfo = property.PropertyInfo ?? (MemberInfo?)property.FieldInfo;

        if (memberInfo == null)
            return;

        var jsonAttribute = memberInfo.GetCustomAttribute<ClickHouseJsonAttribute>();
        if (jsonAttribute == null)
            return;

        // Apply max_dynamic_paths if specified
        if (jsonAttribute.MaxDynamicPaths >= 0)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.JsonMaxDynamicPaths,
                jsonAttribute.MaxDynamicPaths,
                fromDataAnnotation: true);
        }

        // Apply max_dynamic_types if specified
        if (jsonAttribute.MaxDynamicTypes >= 0)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.JsonMaxDynamicTypes,
                jsonAttribute.MaxDynamicTypes,
                fromDataAnnotation: true);
        }

        // Apply typed mapping if specified
        if (jsonAttribute.IsTyped)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.JsonTypedMapping,
                property.ClrType,
                fromDataAnnotation: true);
        }
    }
}
