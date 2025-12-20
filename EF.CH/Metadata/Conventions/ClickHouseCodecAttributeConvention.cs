using System.Reflection;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that discovers <see cref="ClickHouseCodecAttribute"/> on properties
/// and stores the codec specification as an annotation.
/// </summary>
/// <remarks>
/// This convention runs when properties are added to the model. It checks for
/// <see cref="ClickHouseCodecAttribute"/> or any derived attribute (like <see cref="TimestampCodecAttribute"/>)
/// and stores the codec specification as the <see cref="ClickHouseAnnotationNames.CompressionCodec"/> annotation.
/// <para>
/// The annotation is set with <c>fromDataAnnotation: true</c>, so explicit fluent API
/// configuration via <c>HasCodec()</c> will override the attribute value.
/// </para>
/// </remarks>
public class ClickHouseCodecAttributeConvention : IPropertyAddedConvention
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

        var codecAttribute = memberInfo.GetCustomAttribute<ClickHouseCodecAttribute>();
        if (codecAttribute != null)
        {
            propertyBuilder.HasAnnotation(
                ClickHouseAnnotationNames.CompressionCodec,
                codecAttribute.CodecSpec,
                fromDataAnnotation: true);
        }
    }
}
