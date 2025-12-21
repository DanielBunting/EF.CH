using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that applies value converters to typed JSON properties.
/// </summary>
/// <remarks>
/// This convention runs during model finalization. It finds properties with the
/// <see cref="ClickHouseAnnotationNames.JsonTypedMapping"/> annotation and applies
/// the appropriate JSON value converter for POCO serialization.
/// </remarks>
public class ClickHouseJsonValueConverterConvention : IModelFinalizingConvention
{
    /// <inheritdoc />
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                // Check if property has typed JSON mapping annotation
                var typedMappingAnnotation = property.FindAnnotation(ClickHouseAnnotationNames.JsonTypedMapping);
                if (typedMappingAnnotation?.Value is not Type pocoType)
                    continue;

                // Skip if property already has a value converter configured
                if (property.GetValueConverter() != null)
                    continue;

                // Create and apply the JSON value converter
                var converter = ClickHouseJsonValueConverterFactory.Create(pocoType);
                if (converter != null)
                {
                    property.Builder.HasConversion(converter, fromDataAnnotation: true);
                }
            }
        }
    }
}
