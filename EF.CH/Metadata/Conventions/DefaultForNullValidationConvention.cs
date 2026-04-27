using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that validates properties carrying both a <c>HasDefaultForNull</c>
/// annotation and a <see cref="ValueGenerated"/> mode. The combination is
/// internally inconsistent — value-generated columns should not have a
/// null-default applied — and silently produces wrong query results.
/// Throws at model finalisation so the conflict is caught before any query
/// runs.
/// </summary>
internal sealed class DefaultForNullValidationConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                var hasDefaultForNull =
                    property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull) != null;
                if (!hasDefaultForNull)
                    continue;

                var hasValueGenerated = property.ValueGenerated != ValueGenerated.Never;
                if (!hasValueGenerated)
                    continue;

                throw new InvalidOperationException(
                    $"Property '{entityType.DisplayName()}.{property.Name}' has both " +
                    $"HasDefaultForNull and a value-generation strategy " +
                    $"(ValueGenerated.{property.ValueGenerated}). These conflict because " +
                    $"value-generated columns should not have a null-default — drop one " +
                    $"of them.");
            }
        }
    }
}
