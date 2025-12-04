using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Convention that makes all entities keyless by default for ClickHouse.
/// This is appropriate for append-only tables where no change tracking is needed.
/// Enable via UseClickHouse(options => options.UseKeylessEntitiesByDefault()).
/// </summary>
public class ClickHouseKeylessConvention : IEntityTypeAddedConvention
{
    /// <summary>
    /// Called when an entity type is added to the model.
    /// </summary>
    public void ProcessEntityTypeAdded(
        IConventionEntityTypeBuilder entityTypeBuilder,
        IConventionContext<IConventionEntityTypeBuilder> context)
    {
        // Make entity keyless by default
        // Users can override with HasKey() in OnModelCreating
        entityTypeBuilder.HasNoKey();
    }
}
