using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EF.CH.Metadata.Conventions;

/// <summary>
/// Builds the convention set for ClickHouse.
/// </summary>
public class ClickHouseConventionSetBuilder : RelationalConventionSetBuilder
{
    private readonly IDbContextOptions _options;

    public ClickHouseConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies,
        IDbContextOptions options)
        : base(dependencies, relationalDependencies)
    {
        _options = options;
    }

    public override ConventionSet CreateConventionSet()
    {
        var conventionSet = base.CreateConventionSet();

        // Check if keyless entities by default is enabled
        var clickHouseOptions = _options.FindExtension<ClickHouseOptionsExtension>();
        if (clickHouseOptions?.UseKeylessEntitiesByDefault == true)
        {
            // Add convention that makes all entities keyless by default
            // This runs early, so explicit HasKey() in OnModelCreating will override it
            conventionSet.EntityTypeAddedConventions.Insert(0, new ClickHouseKeylessConvention());
        }

        // Add convention that removes ValueGeneratedOnAdd for integer types
        // ClickHouse doesn't support auto-increment/identity columns
        conventionSet.ModelFinalizingConventions.Add(new ClickHouseValueGeneratedConvention());

        // Add convention that discovers [ClickHouseCodec] attributes on properties
        conventionSet.PropertyAddedConventions.Add(new ClickHouseCodecAttributeConvention());

        return conventionSet;
    }
}
