using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Metadata.Internal;

/// <summary>
/// Provides ClickHouse-specific annotations for the relational model.
/// </summary>
public class ClickHouseAnnotationProvider : RelationalAnnotationProvider
{
    public ClickHouseAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
        : base(dependencies)
    {
    }

    // Override methods as needed for ClickHouse-specific annotations
    // For example, MergeTree engine settings, ORDER BY keys, etc.
}
