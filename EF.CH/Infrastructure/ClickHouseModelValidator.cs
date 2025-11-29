using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EF.CH.Infrastructure;

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

    // Add ClickHouse-specific validation here
    // For example:
    // - Warn about missing ORDER BY for MergeTree
    // - Error on foreign key constraints (not supported)
}
