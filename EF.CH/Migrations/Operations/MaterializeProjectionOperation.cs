using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Operations;

/// <summary>
/// A migration operation to materialize a projection for existing data.
/// Generates: ALTER TABLE "table" MATERIALIZE PROJECTION "name" [IN PARTITION partition]
/// </summary>
public class MaterializeProjectionOperation : MigrationOperation
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string Table { get; set; } = null!;

    /// <summary>
    /// The schema name.
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// The projection name to materialize.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// Optional partition to materialize. If null, materializes all partitions.
    /// </summary>
    public string? InPartition { get; set; }
}
