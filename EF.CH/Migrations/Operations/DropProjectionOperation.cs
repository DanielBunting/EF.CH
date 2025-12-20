using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Operations;

/// <summary>
/// A migration operation to drop a projection from a table.
/// Generates: ALTER TABLE "table" DROP PROJECTION "name"
/// </summary>
public class DropProjectionOperation : MigrationOperation
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
    /// The projection name to drop.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// Whether to use IF EXISTS to avoid errors when projection doesn't exist.
    /// </summary>
    public bool IfExists { get; set; } = true;
}
