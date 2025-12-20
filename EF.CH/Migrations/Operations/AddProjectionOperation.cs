using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Operations;

/// <summary>
/// A migration operation to add a projection to a table.
/// Generates: ALTER TABLE "table" ADD PROJECTION "name" (SELECT ...)
/// </summary>
public class AddProjectionOperation : MigrationOperation
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
    /// The projection name.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// The projection SELECT SQL.
    /// </summary>
    public string SelectSql { get; set; } = null!;

    /// <summary>
    /// Whether to materialize existing data into the projection.
    /// </summary>
    public bool Materialize { get; set; } = true;
}
