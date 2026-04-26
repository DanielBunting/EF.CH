using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Operations;

/// <summary>
/// Alters the refresh schedule of an existing refreshable materialized view.
/// Generates: <c>ALTER TABLE "view" MODIFY REFRESH (EVERY|AFTER) &lt;interval&gt; [OFFSET ...] [RANDOMIZE FOR ...] [DEPENDS ON ...]</c>.
/// </summary>
public class ModifyRefreshOperation : MigrationOperation
{
    /// <summary>The view name.</summary>
    public string Name { get; set; } = null!;

    /// <summary>The schema name.</summary>
    public string? Schema { get; set; }

    /// <summary>"EVERY" or "AFTER".</summary>
    public string Kind { get; set; } = null!;

    /// <summary>Refresh interval literal (e.g. "5 MINUTE").</summary>
    public string Interval { get; set; } = null!;

    /// <summary>Optional OFFSET interval literal.</summary>
    public string? Offset { get; set; }

    /// <summary>Optional RANDOMIZE FOR interval literal.</summary>
    public string? RandomizeFor { get; set; }

    /// <summary>Optional DEPENDS ON list (table names).</summary>
    public string[]? DependsOn { get; set; }
}
