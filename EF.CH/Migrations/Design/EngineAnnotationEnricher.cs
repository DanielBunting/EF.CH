using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Bakes ClickHouse engine-knob annotations from an entity type onto a
/// <see cref="CreateTableOperation"/>. The migrations SQL generator can fall
/// back to entity-type lookup when an operation lacks an annotation, but that
/// fallback only works when the runtime model still matches the migration's
/// historical model. Scaffolded migrations must be self-contained, so we
/// copy the annotations onto the operation at scaffold time.
/// </summary>
internal static class EngineAnnotationEnricher
{
    /// <summary>
    /// Engine-knob annotations the SQL generator reads off
    /// <see cref="CreateTableOperation"/>. Mirrors the
    /// <c>GetAnnotation(...) ?? GetEntityAnnotation(...)</c> fallback chain
    /// in <c>ClickHouseMigrationsSqlGenerator</c>'s engine-clause path.
    /// Keep this list in sync with that path — a missing key here means
    /// scaffolded migrations silently lose that knob at user runtime.
    /// </summary>
    private static readonly string[] EngineKnobKeys =
    {
        ClickHouseAnnotationNames.Engine,
        ClickHouseAnnotationNames.OrderBy,
        ClickHouseAnnotationNames.PartitionBy,
        ClickHouseAnnotationNames.PrimaryKey,
        ClickHouseAnnotationNames.SampleBy,
        ClickHouseAnnotationNames.Ttl,
        ClickHouseAnnotationNames.VersionColumn,
        ClickHouseAnnotationNames.IsDeletedColumn,
        ClickHouseAnnotationNames.SignColumn,
        ClickHouseAnnotationNames.Settings,
        ClickHouseAnnotationNames.ExternalEngineArguments,
        ClickHouseAnnotationNames.EntityClusterName,
    };

    /// <summary>
    /// Copies engine-knob annotations from the model's matching entity type
    /// onto <paramref name="operation"/>. No-op when the operation has no
    /// matching entity type or when an annotation already exists on the
    /// operation (caller's explicit value wins).
    /// </summary>
    public static void Enrich(CreateTableOperation operation, IModel model)
    {
        var entityType = model.GetEntityTypes().FirstOrDefault(e =>
            string.Equals(e.GetTableName(), operation.Name, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(e.GetSchema() ?? model.GetDefaultSchema(), operation.Schema, StringComparison.OrdinalIgnoreCase));

        if (entityType is null) return;

        foreach (var key in EngineKnobKeys)
        {
            if (operation.FindAnnotation(key) is not null) continue;
            var value = entityType.FindAnnotation(key)?.Value;
            if (value is not null) operation.AddAnnotation(key, value);
        }
    }
}
