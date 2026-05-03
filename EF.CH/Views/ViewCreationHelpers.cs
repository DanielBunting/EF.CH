using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Views;

/// <summary>
/// Shared logic for enumerating CREATE VIEW DDL from the EF Core model.
/// Used by <c>ClickHouseDatabaseCreator</c> (post-pass after table creation) and by
/// the <c>EnsureViewsAsync</c> / <c>EnsureParameterizedViewsAsync</c> extensions.
/// All emitted DDL is idempotent (forces <c>IF NOT EXISTS</c>).
/// </summary>
internal static class ViewCreationHelpers
{
    /// <summary>
    /// Enumerates idempotent CREATE VIEW SQL for every plain-view entity in the model
    /// (configured via <c>AsView</c> / <c>AsViewRaw</c>). Skips entities marked
    /// <c>AsViewDeferred</c> and entities configured with only <c>HasView(name)</c>
    /// (no DDL metadata to emit).
    /// </summary>
    public static IEnumerable<string> EnumeratePlainViewDdl(IModel model)
    {
        ArgumentNullException.ThrowIfNull(model);

        foreach (var entityType in model.GetEntityTypes())
        {
            var isView = entityType.FindAnnotation(
                ClickHouseAnnotationNames.View)?.Value as bool? ?? false;
            if (!isView)
                continue;

            var deferred = entityType.FindAnnotation(
                ClickHouseAnnotationNames.ViewDeferred)?.Value as bool? ?? false;
            if (deferred)
                continue;

            var metadata = entityType.FindAnnotation(
                ClickHouseAnnotationNames.ViewMetadata)?.Value as ViewMetadataBase;
            if (metadata == null)
                continue;

            yield return ViewSqlGenerator.GenerateCreateViewSql(model, ForceIfNotExists(metadata));
        }
    }

    /// <summary>
    /// Enumerates idempotent CREATE VIEW SQL for every parameterized-view entity in the
    /// model (configured via <c>AsParameterizedView</c>). Skips entities configured with
    /// only <c>ToParameterizedView(name)</c> (no DDL metadata to emit).
    /// </summary>
    public static IEnumerable<string> EnumerateParameterizedViewDdl(IModel model)
    {
        ArgumentNullException.ThrowIfNull(model);

        foreach (var entityType in model.GetEntityTypes())
        {
            var isParameterizedView = entityType.FindAnnotation(
                ClickHouseAnnotationNames.ParameterizedView)?.Value as bool? ?? false;
            if (!isParameterizedView)
                continue;

            var metadata = entityType.FindAnnotation(
                ClickHouseAnnotationNames.ParameterizedViewMetadata)?.Value
                as ParameterizedViews.ParameterizedViewMetadataBase;
            if (metadata == null)
                continue;

            yield return ParameterizedViews.ParameterizedViewSqlGenerator.GenerateCreateViewSql(
                model, metadata, ifNotExists: true);
        }
    }

    /// <summary>
    /// Clones <paramref name="source"/> with <c>IfNotExists = true, OrReplace = false</c>
    /// so the user's <c>AsView(...orReplace: true)</c> configuration doesn't bleed into the
    /// runtime ensure path (the two are mutually exclusive in ClickHouse).
    /// </summary>
    public static ViewMetadataBase ForceIfNotExists(ViewMetadataBase source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new ViewMetadataBase
        {
            ViewName = source.ViewName,
            ResultType = source.ResultType,
            SourceType = source.SourceType,
            SourceTable = source.SourceTable,
            ProjectionExpression = source.ProjectionExpression,
            WhereExpressions = source.WhereExpressions,
            RawSelectSql = source.RawSelectSql,
            IfNotExists = true,
            OrReplace = false,
            OnCluster = source.OnCluster,
            Schema = source.Schema,
        };
    }
}
