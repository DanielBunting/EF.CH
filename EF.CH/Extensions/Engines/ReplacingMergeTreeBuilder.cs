using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder that provides fluent chaining for configuring ReplacingMergeTree-specific options.
/// </summary>
/// <remarks>
/// Returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseReplacingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, Expression{Func{TEntity, object}})"/>.
/// Add a version or is-deleted column via <see cref="WithVersion{TProp}"/> /
/// <see cref="WithIsDeleted{TProp}"/>; these write the same annotations the
/// legacy multi-parameter overloads wrote. Replication and cluster knobs are
/// inherited from <see cref="MergeTreeFamilyBuilder{TBuilder, TEntity}"/>.
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class ReplacingMergeTreeBuilder<TEntity>
    : MergeTreeFamilyBuilder<ReplacingMergeTreeBuilder<TEntity>, TEntity>
    where TEntity : class
{
    internal ReplacingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder) : base(builder)
    {
    }

    /// <summary>
    /// Configures the version column for deduplication. ClickHouse keeps the row
    /// with the highest version per ORDER BY key during background merges.
    /// </summary>
    /// <typeparam name="TProp">The version column type.</typeparam>
    /// <param name="versionColumn">Expression selecting the version column.</param>
    public ReplacingMergeTreeBuilder<TEntity> WithVersion<TProp>(
        Expression<Func<TEntity, TProp>> versionColumn)
    {
        ArgumentNullException.ThrowIfNull(versionColumn);
        var name = ExpressionExtensions.GetPropertyName(versionColumn);
        Builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, name);
        return this;
    }

    /// <summary>
    /// Configures the is-deleted column. Requires ClickHouse 23.2+. When the
    /// winning version has <c>is_deleted = 1</c>, the row is physically removed
    /// during background merges.
    /// </summary>
    /// <typeparam name="TProp">The is-deleted column type (typically UInt8/byte).</typeparam>
    /// <param name="isDeletedColumn">Expression selecting the is-deleted column.</param>
    public ReplacingMergeTreeBuilder<TEntity> WithIsDeleted<TProp>(
        Expression<Func<TEntity, TProp>> isDeletedColumn)
    {
        ArgumentNullException.ThrowIfNull(isDeletedColumn);
        var name = ExpressionExtensions.GetPropertyName(isDeletedColumn);
        Builder.HasAnnotation(ClickHouseAnnotationNames.IsDeletedColumn, name);
        return this;
    }
}
