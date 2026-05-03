using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder that provides fluent chaining for configuring VersionedCollapsingMergeTree-specific options.
/// </summary>
/// <remarks>
/// Returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseVersionedCollapsingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, Expression{Func{TEntity, object}})"/>.
/// Both <see cref="WithSign"/> and <see cref="WithVersion{TProp}"/> are required —
/// the engine uses the version column to correctly collapse rows that arrived
/// out of order. Replication and cluster knobs are inherited from
/// <see cref="MergeTreeFamilyBuilder{TBuilder, TEntity}"/>.
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class VersionedCollapsingMergeTreeBuilder<TEntity>
    : MergeTreeFamilyBuilder<VersionedCollapsingMergeTreeBuilder<TEntity>, TEntity>
    where TEntity : class
{
    internal VersionedCollapsingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder) : base(builder)
    {
    }

    /// <summary>
    /// Configures the sign column (Int8/sbyte; values +1 or -1).
    /// </summary>
    public VersionedCollapsingMergeTreeBuilder<TEntity> WithSign(
        Expression<Func<TEntity, sbyte>> signColumn)
    {
        ArgumentNullException.ThrowIfNull(signColumn);
        var name = ExpressionExtensions.GetPropertyName(signColumn);
        Builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, name);
        return this;
    }

    /// <summary>
    /// Configures the version column (typically a monotonically increasing integer).
    /// </summary>
    public VersionedCollapsingMergeTreeBuilder<TEntity> WithVersion<TProp>(
        Expression<Func<TEntity, TProp>> versionColumn)
    {
        ArgumentNullException.ThrowIfNull(versionColumn);
        var name = ExpressionExtensions.GetPropertyName(versionColumn);
        Builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, name);
        return this;
    }
}
