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
/// out of order.
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class VersionedCollapsingMergeTreeBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;

    internal VersionedCollapsingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Configures the sign column (Int8/sbyte; values +1 or -1).
    /// </summary>
    public VersionedCollapsingMergeTreeBuilder<TEntity> WithSign(
        Expression<Func<TEntity, sbyte>> signColumn)
    {
        ArgumentNullException.ThrowIfNull(signColumn);
        var name = ExpressionExtensions.GetPropertyName(signColumn);
        _builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, name);
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
        _builder.HasAnnotation(ClickHouseAnnotationNames.VersionColumn, name);
        return this;
    }

    /// <summary>
    /// Returns the underlying entity type builder for continued configuration.
    /// </summary>
    public EntityTypeBuilder<TEntity> And() => _builder;

    /// <summary>
    /// Implicit conversion back to <see cref="EntityTypeBuilder{TEntity}"/>.
    /// </summary>
    public static implicit operator EntityTypeBuilder<TEntity>(
        VersionedCollapsingMergeTreeBuilder<TEntity> b) => b._builder;
}
