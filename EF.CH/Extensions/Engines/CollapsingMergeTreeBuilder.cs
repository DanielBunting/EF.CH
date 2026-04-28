using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder that provides fluent chaining for configuring CollapsingMergeTree-specific options.
/// </summary>
/// <remarks>
/// Returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseCollapsingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, Expression{Func{TEntity, object}})"/>.
/// The sign column is required — ClickHouse uses the +1/-1 sign during
/// background merges to collapse cancellation pairs. Replication and cluster
/// knobs are inherited from <see cref="MergeTreeFamilyBuilder{TBuilder, TEntity}"/>.
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class CollapsingMergeTreeBuilder<TEntity>
    : MergeTreeFamilyBuilder<CollapsingMergeTreeBuilder<TEntity>, TEntity>
    where TEntity : class
{
    internal CollapsingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder) : base(builder)
    {
    }

    /// <summary>
    /// Configures the sign column (Int8/sbyte; values +1 or -1).
    /// </summary>
    /// <param name="signColumn">Expression selecting the sign column.</param>
    public CollapsingMergeTreeBuilder<TEntity> WithSign(
        Expression<Func<TEntity, sbyte>> signColumn)
    {
        ArgumentNullException.ThrowIfNull(signColumn);
        var name = ExpressionExtensions.GetPropertyName(signColumn);
        Builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, name);
        return this;
    }
}
