using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseMergeTree{TEntity}(EntityTypeBuilder{TEntity}, System.Linq.Expressions.Expression{System.Func{TEntity, object}})"/>.
/// Plain MergeTree has no engine-specific knobs beyond ORDER BY; the inherited
/// <c>WithReplication</c>, <c>WithCluster</c>, and <c>WithTableGroup</c> from
/// <see cref="MergeTreeFamilyBuilder{TBuilder, TEntity}"/> are the full surface.
/// </summary>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class MergeTreeBuilder<TEntity>
    : MergeTreeFamilyBuilder<MergeTreeBuilder<TEntity>, TEntity>
    where TEntity : class
{
    internal MergeTreeBuilder(EntityTypeBuilder<TEntity> builder) : base(builder)
    {
    }
}
