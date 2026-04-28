using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseAggregatingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, System.Linq.Expressions.Expression{System.Func{TEntity, object}})"/>.
/// Currently has no engine-specific knobs beyond ORDER BY but exists for shape
/// symmetry across engines. Replication and cluster knobs are inherited from
/// <see cref="MergeTreeFamilyBuilder{TBuilder, TEntity}"/>.
/// </summary>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class AggregatingMergeTreeBuilder<TEntity>
    : MergeTreeFamilyBuilder<AggregatingMergeTreeBuilder<TEntity>, TEntity>
    where TEntity : class
{
    internal AggregatingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder) : base(builder)
    {
    }
}
