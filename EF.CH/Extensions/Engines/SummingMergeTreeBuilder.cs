using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseSummingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, System.Linq.Expressions.Expression{System.Func{TEntity, object}})"/>.
/// Currently has no engine-specific knobs beyond ORDER BY but exists for shape
/// symmetry across engines and to leave room for future SUM-column targeting.
/// Replication and cluster knobs are inherited from
/// <see cref="MergeTreeFamilyBuilder{TBuilder, TEntity}"/>.
/// </summary>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class SummingMergeTreeBuilder<TEntity>
    : MergeTreeFamilyBuilder<SummingMergeTreeBuilder<TEntity>, TEntity>
    where TEntity : class
{
    internal SummingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder) : base(builder)
    {
    }
}
