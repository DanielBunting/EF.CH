using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseAggregatingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, System.Linq.Expressions.Expression{System.Func{TEntity, object}})"/>.
/// Currently has no engine-specific knobs beyond ORDER BY but exists for shape
/// symmetry across engines.
/// </summary>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class AggregatingMergeTreeBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;

    internal AggregatingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder)
    {
        _builder = builder;
    }

    /// <summary>
    /// Returns the underlying entity type builder for continued configuration.
    /// </summary>
    public EntityTypeBuilder<TEntity> And() => _builder;

    /// <summary>
    /// Implicit conversion back to <see cref="EntityTypeBuilder{TEntity}"/>.
    /// </summary>
    public static implicit operator EntityTypeBuilder<TEntity>(
        AggregatingMergeTreeBuilder<TEntity> b) => b._builder;
}
