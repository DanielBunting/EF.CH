using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Extensions.Engines;

/// <summary>
/// A builder returned by <see cref="ClickHouseEntityTypeBuilderExtensions.UseSummingMergeTree{TEntity}(EntityTypeBuilder{TEntity}, System.Linq.Expressions.Expression{System.Func{TEntity, object}})"/>.
/// Currently has no engine-specific knobs beyond ORDER BY but exists for shape
/// symmetry across engines and to leave room for future SUM-column targeting.
/// </summary>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class SummingMergeTreeBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;

    internal SummingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder)
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
        SummingMergeTreeBuilder<TEntity> b) => b._builder;
}
