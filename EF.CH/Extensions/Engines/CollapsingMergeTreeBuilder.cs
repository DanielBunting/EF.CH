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
/// background merges to collapse cancellation pairs.
/// </remarks>
/// <typeparam name="TEntity">The entity type being configured.</typeparam>
public sealed class CollapsingMergeTreeBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;

    internal CollapsingMergeTreeBuilder(EntityTypeBuilder<TEntity> builder)
    {
        _builder = builder;
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
        _builder.HasAnnotation(ClickHouseAnnotationNames.SignColumn, name);
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
        CollapsingMergeTreeBuilder<TEntity> b) => b._builder;
}
