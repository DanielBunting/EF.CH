using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Projections;

/// <summary>
/// Entry point fluent builder for configuring projections.
/// Use <see cref="OrderBy{TProperty}"/> for sort-order projections or
/// <see cref="GroupBy{TKey}"/> for aggregation projections.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
public class ProjectionBuilder<TEntity> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;
    private readonly string? _explicitName;

    /// <summary>
    /// Creates a new projection builder.
    /// </summary>
    /// <param name="builder">The entity type builder.</param>
    /// <param name="name">Optional explicit projection name. If null, name is auto-generated.</param>
    internal ProjectionBuilder(EntityTypeBuilder<TEntity> builder, string? name = null)
    {
        _builder = builder;
        _explicitName = name;
    }

    /// <summary>
    /// Starts configuring a sort-order projection with the first ORDER BY column.
    /// </summary>
    /// <typeparam name="TProperty">The property type.</typeparam>
    /// <param name="column">Expression selecting the first column to order by.</param>
    /// <returns>A builder for configuring additional ORDER BY columns.</returns>
    public SortOrderProjectionBuilder<TEntity> OrderBy<TProperty>(
        Expression<Func<TEntity, TProperty>> column)
    {
        ArgumentNullException.ThrowIfNull(column);
        return new SortOrderProjectionBuilder<TEntity>(_builder, _explicitName)
            .OrderBy(column);
    }

    /// <summary>
    /// Starts configuring an aggregation projection with a GROUP BY clause.
    /// </summary>
    /// <typeparam name="TKey">The grouping key type.</typeparam>
    /// <param name="keySelector">Expression selecting the grouping key.</param>
    /// <returns>A builder for configuring the SELECT clause.</returns>
    public GroupByProjectionBuilder<TEntity, TKey> GroupBy<TKey>(
        Expression<Func<TEntity, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        return new GroupByProjectionBuilder<TEntity, TKey>(_builder, _explicitName, keySelector);
    }
}
