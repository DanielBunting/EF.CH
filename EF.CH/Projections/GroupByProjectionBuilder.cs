using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EF.CH.Projections;

/// <summary>
/// Intermediate builder for aggregation projections after GroupBy.
/// Requires <see cref="Select{TResult}"/> to be called to complete the projection.
/// </summary>
/// <typeparam name="TEntity">The entity type.</typeparam>
/// <typeparam name="TKey">The grouping key type.</typeparam>
public class GroupByProjectionBuilder<TEntity, TKey> where TEntity : class
{
    private readonly EntityTypeBuilder<TEntity> _builder;
    private readonly string? _explicitName;
    private readonly Expression<Func<TEntity, TKey>> _keySelector;

    /// <summary>
    /// Creates a new GroupBy projection builder.
    /// </summary>
    internal GroupByProjectionBuilder(
        EntityTypeBuilder<TEntity> builder,
        string? name,
        Expression<Func<TEntity, TKey>> keySelector)
    {
        _builder = builder;
        _explicitName = name;
        _keySelector = keySelector;
    }

    /// <summary>
    /// Configures the SELECT clause for the aggregation projection.
    /// </summary>
    /// <typeparam name="TResult">The result type (can be anonymous).</typeparam>
    /// <param name="selector">Expression selecting the aggregated result.</param>
    /// <returns>A builder for finalizing the projection.</returns>
    public AggregationProjectionBuilder<TEntity, TKey, TResult> Select<TResult>(
        Expression<Func<IGrouping<TKey, TEntity>, TResult>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        return new AggregationProjectionBuilder<TEntity, TKey, TResult>(
            _builder, _explicitName, _keySelector, selector);
    }
}
