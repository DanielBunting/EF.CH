using System.Linq.Expressions;
using EF.CH.InsertSelect;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for server-side INSERT ... SELECT operations.
/// </summary>
public static class ClickHouseInsertSelectExtensions
{
    /// <summary>
    /// Inserts the results of this query into the specified target DbSet using a server-side INSERT ... SELECT operation.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <param name="sourceQuery">The source query to select data from.</param>
    /// <param name="targetDbSet">The target DbSet to insert into.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the insert operation.</returns>
    /// <example>
    /// <code>
    /// await context.Events
    ///     .Where(e => e.Category == "important")
    ///     .InsertIntoAsync(context.ArchivedEvents);
    /// </code>
    /// </example>
    public static Task<ClickHouseInsertSelectResult> InsertIntoAsync<TEntity>(
        this IQueryable<TEntity> sourceQuery,
        DbSet<TEntity> targetDbSet,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var context = targetDbSet.GetService<ICurrentDbContext>().Context;
        var executor = context.GetService<IClickHouseInsertSelectExecutor>();
        return executor.ExecuteAsync(sourceQuery, cancellationToken);
    }

    /// <summary>
    /// Inserts the results of this query into the specified target DbSet using a server-side INSERT ... SELECT operation,
    /// with a mapping expression to transform the data.
    /// </summary>
    /// <typeparam name="TSource">The source entity type.</typeparam>
    /// <typeparam name="TTarget">The target entity type.</typeparam>
    /// <param name="sourceQuery">The source query to select data from.</param>
    /// <param name="targetDbSet">The target DbSet to insert into.</param>
    /// <param name="mapping">Expression to map source entities to target entities.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the insert operation.</returns>
    /// <example>
    /// <code>
    /// await context.Events
    ///     .Where(e => e.Category == "important")
    ///     .InsertIntoAsync(context.Archives, e => new Archive { OriginalId = e.Id, Data = e.Data });
    /// </code>
    /// </example>
    public static Task<ClickHouseInsertSelectResult> InsertIntoAsync<TSource, TTarget>(
        this IQueryable<TSource> sourceQuery,
        DbSet<TTarget> targetDbSet,
        Expression<Func<TSource, TTarget>> mapping,
        CancellationToken cancellationToken = default)
        where TSource : class
        where TTarget : class
    {
        var context = targetDbSet.GetService<ICurrentDbContext>().Context;
        var executor = context.GetService<IClickHouseInsertSelectExecutor>();
        return executor.ExecuteAsync(sourceQuery, mapping, cancellationToken);
    }
}
