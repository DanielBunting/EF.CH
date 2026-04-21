using EF.CH.QueryProfiling;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for ClickHouse query profiling operations.
/// </summary>
public static class ClickHouseQueryProfilingExtensions
{
    /// <summary>
    /// Executes an EXPLAIN query for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="configure">Optional action to configure explain options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN result containing query plan information.</returns>
    public static Task<ExplainResult> ExplainAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        Action<ExplainOptions>? configure = null,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(context);

        var profiler = context.GetService<IClickHouseQueryProfiler>();
        return profiler.ExplainAsync(query, configure, cancellationToken);
    }

    /// <summary>
    /// Executes EXPLAIN PLAN for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN PLAN result.</returns>
    public static Task<ExplainResult> ExplainPlanAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ExplainAsync(context, opts => opts.Type = ExplainType.Plan, cancellationToken);
    }

    /// <summary>
    /// Executes EXPLAIN AST for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN AST result.</returns>
    public static Task<ExplainResult> ExplainAstAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ExplainAsync(context, opts => opts.Type = ExplainType.Ast, cancellationToken);
    }

    /// <summary>
    /// Executes EXPLAIN SYNTAX for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN SYNTAX result.</returns>
    public static Task<ExplainResult> ExplainSyntaxAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ExplainAsync(context, opts => opts.Type = ExplainType.Syntax, cancellationToken);
    }

    /// <summary>
    /// Executes EXPLAIN QUERY TREE for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN QUERY TREE result.</returns>
    public static Task<ExplainResult> ExplainQueryTreeAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ExplainAsync(context, opts => opts.Type = ExplainType.QueryTree, cancellationToken);
    }

    /// <summary>
    /// Executes EXPLAIN PIPELINE for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN PIPELINE result.</returns>
    public static Task<ExplainResult> ExplainPipelineAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ExplainAsync(context, opts => opts.Type = ExplainType.Pipeline, cancellationToken);
    }

    /// <summary>
    /// Executes EXPLAIN ESTIMATE for the specified LINQ query.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to explain.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN ESTIMATE result.</returns>
    public static Task<ExplainResult> ExplainEstimateAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        return query.ExplainAsync(context, opts => opts.Type = ExplainType.Estimate, cancellationToken);
    }

    /// <summary>
    /// Executes the query and returns results along with execution statistics.
    /// </summary>
    /// <typeparam name="T">The entity type of the query.</typeparam>
    /// <param name="query">The LINQ query to execute.</param>
    /// <param name="context">The DbContext associated with the query.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Query results with execution statistics.</returns>
    /// <remarks>
    /// Statistics are retrieved from system.query_log after query execution.
    /// Due to ClickHouse's asynchronous logging, statistics may not be immediately
    /// available and retrieval is best-effort.
    /// </remarks>
    public static Task<QueryResultWithStats<T>> ToListWithStatsAsync<T>(
        this IQueryable<T> query,
        DbContext context,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(context);

        var profiler = context.GetService<IClickHouseQueryProfiler>();
        return profiler.ToListWithStatsAsync(query, cancellationToken);
    }

    /// <summary>
    /// Executes an EXPLAIN query for raw SQL.
    /// </summary>
    /// <param name="context">The DbContext to use for execution.</param>
    /// <param name="sql">The SQL query to explain.</param>
    /// <param name="configure">Optional action to configure explain options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The EXPLAIN result containing query plan information.</returns>
    public static Task<ExplainResult> ExplainSqlAsync(
        this DbContext context,
        string sql,
        Action<ExplainOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(sql);

        var profiler = context.GetService<IClickHouseQueryProfiler>();
        return profiler.ExplainSqlAsync(sql, configure, cancellationToken);
    }
}
