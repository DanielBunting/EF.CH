using System.Linq.Expressions;

namespace EF.CH.Views;

/// <summary>
/// Strongly-typed metadata for a plain (non-parameterized, non-materialized) ClickHouse view.
/// </summary>
/// <typeparam name="TResult">The view result entity type.</typeparam>
public sealed class ViewMetadata<TResult>
    where TResult : class
{
    /// <summary>
    /// Creates a new view metadata instance.
    /// </summary>
    /// <param name="viewName">The view name in ClickHouse.</param>
    /// <param name="resultType">The CLR type of the result entity.</param>
    public ViewMetadata(string viewName, Type resultType)
    {
        ViewName = viewName;
        ResultType = resultType;
    }

    /// <summary>
    /// The view name in ClickHouse.
    /// </summary>
    public string ViewName { get; }

    /// <summary>
    /// The CLR type of the result entity.
    /// </summary>
    public Type ResultType { get; }
}

/// <summary>
/// Non-generic base class for plain view metadata storage.
/// Used for annotation storage, fluent configuration, and SQL generation.
/// </summary>
public sealed class ViewMetadataBase
{
    /// <summary>
    /// The view name in ClickHouse.
    /// </summary>
    public required string ViewName { get; init; }

    /// <summary>
    /// The CLR type of the view result entity.
    /// </summary>
    public required Type ResultType { get; init; }

    /// <summary>
    /// The CLR type of the source entity (for fluent configuration).
    /// </summary>
    public Type? SourceType { get; init; }

    /// <summary>
    /// The source table name (explicit or derived from source entity).
    /// </summary>
    public string? SourceTable { get; init; }

    /// <summary>
    /// The projection expression for SELECT clause generation.
    /// </summary>
    public LambdaExpression? ProjectionExpression { get; init; }

    /// <summary>
    /// The WHERE clause expressions for the view.
    /// </summary>
    public List<LambdaExpression>? WhereExpressions { get; init; }

    /// <summary>
    /// A pre-built raw SELECT SQL body (used by AsViewRaw). When present, this
    /// takes precedence over <see cref="ProjectionExpression"/> and
    /// <see cref="WhereExpressions"/>.
    /// </summary>
    public string? RawSelectSql { get; init; }

    /// <summary>
    /// Whether to emit IF NOT EXISTS.
    /// </summary>
    public bool IfNotExists { get; init; }

    /// <summary>
    /// Whether to emit OR REPLACE. Mutually exclusive with <see cref="IfNotExists"/>.
    /// </summary>
    public bool OrReplace { get; init; }

    /// <summary>
    /// Optional ON CLUSTER cluster name.
    /// </summary>
    public string? OnCluster { get; init; }

    /// <summary>
    /// Optional schema (database) qualifier for the view.
    /// </summary>
    public string? Schema { get; init; }
}
