namespace EF.CH.QueryProfiling;

/// <summary>
/// Specifies the type of EXPLAIN query to execute against ClickHouse.
/// </summary>
public enum ExplainType
{
    /// <summary>
    /// EXPLAIN PLAN - Shows the query execution plan (default).
    /// </summary>
    Plan,

    /// <summary>
    /// EXPLAIN AST - Shows the Abstract Syntax Tree of the query.
    /// </summary>
    Ast,

    /// <summary>
    /// EXPLAIN SYNTAX - Shows the query after syntax optimization (query rewriting).
    /// </summary>
    Syntax,

    /// <summary>
    /// EXPLAIN QUERY TREE - Shows the query tree representation.
    /// </summary>
    QueryTree,

    /// <summary>
    /// EXPLAIN PIPELINE - Shows the query execution pipeline.
    /// </summary>
    Pipeline,

    /// <summary>
    /// EXPLAIN ESTIMATE - Shows estimated row counts and sizes.
    /// </summary>
    Estimate
}
