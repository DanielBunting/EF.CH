namespace EF.CH.Projections;

/// <summary>
/// The type of ClickHouse projection.
/// </summary>
public enum ProjectionType
{
    /// <summary>
    /// A sort-order projection that re-sorts data by different columns.
    /// Generates: SELECT * ORDER BY (column1, column2)
    /// </summary>
    SortOrder,

    /// <summary>
    /// An aggregation projection that pre-aggregates data with GROUP BY.
    /// Generates: SELECT grouping_cols, agg_funcs GROUP BY grouping_cols
    /// </summary>
    Aggregation,

    /// <summary>
    /// A raw SQL projection (escape hatch for complex expressions).
    /// The user provides the full SELECT statement.
    /// </summary>
    Raw
}
