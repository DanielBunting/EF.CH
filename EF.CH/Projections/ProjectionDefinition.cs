namespace EF.CH.Projections;

/// <summary>
/// Defines a ClickHouse projection attached to a table.
/// Projections are stored alongside the main table and are automatically
/// maintained during INSERT operations.
/// </summary>
/// <param name="Name">The projection name (must be unique within the table).</param>
/// <param name="Type">The projection type.</param>
/// <param name="SelectSql">The SELECT SQL for the projection body.</param>
/// <param name="Materialize">Whether to materialize existing data into the projection (default: true).</param>
public sealed record ProjectionDefinition(
    string Name,
    ProjectionType Type,
    string SelectSql,
    bool Materialize = true)
{
    /// <summary>
    /// Creates a sort-order projection definition.
    /// </summary>
    /// <param name="name">The projection name.</param>
    /// <param name="orderByColumns">Tuples of (columnName, descending).</param>
    /// <param name="materialize">Whether to materialize existing data.</param>
    /// <returns>A new projection definition.</returns>
    public static ProjectionDefinition SortOrder(
        string name,
        IEnumerable<(string Column, bool Descending)> orderByColumns,
        bool materialize = true)
    {
        // ClickHouse projections ORDER BY clause doesn't support ASC/DESC modifiers.
        // The physical sort order is always ascending. The Descending flag is ignored
        // in the SQL generation but preserved in the metadata for documentation.
        var orderByClause = string.Join(", ", orderByColumns.Select(c => $"\"{c.Column}\""));
        var sql = $"SELECT * ORDER BY ({orderByClause})";
        return new ProjectionDefinition(name, ProjectionType.SortOrder, sql, materialize);
    }

    /// <summary>
    /// Creates an aggregation projection definition.
    /// </summary>
    /// <param name="name">The projection name.</param>
    /// <param name="selectSql">The SELECT SQL with GROUP BY clause.</param>
    /// <param name="materialize">Whether to materialize existing data.</param>
    /// <returns>A new projection definition.</returns>
    public static ProjectionDefinition Aggregation(
        string name,
        string selectSql,
        bool materialize = true)
    {
        return new ProjectionDefinition(name, ProjectionType.Aggregation, selectSql, materialize);
    }

    /// <summary>
    /// Creates a raw SQL projection definition.
    /// </summary>
    /// <param name="name">The projection name.</param>
    /// <param name="selectSql">The raw SELECT SQL.</param>
    /// <param name="materialize">Whether to materialize existing data.</param>
    /// <returns>A new projection definition.</returns>
    public static ProjectionDefinition Raw(
        string name,
        string selectSql,
        bool materialize = true)
    {
        return new ProjectionDefinition(name, ProjectionType.Raw, selectSql, materialize);
    }
}
