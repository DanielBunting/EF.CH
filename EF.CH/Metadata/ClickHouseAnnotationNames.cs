namespace EF.CH.Metadata;

/// <summary>
/// Annotation names for ClickHouse-specific metadata.
/// </summary>
public static class ClickHouseAnnotationNames
{
    /// <summary>
    /// The prefix for all ClickHouse annotations.
    /// </summary>
    public const string Prefix = "ClickHouse:";

    /// <summary>
    /// The table engine (e.g., MergeTree, ReplacingMergeTree).
    /// </summary>
    public const string Engine = Prefix + "Engine";

    /// <summary>
    /// The ORDER BY columns for MergeTree engines.
    /// </summary>
    public const string OrderBy = Prefix + "OrderBy";

    /// <summary>
    /// The PRIMARY KEY columns (defaults to ORDER BY if not specified).
    /// </summary>
    public const string PrimaryKey = Prefix + "PrimaryKey";

    /// <summary>
    /// The PARTITION BY expression.
    /// </summary>
    public const string PartitionBy = Prefix + "PartitionBy";

    /// <summary>
    /// The SAMPLE BY expression for sampling.
    /// </summary>
    public const string SampleBy = Prefix + "SampleBy";

    /// <summary>
    /// The TTL expression for data expiration.
    /// </summary>
    public const string Ttl = Prefix + "TTL";

    /// <summary>
    /// Additional engine settings.
    /// </summary>
    public const string Settings = Prefix + "Settings";

    /// <summary>
    /// The version column for ReplacingMergeTree.
    /// </summary>
    public const string VersionColumn = Prefix + "VersionColumn";

    /// <summary>
    /// The sign column for CollapsingMergeTree.
    /// </summary>
    public const string SignColumn = Prefix + "SignColumn";

    #region Materialized Views

    /// <summary>
    /// Marks this entity as a materialized view.
    /// </summary>
    public const string MaterializedView = Prefix + "MaterializedView";

    /// <summary>
    /// The source table name for the materialized view.
    /// </summary>
    public const string MaterializedViewSource = Prefix + "MaterializedViewSource";

    /// <summary>
    /// The SELECT query for the materialized view.
    /// </summary>
    public const string MaterializedViewQuery = Prefix + "MaterializedViewQuery";

    /// <summary>
    /// Whether to use POPULATE when creating the materialized view.
    /// </summary>
    public const string MaterializedViewPopulate = Prefix + "MaterializedViewPopulate";

    #endregion
}
