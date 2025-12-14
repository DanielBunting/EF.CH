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

    #region Nested Types

    /// <summary>
    /// Nested type field definitions for scaffolding.
    /// Stores field info as string array: ["FieldName (ClrType)", ...].
    /// </summary>
    public const string NestedFields = Prefix + "NestedFields";

    #endregion

    #region Default For Null

    /// <summary>
    /// The default value to use instead of NULL for this property.
    /// When set, the column is generated as non-nullable with this value as the DEFAULT.
    /// </summary>
    public const string DefaultForNull = Prefix + "DefaultForNull";

    #endregion

    #region Enum Types

    /// <summary>
    /// The ClickHouse enum type definition string (e.g., "Enum8('Pending' = 0, 'Shipped' = 1)").
    /// </summary>
    public const string EnumDefinition = Prefix + "EnumDefinition";

    /// <summary>
    /// The suggested C# enum type name for scaffolding.
    /// </summary>
    public const string EnumTypeName = Prefix + "EnumTypeName";

    #endregion

    #region Dictionaries

    /// <summary>
    /// Marks this entity as a ClickHouse dictionary.
    /// </summary>
    public const string Dictionary = Prefix + "Dictionary";

    /// <summary>
    /// The source table name for the dictionary.
    /// </summary>
    public const string DictionarySource = Prefix + "DictionarySource";

    /// <summary>
    /// The source entity type for the dictionary.
    /// </summary>
    public const string DictionarySourceType = Prefix + "DictionarySourceType";

    /// <summary>
    /// The custom query for the dictionary SOURCE clause.
    /// </summary>
    public const string DictionarySourceQuery = Prefix + "DictionarySourceQuery";

    /// <summary>
    /// The dictionary layout type (Flat, Hashed, etc.).
    /// </summary>
    public const string DictionaryLayout = Prefix + "DictionaryLayout";

    /// <summary>
    /// Layout-specific options as a dictionary.
    /// </summary>
    public const string DictionaryLayoutOptions = Prefix + "DictionaryLayoutOptions";

    /// <summary>
    /// The minimum lifetime in seconds for dictionary refresh.
    /// </summary>
    public const string DictionaryLifetimeMin = Prefix + "DictionaryLifetimeMin";

    /// <summary>
    /// The maximum lifetime in seconds for dictionary refresh.
    /// </summary>
    public const string DictionaryLifetimeMax = Prefix + "DictionaryLifetimeMax";

    /// <summary>
    /// The key columns for the dictionary PRIMARY KEY.
    /// </summary>
    public const string DictionaryKeyColumns = Prefix + "DictionaryKeyColumns";

    /// <summary>
    /// Default values for dictionary attributes (property name → default value).
    /// </summary>
    public const string DictionaryDefaults = Prefix + "DictionaryDefaults";

    /// <summary>
    /// The LINQ expression for dictionary source projection.
    /// </summary>
    public const string DictionaryProjectionExpression = Prefix + "DictionaryProjectionExpression";

    /// <summary>
    /// The LINQ expression for dictionary source filter.
    /// </summary>
    public const string DictionaryFilterExpression = Prefix + "DictionaryFilterExpression";

    #endregion
}
