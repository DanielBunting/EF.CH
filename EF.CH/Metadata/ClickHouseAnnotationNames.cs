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
    /// The is_deleted column for ReplacingMergeTree (ClickHouse 23.2+).
    /// When specified, rows with is_deleted=1 are physically removed during merge.
    /// </summary>
    public const string IsDeletedColumn = Prefix + "IsDeletedColumn";

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

    #region Dictionary External Sources

    /// <summary>
    /// The dictionary source provider type: "clickhouse" (default), "postgresql", "mysql", "http".
    /// </summary>
    public const string DictionarySourceProvider = Prefix + "DictionarySourceProvider";

    /// <summary>
    /// For external dictionary sources: table name in remote database.
    /// </summary>
    public const string DictionaryExternalTable = Prefix + "DictionaryExternalTable";

    /// <summary>
    /// For external dictionary sources: schema name (PostgreSQL only).
    /// </summary>
    public const string DictionaryExternalSchema = Prefix + "DictionaryExternalSchema";

    /// <summary>
    /// For external dictionary sources: WHERE clause filter.
    /// </summary>
    public const string DictionaryExternalWhere = Prefix + "DictionaryExternalWhere";

    /// <summary>
    /// For external dictionary sources: invalidate_query for cache refresh checking.
    /// </summary>
    public const string DictionaryInvalidateQuery = Prefix + "DictionaryInvalidateQuery";

    /// <summary>
    /// For MySQL dictionary sources: fail_on_connection_loss setting.
    /// </summary>
    public const string DictionaryMySqlFailOnConnectionLoss = Prefix + "DictionaryMySqlFailOnConnectionLoss";

    /// <summary>
    /// For HTTP dictionary sources: URL endpoint.
    /// </summary>
    public const string DictionaryHttpUrl = Prefix + "DictionaryHttpUrl";

    /// <summary>
    /// For HTTP dictionary sources: format (JSONEachRow, CSV, etc.).
    /// </summary>
    public const string DictionaryHttpFormat = Prefix + "DictionaryHttpFormat";

    /// <summary>
    /// For HTTP dictionary sources: custom headers as Dictionary&lt;string, string&gt;.
    /// </summary>
    public const string DictionaryHttpHeaders = Prefix + "DictionaryHttpHeaders";

    #endregion

    #region External Table Functions

    /// <summary>
    /// Marks this entity as an external table function (no DDL generated).
    /// </summary>
    public const string IsExternalTableFunction = Prefix + "External:IsTableFunction";

    /// <summary>
    /// The external provider name (e.g., "postgresql", "mysql").
    /// </summary>
    public const string ExternalProvider = Prefix + "External:Provider";

    /// <summary>
    /// The remote table name.
    /// </summary>
    public const string ExternalTable = Prefix + "External:Table";

    /// <summary>
    /// The remote schema name (PostgreSQL only).
    /// </summary>
    public const string ExternalSchema = Prefix + "External:Schema";

    /// <summary>
    /// Whether the external entity is read-only (default: true).
    /// </summary>
    public const string ExternalReadOnly = Prefix + "External:ReadOnly";

    /// <summary>
    /// Environment variable name for host:port.
    /// </summary>
    public const string ExternalHostPortEnv = Prefix + "External:HostPortEnv";

    /// <summary>
    /// Literal value for host:port (not recommended for production).
    /// </summary>
    public const string ExternalHostPortValue = Prefix + "External:HostPortValue";

    /// <summary>
    /// Environment variable name for database.
    /// </summary>
    public const string ExternalDatabaseEnv = Prefix + "External:DatabaseEnv";

    /// <summary>
    /// Literal value for database.
    /// </summary>
    public const string ExternalDatabaseValue = Prefix + "External:DatabaseValue";

    /// <summary>
    /// Environment variable name for username.
    /// </summary>
    public const string ExternalUserEnv = Prefix + "External:UserEnv";

    /// <summary>
    /// Literal value for username (not recommended for production).
    /// </summary>
    public const string ExternalUserValue = Prefix + "External:UserValue";

    /// <summary>
    /// Environment variable name for password.
    /// </summary>
    public const string ExternalPasswordEnv = Prefix + "External:PasswordEnv";

    /// <summary>
    /// Literal value for password (not recommended for production).
    /// </summary>
    public const string ExternalPasswordValue = Prefix + "External:PasswordValue";

    /// <summary>
    /// Connection profile name from IConfiguration (ExternalConnections section).
    /// </summary>
    public const string ExternalConnectionProfile = Prefix + "External:ConnectionProfile";

    #endregion

    #region MySQL External Options

    /// <summary>
    /// Flag to use REPLACE INTO instead of INSERT INTO for MySQL.
    /// </summary>
    public const string ExternalMySqlReplaceQuery = Prefix + "External:MySql:ReplaceQuery";

    /// <summary>
    /// ON DUPLICATE KEY clause for MySQL inserts.
    /// </summary>
    public const string ExternalMySqlOnDuplicateClause = Prefix + "External:MySql:OnDuplicateClause";

    #endregion

    #region ODBC External Options

    /// <summary>
    /// Environment variable name for ODBC DSN.
    /// </summary>
    public const string ExternalOdbcDsnEnv = Prefix + "External:Odbc:DsnEnv";

    /// <summary>
    /// Literal value for ODBC DSN (not recommended for production).
    /// </summary>
    public const string ExternalOdbcDsnValue = Prefix + "External:Odbc:DsnValue";

    #endregion

    #region Redis External Options

    /// <summary>
    /// The key column name for Redis (becomes the Redis key).
    /// </summary>
    public const string ExternalRedisKeyColumn = Prefix + "External:Redis:KeyColumn";

    /// <summary>
    /// Explicit structure definition for Redis (e.g., "id UInt64, name String").
    /// </summary>
    public const string ExternalRedisStructure = Prefix + "External:Redis:Structure";

    /// <summary>
    /// Redis database index (0-15).
    /// </summary>
    public const string ExternalRedisDbIndex = Prefix + "External:Redis:DbIndex";

    /// <summary>
    /// Redis connection pool size.
    /// </summary>
    public const string ExternalRedisPoolSize = Prefix + "External:Redis:PoolSize";

    #endregion

    #region Projections

    /// <summary>
    /// List of projection definitions for this entity.
    /// Value type: List&lt;ProjectionDefinition&gt;
    /// </summary>
    public const string Projections = Prefix + "Projections";

    #endregion

    #region Compression Codecs

    /// <summary>
    /// Compression codec specification for a column.
    /// Value is the codec string (e.g., "DoubleDelta, LZ4" or "ZSTD(9)").
    /// </summary>
    public const string CompressionCodec = Prefix + "CompressionCodec";

    #endregion
}
