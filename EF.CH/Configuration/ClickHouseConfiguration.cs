namespace EF.CH.Configuration;

/// <summary>
/// Root configuration object for multi-datacenter ClickHouse setup.
/// Can be bound from appsettings.json via IConfiguration.
/// </summary>
public class ClickHouseConfiguration
{
    /// <summary>
    /// Named connection configurations, each defining write/read endpoints.
    /// </summary>
    public Dictionary<string, ConnectionConfig> Connections { get; set; } = new();

    /// <summary>
    /// Named cluster configurations, each referencing a connection and defining replication settings.
    /// </summary>
    public Dictionary<string, ClusterConfig> Clusters { get; set; } = new();

    /// <summary>
    /// Named table groups that map to clusters and define replication behavior.
    /// </summary>
    public Dictionary<string, TableGroupConfig> TableGroups { get; set; } = new();

    /// <summary>
    /// Default settings applied when entities don't specify explicit configuration.
    /// </summary>
    public DefaultsConfig Defaults { get; set; } = new();
}

/// <summary>
/// Configuration for a single ClickHouse connection with read/write endpoint separation.
/// </summary>
public class ConnectionConfig
{
    /// <summary>
    /// The database name to connect to.
    /// </summary>
    public string Database { get; set; } = "default";

    /// <summary>
    /// The primary endpoint for write operations (INSERT, ALTER, etc.).
    /// Format: "host:port" (e.g., "dc1-clickhouse.example.com:8123").
    /// </summary>
    public string WriteEndpoint { get; set; } = null!;

    /// <summary>
    /// Ordered list of endpoints for read operations (SELECT).
    /// The first healthy endpoint is preferred when using PreferFirst strategy.
    /// </summary>
    public List<string> ReadEndpoints { get; set; } = new();

    /// <summary>
    /// Strategy for selecting read endpoints.
    /// </summary>
    public ReadStrategy ReadStrategy { get; set; } = ReadStrategy.PreferFirst;

    /// <summary>
    /// Failover configuration for connection resilience.
    /// </summary>
    public FailoverConfig Failover { get; set; } = new();

    /// <summary>
    /// Optional username for authentication.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Optional password for authentication.
    /// </summary>
    public string? Password { get; set; }
}

/// <summary>
/// Strategy for selecting which read endpoint to use.
/// </summary>
public enum ReadStrategy
{
    /// <summary>
    /// Try the first endpoint in the list, failover to others if unhealthy.
    /// </summary>
    PreferFirst,

    /// <summary>
    /// Rotate through all healthy endpoints in order.
    /// </summary>
    RoundRobin,

    /// <summary>
    /// Randomly select from available healthy endpoints.
    /// </summary>
    Random
}

/// <summary>
/// Configuration for connection failover behavior.
/// </summary>
public class FailoverConfig
{
    /// <summary>
    /// Whether failover is enabled. When false, connection failures are not retried.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Maximum number of retry attempts before giving up.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Delay in milliseconds between retry attempts.
    /// </summary>
    public int RetryDelayMs { get; set; } = 1000;

    /// <summary>
    /// Interval in milliseconds between background health checks.
    /// </summary>
    public int HealthCheckIntervalMs { get; set; } = 30000;
}

/// <summary>
/// Configuration for a ClickHouse cluster.
/// </summary>
public class ClusterConfig
{
    /// <summary>
    /// The name of the connection configuration to use for this cluster.
    /// Must match a key in <see cref="ClickHouseConfiguration.Connections"/>.
    /// </summary>
    public string Connection { get; set; } = null!;

    /// <summary>
    /// Replication settings for this cluster.
    /// </summary>
    public ReplicationConfig Replication { get; set; } = new();
}

/// <summary>
/// Configuration for ClickHouse replication via ZooKeeper/Keeper.
/// </summary>
public class ReplicationConfig
{
    /// <summary>
    /// Base path in ZooKeeper/Keeper for storing replication metadata.
    /// Supports placeholders: {database}, {table}, {uuid}.
    /// Default: "/clickhouse/tables/{uuid}"
    /// </summary>
    public string ZooKeeperBasePath { get; set; } = "/clickhouse/tables/{uuid}";

    /// <summary>
    /// Macro for replica name. Usually "{replica}" which ClickHouse resolves from server macros.
    /// </summary>
    public string ReplicaNameMacro { get; set; } = "{replica}";
}

/// <summary>
/// Configuration for a table group - a logical grouping of tables with shared cluster/replication settings.
/// </summary>
public class TableGroupConfig
{
    /// <summary>
    /// The cluster name to use for tables in this group.
    /// Null means tables are local (not replicated).
    /// Must match a key in <see cref="ClickHouseConfiguration.Clusters"/>.
    /// </summary>
    public string? Cluster { get; set; }

    /// <summary>
    /// Whether tables in this group should use replicated engines.
    /// When true and Cluster is set, engines like MergeTree become ReplicatedMergeTree.
    /// </summary>
    public bool Replicated { get; set; } = true;

    /// <summary>
    /// Human-readable description of this table group's purpose.
    /// </summary>
    public string? Description { get; set; }
}

/// <summary>
/// Default configuration settings.
/// </summary>
public class DefaultsConfig
{
    /// <summary>
    /// The default table group for entities that don't specify one explicitly.
    /// </summary>
    public string TableGroup { get; set; } = "Core";

    /// <summary>
    /// The cluster to use for the migrations history table.
    /// </summary>
    public string? MigrationsHistoryCluster { get; set; }

    /// <summary>
    /// Whether the migrations history table should use a replicated engine.
    /// </summary>
    public bool ReplicateMigrationsHistory { get; set; } = true;
}
