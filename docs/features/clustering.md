# Clustering and Replication

EF.CH provides comprehensive support for multi-node ClickHouse clusters with data replication, connection routing, and coordinated DDL operations.

## Architecture

A typical ClickHouse cluster with EF.CH looks like this:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          ClickHouse Cluster                               │
│                                                                          │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │
│   │   Node 1    │◄──►│   Node 2    │◄──►│   Node 3    │                 │
│   │  (replica)  │    │  (replica)  │    │  (replica)  │                 │
│   │             │    │             │    │             │                 │
│   │   Keeper    │    │   Keeper    │    │   Keeper    │                 │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                 │
│          │                  │                  │                         │
│          └──────────────────┴──────────────────┘                         │
│                    Raft Consensus (Keeper)                               │
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                     EF.CH Application                            │   │
│   │                                                                  │   │
│   │   Write Endpoint ──────► Node 1                                  │   │
│   │   Read Endpoints  ──────► Node 1, Node 2, Node 3                 │   │
│   │                                                                  │   │
│   │   Connection Routing: SELECT → Read, INSERT/ALTER → Write        │   │
│   └─────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Key Components

- **ClickHouse Keeper**: Coordinates replication between nodes (replaces ZooKeeper)
- **Replicated Engines**: MergeTree variants that sync data across replicas
- **ON CLUSTER DDL**: Schema changes execute on all cluster nodes automatically
- **Connection Routing**: Directs reads and writes to appropriate endpoints

## Quick Start

The simplest cluster setup requires three steps:

```csharp
// 1. Configure cluster-wide settings
services.AddDbContext<MyDbContext>(options =>
    options.UseClickHouse("Host=node1;Database=myapp", ch =>
    {
        ch.UseCluster("my_cluster");  // Default cluster for all entities
    }));

// 2. Use replicated engines in OnModelCreating
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.Entity<Order>(entity =>
    {
        entity.UseReplicatedMergeTree(x => new { x.OrderDate, x.Id })
              .WithCluster("my_cluster")
              .WithReplication("/clickhouse/tables/{database}/{table}");
    });
}
```

This generates DDL with ON CLUSTER and ReplicatedMergeTree:

```sql
CREATE TABLE "Orders" ON CLUSTER my_cluster
(
    "Id" UUID NOT NULL,
    "OrderDate" DateTime64(3) NOT NULL,
    ...
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/Orders', '{replica}')
ORDER BY ("OrderDate", "Id")
```

See [ClusterSample](../../samples/ClusterSample/) for a complete working example with Docker Compose.

## Configuration Approaches

EF.CH supports three configuration approaches that can be combined:

### 1. Fluent API (Recommended)

Configure everything in code with full IntelliSense support:

```csharp
options.UseClickHouse("Host=localhost;Database=myapp", ch =>
{
    // Define connections
    ch.AddConnection("Primary", conn => conn
        .Database("myapp")
        .WriteEndpoint("dc1-clickhouse:8123")
        .ReadEndpoints("dc1-clickhouse:8123", "dc2-clickhouse:8123")
        .ReadStrategy(ReadStrategy.RoundRobin)
        .WithFailover(f => f
            .MaxRetries(3)
            .RetryDelayMs(1000)));

    // Define clusters
    ch.AddCluster("geo_cluster", cluster => cluster
        .UseConnection("Primary")
        .WithReplication(r => r
            .ZooKeeperBasePath("/clickhouse/tables/{database}/{table}")
            .ReplicaNameMacro("{replica}")));

    // Define table groups
    ch.AddTableGroup("Core", group => group
        .UseCluster("geo_cluster")
        .Replicated()
        .Description("Core business tables"));

    ch.AddTableGroup("Staging", group => group
        .NoCluster()
        .NotReplicated()
        .Description("Local staging tables"));

    // Set defaults
    ch.DefaultTableGroup("Core");
    ch.UseConnectionRouting();  // Enable read/write splitting
});
```

### 2. JSON Configuration

Configure via `appsettings.json` for environment-specific settings:

```json
{
  "ClickHouse": {
    "Connections": {
      "Primary": {
        "Database": "myapp",
        "WriteEndpoint": "dc1-clickhouse:8123",
        "ReadEndpoints": ["dc1-clickhouse:8123", "dc2-clickhouse:8123"],
        "ReadStrategy": "RoundRobin",
        "Failover": {
          "Enabled": true,
          "MaxRetries": 3,
          "RetryDelayMs": 1000,
          "HealthCheckIntervalMs": 30000
        },
        "Username": "default",
        "Password": ""
      }
    },
    "Clusters": {
      "geo_cluster": {
        "Connection": "Primary",
        "Replication": {
          "ZooKeeperBasePath": "/clickhouse/tables/{database}/{table}",
          "ReplicaNameMacro": "{replica}"
        }
      }
    },
    "TableGroups": {
      "Core": {
        "Cluster": "geo_cluster",
        "Replicated": true,
        "Description": "Core business tables"
      },
      "Staging": {
        "Cluster": null,
        "Replicated": false,
        "Description": "Local staging tables"
      }
    },
    "Defaults": {
      "TableGroup": "Core",
      "MigrationsHistoryCluster": "geo_cluster",
      "ReplicateMigrationsHistory": true
    }
  }
}
```

Load the configuration:

```csharp
options.UseClickHouse("...", ch =>
{
    ch.FromConfiguration(configuration.GetSection("ClickHouse"));
});
```

### 3. Direct Configuration Object

Build the configuration object manually:

```csharp
var config = new ClickHouseConfiguration
{
    Connections = new Dictionary<string, ConnectionConfig>
    {
        ["Primary"] = new ConnectionConfig
        {
            Database = "myapp",
            WriteEndpoint = "dc1:8123",
            ReadEndpoints = new List<string> { "dc1:8123", "dc2:8123" },
            ReadStrategy = ReadStrategy.RoundRobin
        }
    },
    Clusters = new Dictionary<string, ClusterConfig>
    {
        ["geo_cluster"] = new ClusterConfig
        {
            Connection = "Primary",
            Replication = new ReplicationConfig
            {
                ZooKeeperBasePath = "/clickhouse/tables/{database}/{table}"
            }
        }
    }
};

options.UseClickHouse("...", ch => ch.WithConfiguration(config));
```

## Table Groups

Table groups provide logical grouping of entities that share cluster and replication settings:

```csharp
// Define table groups
ch.AddTableGroup("Core", group => group
    .UseCluster("geo_cluster")
    .Replicated());

ch.AddTableGroup("Analytics", group => group
    .UseCluster("analytics_cluster")
    .Replicated());

ch.AddTableGroup("Local", group => group
    .NoCluster()        // No ON CLUSTER clause
    .NotReplicated());  // Use non-replicated engines

// Assign entities to groups
modelBuilder.Entity<Order>(entity =>
{
    entity.UseReplicatedMergeTree(x => x.Id)
          .WithTableGroup("Core");  // Inherits geo_cluster settings
});

modelBuilder.Entity<ClickEvent>(entity =>
{
    entity.UseReplicatedMergeTree(x => x.Timestamp)
          .WithTableGroup("Analytics");  // Inherits analytics_cluster settings
});

modelBuilder.Entity<TempImport>(entity =>
{
    entity.UseMergeTree(x => x.Id)  // Non-replicated engine
          .WithTableGroup("Local");  // Local-only, no cluster DDL
});
```

### Default Table Group

Set a default group for entities without explicit assignment:

```csharp
ch.DefaultTableGroup("Core");
```

Entities without `.WithTableGroup()` will use the default group's settings.

## Entity-Level Configuration

### UseCluster

Assign an entity to a specific cluster for DDL operations:

```csharp
// Explicit cluster assignment (overrides table group)
entity.UseReplicatedMergeTree(x => x.Id)
      .WithCluster("special_cluster");

// Or using the standalone method
entity.UseReplicatedMergeTree(x => x.Id);
entity.UseCluster("special_cluster");
```

### HasReplication

Configure the ZooKeeper/Keeper path for a replicated table:

```csharp
entity.UseReplicatedMergeTree(x => x.Id)
      .WithReplication(
          zooKeeperPath: "/clickhouse/tables/{database}/{table}",
          replicaName: "{replica}");
```

**Path Placeholders:**

| Placeholder | Description |
|-------------|-------------|
| `{database}` | Expands to the database name |
| `{table}` | Expands to the table name |
| `{uuid}` | Expands to a UUID (useful for unique paths) |
| `{replica}` | Expands to the replica name macro (set in ClickHouse config) |

### IsLocalOnly

Mark an entity as local-only, preventing ON CLUSTER DDL:

```csharp
entity.UseMergeTree(x => x.Id)
      .IsLocalOnly();  // DDL executes only on connected node
```

## Generated DDL

### Single-Node (No Cluster)

```csharp
entity.UseMergeTree(x => new { x.Date, x.Id });
```

```sql
CREATE TABLE "Orders"
(
    ...
)
ENGINE = MergeTree
ORDER BY ("Date", "Id")
```

### Cluster with Replicated Engine

```csharp
entity.UseReplicatedMergeTree(x => new { x.Date, x.Id })
      .WithCluster("my_cluster")
      .WithReplication("/clickhouse/tables/{database}/{table}");
```

```sql
CREATE TABLE "Orders" ON CLUSTER my_cluster
(
    ...
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/mydb/Orders', '{replica}')
ORDER BY ("Date", "Id")
```

### ReplicatedReplacingMergeTree with Version

```csharp
entity.UseReplicatedReplacingMergeTree(x => x.Version, x => new { x.Date, x.Id })
      .WithCluster("my_cluster")
      .WithReplication("/clickhouse/tables/{database}/{table}");
```

```sql
CREATE TABLE "Orders" ON CLUSTER my_cluster
(
    ...
    "Version" UInt64 NOT NULL
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/mydb/Orders', '{replica}', "Version")
ORDER BY ("Date", "Id")
```

## ClickHouse Server Setup

For EF.CH clustering to work, your ClickHouse servers must be configured properly.

### Cluster Definition (config.xml)

```xml
<clickhouse>
    <remote_servers>
        <my_cluster>
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>clickhouse1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse2</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse3</host>
                    <port>9000</port>
                </replica>
            </shard>
        </my_cluster>
    </remote_servers>
</clickhouse>
```

### ClickHouse Keeper (or ZooKeeper)

```xml
<clickhouse>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
        </coordination_settings>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>clickhouse1</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>2</id>
                <hostname>clickhouse2</hostname>
                <port>9234</port>
            </server>
            <server>
                <id>3</id>
                <hostname>clickhouse3</hostname>
                <port>9234</port>
            </server>
        </raft_configuration>
    </keeper_server>

    <zookeeper>
        <node>
            <host>clickhouse1</host>
            <port>9181</port>
        </node>
        <node>
            <host>clickhouse2</host>
            <port>9181</port>
        </node>
        <node>
            <host>clickhouse3</host>
            <port>9181</port>
        </node>
    </zookeeper>
</clickhouse>
```

### Macros

Each node needs a unique replica name:

```xml
<!-- On clickhouse1 -->
<macros>
    <replica>clickhouse1</replica>
    <shard>01</shard>
</macros>

<!-- On clickhouse2 -->
<macros>
    <replica>clickhouse2</replica>
    <shard>01</shard>
</macros>
```

The `{replica}` placeholder in EF.CH expands to this macro value.

## Migrations with Clusters

EF Core migrations work with clusters. The `__EFMigrationsHistory` table can also be replicated:

```csharp
ch.AddTableGroup("Migrations", group => group
    .UseCluster("my_cluster")
    .Replicated());

// In Defaults configuration
ch.WithConfiguration(new ClickHouseConfiguration
{
    Defaults = new DefaultsConfig
    {
        MigrationsHistoryCluster = "my_cluster",
        ReplicateMigrationsHistory = true
    }
});
```

## Limitations

- **Eventual Consistency**: Replication is asynchronous. Queries immediately after writes may return stale data.
- **No Distributed Transactions**: Each node operates independently. Cross-node atomicity is not guaranteed.
- **Keeper Quorum**: Requires majority of Keeper nodes to be available (2/3, 3/5, etc.).
- **No Automatic Failover Promotion**: EF.CH doesn't promote read replicas to write. Configure this at the infrastructure level.

## See Also

- [Replicated Engines](replicated-engines.md) - Detailed guide on replicated MergeTree variants
- [Connection Routing](connection-routing.md) - Read/write splitting and failover
- [ClusterSample](../../samples/ClusterSample/) - Complete working example
- [Table Engines Overview](../engines/overview.md) - Non-replicated engine comparison
