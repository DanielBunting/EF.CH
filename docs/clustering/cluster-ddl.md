# Cluster DDL

When DDL statements (CREATE TABLE, ALTER TABLE, DROP TABLE) include an `ON CLUSTER` clause, ClickHouse distributes the operation to all nodes in the specified cluster. EF.CH supports `ON CLUSTER` at multiple configuration levels.

## Entity-Level Cluster

Assign a cluster to a specific entity using `UseCluster` on the entity type builder.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
    entity.UseCluster("my_cluster");
});
```

Generated DDL:

```sql
CREATE TABLE "Order" ON CLUSTER 'my_cluster'
(
    "Id" UInt64,
    "OrderDate" DateTime64(3),
    "Amount" Decimal(18,4)
)
ENGINE = MergeTree()
ORDER BY ("OrderDate", "Id");
```

## Options-Level Default Cluster

Set a default cluster for all entities at the options level. Individual entity `UseCluster` calls override this default.

```csharp
options.UseClickHouse("Host=localhost", o => o
    .UseCluster("default_cluster")
);
```

All entities without an explicit `UseCluster` call will use `default_cluster` for their DDL operations.

## Replicated Engine Chaining

When using replicated engines, `WithCluster` is available directly on the engine builder.

```csharp
entity.UseReplicatedMergeTree(x => x.Id)
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

This is equivalent to calling `entity.UseCluster("my_cluster")` separately.

## Table Groups

Table groups provide logical grouping of entities that share cluster, replication, and connection settings. This is useful when different entities need different cluster configurations.

### Defining Table Groups

Configure table groups at the options level:

```csharp
options.UseClickHouse("Host=localhost", o => o
    .AddTableGroup("Core", group => group
        .UseCluster("geo_cluster")
        .Replicated()
        .Description("Core business tables replicated across all DCs"))
    .AddTableGroup("LocalCache", group => group
        .NoCluster()
        .NotReplicated()
        .Description("Local lookup tables, not replicated"))
    .DefaultTableGroup("Core")
);
```

### Assigning Entities to Table Groups

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.UseReplicatedMergeTree(x => new { x.OrderDate, x.Id })
        .WithTableGroup("Core");
});

modelBuilder.Entity<LocalCache>(entity =>
{
    entity.UseMergeTree(x => x.Key);
    entity.UseTableGroup("LocalCache");
});
```

Entities assigned to the `Core` table group inherit the `geo_cluster` cluster and replicated engine settings. Entities in `LocalCache` have no cluster DDL and use non-replicated engines.

### Default Table Group

The `DefaultTableGroup` setting applies to entities that do not explicitly call `WithTableGroup` or `UseTableGroup`.

```csharp
.DefaultTableGroup("Core")
```

## IsLocalOnly

Mark an entity as local-only to exclude it from cluster DDL entirely, regardless of any default cluster configuration.

```csharp
modelBuilder.Entity<TempMetrics>(entity =>
{
    entity.UseMergeTree(x => x.Timestamp);
    entity.IsLocalOnly();
});
```

This entity's DDL will never include an `ON CLUSTER` clause, even if a default cluster is configured at the options level.

## Configuration from appsettings.json

Table groups and clusters can be defined in `appsettings.json` and loaded at startup.

```json
{
    "ClickHouse": {
        "Connections": {
            "Primary": {
                "WriteEndpoint": "dc1-clickhouse:8123",
                "ReadEndpoints": ["dc2-clickhouse:8123", "dc1-clickhouse:8123"],
                "Database": "production"
            }
        },
        "Clusters": {
            "geo_cluster": {
                "Connection": "Primary",
                "Replication": {
                    "ZooKeeperBasePath": "/clickhouse/geo/{database}"
                }
            }
        },
        "TableGroups": {
            "Core": {
                "Cluster": "geo_cluster",
                "Replicated": true
            },
            "LocalCache": {
                "Replicated": false
            }
        },
        "Defaults": {
            "TableGroup": "Core"
        }
    }
}
```

```csharp
options.UseClickHouse("Host=localhost", o => o
    .FromConfiguration(config.GetSection("ClickHouse"))
);
```

## Precedence Rules

Cluster assignment is resolved in this order (first match wins):

1. Entity-level `UseCluster("name")` or `.WithCluster("name")`
2. Table group assignment via `UseTableGroup("name")` or `.WithTableGroup("name")`
3. Default table group via `DefaultTableGroup("name")`
4. Options-level `UseCluster("name")`
5. `IsLocalOnly()` -- suppresses all cluster DDL for that entity

## See Also

- [Clustering Overview](overview.md) -- architecture and decision guide
- [Replicated Engines](replicated-engines.md) -- engine configuration for replication
- [Connection Routing](connection-routing.md) -- read/write endpoint splitting
