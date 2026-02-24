# Replicated Engines

Replicated engines synchronize data across multiple ClickHouse nodes using ZooKeeper or ClickHouse Keeper for coordination. EF.CH supports all six replicated MergeTree variants with fluent API configuration for cluster assignment, ZooKeeper paths, and replica naming.

## Engine Variants

Each replicated engine mirrors its non-replicated counterpart, adding multi-node replication.

### ReplicatedMergeTree

```csharp
entity.UseReplicatedMergeTree(x => x.Id)
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

Generated DDL:

```sql
CREATE TABLE "Events" ON CLUSTER 'my_cluster'
(
    "Id" UInt64,
    "Name" String,
    "Timestamp" DateTime64(3)
)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{table}', '{replica}')
ORDER BY ("Id");
```

### ReplicatedReplacingMergeTree

```csharp
entity.UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id)
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

```sql
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{database}/{table}', '{replica}', "Version"
)
ORDER BY ("Id");
```

### ReplicatedSummingMergeTree

```csharp
entity.UseReplicatedSummingMergeTree(x => new { x.Hour, x.Category })
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

```sql
ENGINE = ReplicatedSummingMergeTree(
    '/clickhouse/{database}/{table}', '{replica}'
)
ORDER BY ("Hour", "Category");
```

### ReplicatedAggregatingMergeTree

```csharp
entity.UseReplicatedAggregatingMergeTree(x => x.Timestamp)
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

```sql
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/{database}/{table}', '{replica}'
)
ORDER BY ("Timestamp");
```

### ReplicatedCollapsingMergeTree

```csharp
entity.UseReplicatedCollapsingMergeTree(x => x.Sign, x => new { x.UserId })
    .WithCluster("my_cluster")
    .WithReplication("/clickhouse/{database}/{table}");
```

```sql
ENGINE = ReplicatedCollapsingMergeTree(
    '/clickhouse/{database}/{table}', '{replica}', "Sign"
)
ORDER BY ("UserId");
```

### ReplicatedVersionedCollapsingMergeTree

```csharp
entity.UseReplicatedVersionedCollapsingMergeTree(
    x => x.Sign, x => x.Version, x => new { x.UserId }
)
.WithCluster("my_cluster")
.WithReplication("/clickhouse/{database}/{table}");
```

```sql
ENGINE = ReplicatedVersionedCollapsingMergeTree(
    '/clickhouse/{database}/{table}', '{replica}', "Sign", "Version"
)
ORDER BY ("UserId");
```

## ZooKeeper / Keeper Paths

The first argument to a replicated engine is the ZooKeeper path where replication metadata is stored. ClickHouse supports placeholders that are expanded at table creation time.

| Placeholder | Expanded To |
|-------------|-------------|
| `{database}` | The current database name |
| `{table}` | The table name |
| `{uuid}` | A unique identifier for the table |

Common path patterns:

```csharp
// Per-database, per-table path (recommended)
.WithReplication("/clickhouse/{database}/{table}")

// With a prefix for multi-tenant environments
.WithReplication("/clickhouse/geo/{database}/{table}")

// UUID-based (avoids rename conflicts)
.WithReplication("/clickhouse/tables/{uuid}")
```

> **Note:** All replicas of the same table must use the same ZooKeeper path. The path uniquely identifies a replicated table group across the cluster.

## Replica Naming

The second argument is the replica name. It defaults to `{replica}`, which is expanded from the ClickHouse server macro configured in `config.xml`:

```xml
<macros>
    <replica>clickhouse-node-1</replica>
</macros>
```

To override the default:

```csharp
.WithReplication("/clickhouse/{database}/{table}", "custom-replica-name")
```

## Fluent Chaining

The replicated engine builder supports fluent chaining and implicit conversion back to `EntityTypeBuilder<T>`, allowing you to continue configuring the entity.

```csharp
entity.UseReplicatedMergeTree(x => x.Id)
    .WithCluster("geo_cluster")
    .WithReplication("/clickhouse/geo/{database}/{table}")
    .WithTableGroup("Core")
    .And()  // or just continue chaining
    .HasPartitionByMonth(x => x.OrderDate)
    .HasTtl(x => x.OrderDate, TimeSpan.FromDays(365));
```

The `.And()` call explicitly returns the `EntityTypeBuilder<TEntity>`, but implicit conversion also works:

```csharp
// Implicit conversion -- no .And() needed
entity.UseReplicatedMergeTree(x => x.Id)
    .WithCluster("geo_cluster")
    .HasPartitionByMonth(x => x.OrderDate);
```

## Full Example

```csharp
public class AnalyticsDbContext : DbContext
{
    public DbSet<PageView> PageViews { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.UseReplicatedMergeTree(x => new { x.Timestamp, x.UserId })
                .WithCluster("analytics_cluster")
                .WithReplication("/clickhouse/analytics/{database}/{table}")
                .WithTableGroup("Core");

            entity.HasPartitionByMonth(x => x.Timestamp);
            entity.HasTtl(x => x.Timestamp, TimeSpan.FromDays(90));

            entity.HasIndex(x => x.UserId)
                .UseBloomFilter();
        });
    }
}
```

## See Also

- [Clustering Overview](overview.md) -- architecture and decision guide
- [Cluster DDL](cluster-ddl.md) -- ON CLUSTER and table groups
- [Connection Routing](connection-routing.md) -- read/write splitting
