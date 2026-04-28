# Replicated Engines

Replicated engines synchronize data across multiple ClickHouse nodes using ZooKeeper or ClickHouse Keeper for coordination. EF.CH treats replication as a *property* of the engine, not a separate engine type — call `WithReplication(...)` on any `Use*MergeTree` builder and EF.CH emits the matching `Replicated*` engine variant at SQL-generation time.

## Engine Variants

Each MergeTree-family builder gains a `Replicated*` form when you call `WithReplication`. Engine-specific knobs (`WithVersion`, `WithSign`, `WithIsDeleted`) compose with replication and cluster knobs in any order.

### ReplicatedMergeTree

```csharp
entity.UseMergeTree(x => x.Id)
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
entity.UseReplacingMergeTree(x => x.Id)
    .WithVersion(x => x.Version)
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
entity.UseSummingMergeTree(x => new { x.Hour, x.Category })
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
entity.UseAggregatingMergeTree(x => x.Timestamp)
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
entity.UseCollapsingMergeTree(x => new { x.UserId })
    .WithSign(x => x.Sign)
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
entity.UseVersionedCollapsingMergeTree(x => new { x.UserId })
    .WithSign(x => x.Sign)
    .WithVersion(x => x.Version)
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

`WithReplication`, `WithCluster`, and `WithTableGroup` come from the shared `MergeTreeFamilyBuilder` base, so they are available on every typed builder. Each chained call returns the leaf builder, so engine-specific knobs remain reachable in any order; the builder also implicitly converts back to `EntityTypeBuilder<T>` so you can keep configuring the entity without an explicit `.And()`.

```csharp
entity.UseMergeTree(x => x.Id)
    .WithCluster("geo_cluster")
    .WithReplication("/clickhouse/geo/{database}/{table}")
    .WithTableGroup("Core")
    .And()  // or just continue chaining via implicit conversion
    .HasPartitionBy(x => x.OrderDate, PartitionGranularity.Month)
    .HasTtl(x => x.OrderDate, TimeSpan.FromDays(365));
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
            entity.UseMergeTree(x => new { x.Timestamp, x.UserId })
                .WithCluster("analytics_cluster")
                .WithReplication("/clickhouse/analytics/{database}/{table}")
                .WithTableGroup("Core");

            entity.HasPartitionBy(x => x.Timestamp, PartitionGranularity.Month);
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
