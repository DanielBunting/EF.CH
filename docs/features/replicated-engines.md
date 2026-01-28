# Replicated MergeTree Engines

Every MergeTree engine has a `Replicated` variant that synchronizes data across cluster nodes using ClickHouse Keeper (or ZooKeeper).

## Why Replicated Engines?

Non-replicated engines store data only on the node where INSERTs occur:

```
INSERT → Node 1 only
         Node 2 has no data
         Node 3 has no data
```

Replicated engines coordinate through Keeper to ensure all replicas have the same data:

```
INSERT → Node 1 ──┐
                  │ Keeper coordination
         Node 2 ◄─┤
         Node 3 ◄─┘
```

This provides:
- **High Availability**: Data survives node failures
- **Read Scaling**: Queries can hit any replica
- **Consistency**: All replicas converge to the same state

## Supported Engines

| Method | Engine | Additional Parameters |
|--------|--------|----------------------|
| `UseReplicatedMergeTree` | ReplicatedMergeTree | - |
| `UseReplicatedReplacingMergeTree` | ReplicatedReplacingMergeTree | version column |
| `UseReplicatedSummingMergeTree` | ReplicatedSummingMergeTree | - |
| `UseReplicatedAggregatingMergeTree` | ReplicatedAggregatingMergeTree | - |
| `UseReplicatedCollapsingMergeTree` | ReplicatedCollapsingMergeTree | sign column |
| `UseReplicatedVersionedCollapsingMergeTree` | ReplicatedVersionedCollapsingMergeTree | sign + version columns |

## Fluent Chain API

All replicated engine methods return a `ReplicatedEngineBuilder<TEntity>` that enables fluent configuration:

```csharp
entity.UseReplicatedMergeTree(x => x.Id)
      .WithCluster("my_cluster")
      .WithReplication("/clickhouse/tables/{database}/{table}")
      .WithTableGroup("Core")
      .And()  // Explicit conversion back to EntityTypeBuilder
      .HasPartitionByMonth(x => x.CreatedAt);
```

### Available Methods

| Method | Description |
|--------|-------------|
| `WithCluster(string)` | Sets the cluster name for ON CLUSTER DDL |
| `WithReplication(string, string?)` | Sets ZooKeeper path and replica name |
| `WithTableGroup(string)` | Assigns entity to a table group |
| `And()` | Explicitly returns `EntityTypeBuilder<TEntity>` |

### Implicit Conversion

The builder implicitly converts to `EntityTypeBuilder<TEntity>`, so `.And()` is often optional:

```csharp
// These are equivalent
entity.UseReplicatedMergeTree(x => x.Id)
      .WithCluster("my_cluster")
      .And()
      .HasPartitionByMonth(x => x.CreatedAt);

entity.UseReplicatedMergeTree(x => x.Id)
      .WithCluster("my_cluster")
      .HasPartitionByMonth(x => x.CreatedAt);  // Implicit conversion
```

## Examples

### ReplicatedMergeTree

Basic append-only replicated table:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseReplicatedMergeTree(x => new { x.Timestamp, x.Id })
          .WithCluster("events_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}");

    entity.HasPartitionByMonth(x => x.Timestamp);
});
```

Generated DDL:

```sql
CREATE TABLE "Events" ON CLUSTER events_cluster
(
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL,
    "Payload" String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/mydb/Events', '{replica}')
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Id")
```

### ReplicatedReplacingMergeTree

Deduplication by key with version column:

```csharp
modelBuilder.Entity<User>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseReplicatedReplacingMergeTree(
              versionColumnExpression: x => x.Version,
              orderByExpression: x => x.Id)
          .WithCluster("users_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}");
});
```

Generated DDL:

```sql
CREATE TABLE "Users" ON CLUSTER users_cluster
(
    "Id" UUID NOT NULL,
    "Name" String NOT NULL,
    "Email" String NOT NULL,
    "Version" UInt64 NOT NULL
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/mydb/Users', '{replica}', "Version")
ORDER BY ("Id")
```

### ReplicatedSummingMergeTree

Auto-sum numeric columns during merges:

```csharp
modelBuilder.Entity<DailySummary>(entity =>
{
    entity.HasNoKey();
    entity.UseReplicatedSummingMergeTree(x => new { x.Date, x.ProductId })
          .WithCluster("analytics_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}");
});
```

Generated DDL:

```sql
CREATE TABLE "DailySummary" ON CLUSTER analytics_cluster
(
    "Date" Date NOT NULL,
    "ProductId" String NOT NULL,
    "OrderCount" Int64 NOT NULL,
    "Revenue" Decimal(18, 4) NOT NULL
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/mydb/DailySummary', '{replica}')
ORDER BY ("Date", "ProductId")
```

### ReplicatedAggregatingMergeTree

For pre-aggregated data with aggregate function columns:

```csharp
modelBuilder.Entity<HourlyStats>(entity =>
{
    entity.HasNoKey();
    entity.UseReplicatedAggregatingMergeTree(x => new { x.Hour, x.Category })
          .WithCluster("stats_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}");

    entity.Property(e => e.CountState).HasAggregateFunction("count", typeof(ulong));
    entity.Property(e => e.SumState).HasAggregateFunction("sum", typeof(decimal));
});
```

### ReplicatedCollapsingMergeTree

Row cancellation with sign column (+1 insert, -1 cancel):

```csharp
modelBuilder.Entity<UserSession>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseReplicatedCollapsingMergeTree(
              signColumnExpression: x => x.Sign,
              orderByExpression: x => new { x.UserId, x.SessionId })
          .WithCluster("sessions_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}");
});
```

The `Sign` column must be `sbyte` (Int8) with values +1 or -1.

### ReplicatedVersionedCollapsingMergeTree

For out-of-order collapsing with version tracking:

```csharp
modelBuilder.Entity<Inventory>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseReplicatedVersionedCollapsingMergeTree(
              signColumnExpression: x => x.Sign,
              versionColumnExpression: x => x.Version,
              orderByExpression: x => new { x.WarehouseId, x.ProductId })
          .WithCluster("inventory_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}");
});
```

## ZooKeeper Path Placeholders

The `WithReplication()` path supports several placeholders:

| Placeholder | Expands To | Example |
|-------------|-----------|---------|
| `{database}` | Database name | `mydb` |
| `{table}` | Table name | `Orders` |
| `{uuid}` | Unique identifier | `550e8400-e29b-41d4-a716-446655440000` |
| `{replica}` | Replica name macro | `clickhouse1` |

**Common Path Patterns:**

```csharp
// Database and table based (recommended)
.WithReplication("/clickhouse/tables/{database}/{table}")
// Path: /clickhouse/tables/mydb/Orders

// UUID-based (avoids name conflicts on rename)
.WithReplication("/clickhouse/tables/{uuid}")
// Path: /clickhouse/tables/550e8400-e29b-41d4-a716-446655440000

// Custom hierarchy
.WithReplication("/clickhouse/{database}/analytics/{table}")
// Path: /clickhouse/mydb/analytics/Orders
```

### Custom Replica Name

By default, `{replica}` expands to the `replica` macro configured on each ClickHouse node. You can override this:

```csharp
// Use node-specific macro (default)
.WithReplication("/clickhouse/tables/{database}/{table}", "{replica}")

// Use custom replica naming
.WithReplication("/clickhouse/tables/{database}/{table}", "dc1-node1")
```

## Mixing with Standard Configuration

The fluent chain pattern integrates seamlessly with other entity configuration:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);

    // Replicated engine with cluster settings
    entity.UseReplicatedReplacingMergeTree(x => x.Version, x => new { x.OrderDate, x.Id })
          .WithCluster("orders_cluster")
          .WithReplication("/clickhouse/tables/{database}/{table}")
          .WithTableGroup("Core");

    // Standard EF.CH configuration continues
    entity.HasPartitionByMonth(x => x.OrderDate);
    entity.HasTtl("OrderDate + INTERVAL 2 YEAR");

    entity.Property(e => e.Total)
          .HasColumnType("Decimal(18, 2)");

    entity.HasIndex(e => e.CustomerId)
          .UseBloomFilter();
});
```

## Backward Compatibility

You can still use separate method calls instead of the fluent chain:

```csharp
// Fluent chain (recommended)
entity.UseReplicatedMergeTree(x => x.Id)
      .WithCluster("my_cluster")
      .WithReplication("/clickhouse/tables/{database}/{table}");

// Separate calls (equivalent)
entity.UseReplicatedMergeTree(x => x.Id);
entity.UseCluster("my_cluster");
entity.HasReplication("/clickhouse/tables/{database}/{table}");
```

Both approaches produce identical DDL.

## See Also

- [Clustering and Replication](clustering.md) - Full clustering setup guide
- [Connection Routing](connection-routing.md) - Read/write splitting
- [MergeTree](../engines/mergetree.md) - Non-replicated MergeTree details
- [ReplacingMergeTree](../engines/replacing-mergetree.md) - Deduplication patterns
- [AggregatingMergeTree](../engines/aggregating-mergetree.md) - Pre-aggregation
- [CollapsingMergeTree](../engines/collapsing-mergetree.md) - State tracking
