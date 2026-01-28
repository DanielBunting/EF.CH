# Table Engines Overview

ClickHouse requires every table to have an engine that determines how data is stored, indexed, and merged. The MergeTree family is used for nearly all production workloads.

## Engine Decision Tree

```
What's your use case?
│
├─ Append-only data (logs, events, metrics)
│  └─ Use MergeTree
│
├─ Need to "update" rows by key
│  └─ Use ReplacingMergeTree
│
├─ Pre-aggregate numeric columns
│  ├─ Simple sums → SummingMergeTree
│  └─ Complex aggregates → AggregatingMergeTree
│
└─ Track state changes (+1/-1 pattern)
   ├─ Ordered inserts → CollapsingMergeTree
   └─ Out-of-order inserts → VersionedCollapsingMergeTree
```

## Quick Reference

| Engine | Use Case | EF.CH Configuration |
|--------|----------|---------------------|
| [MergeTree](mergetree.md) | General purpose, append-only | `entity.UseMergeTree(x => x.Key)` |
| [ReplacingMergeTree](replacing-mergetree.md) | Deduplication by key | `entity.UseReplacingMergeTree(x => x.Version, x => x.Key)` |
| [SummingMergeTree](summing-mergetree.md) | Auto-sum numeric columns | `entity.UseSummingMergeTree(x => x.Key)` |
| [AggregatingMergeTree](aggregating-mergetree.md) | Store aggregate state | `entity.UseAggregatingMergeTree(x => x.Key)` |
| [CollapsingMergeTree](collapsing-mergetree.md) | State with sign column | `entity.UseCollapsingMergeTree(x => x.Sign, x => x.Key)` |
| [VersionedCollapsingMergeTree](versioned-collapsing.md) | Out-of-order collapsing | `entity.UseVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => x.Key)` |
| [Null](null.md) | Discard data (MV source) | `entity.UseNullEngine()` |

## Common Configuration

All engines share these configuration options:

### ORDER BY (Required)

Defines the sort order and primary index:

```csharp
// Single column
entity.UseMergeTree(x => x.Id);

// Multiple columns (composite key)
entity.UseMergeTree(x => new { x.Timestamp, x.UserId, x.Id });
```

**Best Practices:**
- Put frequently-filtered columns first
- Put high-cardinality columns last
- 2-4 columns is typical

### PARTITION BY (Optional)

Divides data into partitions for efficient management:

```csharp
// By month (most common for time-series)
entity.HasPartitionByMonth(x => x.CreatedAt);

// By day (high-volume data)
entity.HasPartitionByDay(x => x.EventDate);

// By year (low-volume or long-retention data)
entity.HasPartitionByYear(x => x.OrderDate);

// Custom expression
entity.HasPartitionBy("toYYYYMM(\"Timestamp\")");
```

### TTL (Optional)

Automatic data expiration:

```csharp
entity.HasTtl("CreatedAt + INTERVAL 90 DAY");
entity.HasTtl("EventTime + INTERVAL 1 YEAR");
```

### SAMPLE BY (Optional)

Enable sampling for approximate queries:

```csharp
entity.HasSampleBy("intHash32(UserId)");
```

### Engine Settings (Optional)

Fine-tune engine behavior:

```csharp
entity.HasEngineSettings(new Dictionary<string, string>
{
    ["index_granularity"] = "8192",
    ["min_bytes_for_wide_part"] = "10485760"
});
```

## Understanding Merges

All MergeTree engines work by:

1. **Writing**: New data is written to small "parts"
2. **Merging**: Background process combines parts, applying engine logic
3. **Querying**: May see unmerged data unless using FINAL

```
Write → Part 1 ─┐
Write → Part 2 ─┼→ Merge → Larger Part → Eventually one large part
Write → Part 3 ─┘
```

**Key Insight:** Engine-specific logic (deduplication, summing, collapsing) happens during **merges**, not writes. This is why you might see "duplicate" rows before merging completes.

## When to Use FINAL

For engines that deduplicate or collapse:

```csharp
// May return unmerged rows
var users = await context.Users.ToListAsync();

// Forces deduplication (slower, but accurate)
var users = await context.Users.Final().ToListAsync();
```

**Trade-off:**
- Without FINAL: Faster, but may see duplicates
- With FINAL: Slower, but guaranteed accurate

For dashboards showing latest data, often FINAL is necessary. For historical aggregations where duplicates don't matter, skip it.

## Generated DDL

Here's what EF.CH generates for a typical configuration:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
    entity.HasPartitionByMonth(x => x.OrderDate);
    entity.HasTtl("OrderDate + INTERVAL 2 YEAR");
});
```

Generates:

```sql
CREATE TABLE "Orders" (
    "Id" UUID NOT NULL,
    "OrderDate" DateTime64(3) NOT NULL,
    "CustomerId" String NOT NULL,
    "Total" Decimal(18, 4) NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("OrderDate")
ORDER BY ("OrderDate", "Id")
TTL "OrderDate" + INTERVAL 2 YEAR
```

## Replicated Engine Variants

Every MergeTree engine has a `Replicated` variant for multi-node clusters:

| Non-Replicated | Replicated |
|----------------|------------|
| `UseMergeTree` | `UseReplicatedMergeTree` |
| `UseReplacingMergeTree` | `UseReplicatedReplacingMergeTree` |
| `UseSummingMergeTree` | `UseReplicatedSummingMergeTree` |
| `UseAggregatingMergeTree` | `UseReplicatedAggregatingMergeTree` |
| `UseCollapsingMergeTree` | `UseReplicatedCollapsingMergeTree` |
| `UseVersionedCollapsingMergeTree` | `UseReplicatedVersionedCollapsingMergeTree` |

Replicated engines use ClickHouse Keeper (or ZooKeeper) to coordinate data across replicas:

```csharp
entity.UseReplicatedMergeTree(x => new { x.Timestamp, x.Id })
      .WithCluster("my_cluster")
      .WithReplication("/clickhouse/tables/{database}/{table}");
```

This generates:

```sql
CREATE TABLE "Events" ON CLUSTER my_cluster
(
    ...
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/mydb/Events', '{replica}')
ORDER BY ("Timestamp", "Id")
```

See [Replicated Engines](../features/replicated-engines.md) for detailed configuration and examples.

## See Also

- [MergeTree](mergetree.md) - Basic append-only engine
- [ReplacingMergeTree](replacing-mergetree.md) - Deduplication
- [SummingMergeTree](summing-mergetree.md) - Auto-aggregation
- [AggregatingMergeTree](aggregating-mergetree.md) - Complex aggregates
- [CollapsingMergeTree](collapsing-mergetree.md) - State tracking
- [Null](null.md) - Discard data (MV source tables)
- [Replicated Engines](../features/replicated-engines.md) - Cluster replication
- [Clustering](../features/clustering.md) - Multi-datacenter setup
- [ClickHouse Official Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family)
