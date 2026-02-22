# Engine Overview

Every ClickHouse table requires a storage engine. The engine determines how data is stored, merged, replicated, and queried. EF.CH supports 14 engine types across three families.

## Decision Flowchart

Start here to choose the right engine for your use case.

```
Do you need to store data?
├── No  ──────────────────────────────────── Null engine
│                                            (materialized view source)
│
└── Yes
    │
    ├── Single node or multi-node?
    │   └── Multi-node  ─────────────────── Distributed engine
    │       (query fan-out)                  (+ local table with a MergeTree variant)
    │
    └── What is your write pattern?
        │
        ├── Append-only events/logs  ─────── MergeTree
        │
        ├── Need latest row per key  ─────── ReplacingMergeTree
        │   (deduplication)
        │
        ├── Need running totals  ──────────── SummingMergeTree
        │   (auto-sum numerics)
        │
        ├── Pre-aggregate for dashboards ─── AggregatingMergeTree
        │   (intermediate states)
        │
        ├── State tracking with cancellation
        │   ├── Ordered inserts  ──────────── CollapsingMergeTree
        │   └── Out-of-order inserts  ─────── VersionedCollapsingMergeTree
        │
        └── Need fault tolerance?
            └── Yes  ─────────────────────── Replicated* variant
                                             (any of the 6 engines above)
```

## Quick Reference

| Engine | Use Case | Key Feature | EF.CH API |
|--------|----------|-------------|-----------|
| **MergeTree** | General analytics, logs, events | Columnar storage with ORDER BY | `UseMergeTree(x => new { x.Col1, x.Col2 })` |
| **ReplacingMergeTree** | Latest state per key | Row deduplication by version | `UseReplacingMergeTree(x => x.Version, x => new { x.Id })` |
| **SummingMergeTree** | Counters, running totals | Auto-sums numeric columns on merge | `UseSummingMergeTree(x => new { x.Hour, x.Category })` |
| **AggregatingMergeTree** | Pre-aggregated dashboards | Stores intermediate aggregate states | `UseAggregatingMergeTree(x => x.Timestamp)` |
| **CollapsingMergeTree** | Mutable state with ordered writes | +1/-1 sign column for state tracking | `UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId })` |
| **VersionedCollapsingMergeTree** | Mutable state with unordered writes | Sign + version for out-of-order collapsing | `UseVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => new { x.UserId })` |
| **ReplicatedMergeTree** | Replicated general analytics | MergeTree + ZooKeeper replication | `UseReplicatedMergeTree(x => x.Id)` |
| **ReplicatedReplacingMergeTree** | Replicated deduplication | ReplacingMergeTree + replication | `UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id)` |
| **ReplicatedSummingMergeTree** | Replicated counters | SummingMergeTree + replication | `UseReplicatedSummingMergeTree(x => new { x.Hour })` |
| **ReplicatedAggregatingMergeTree** | Replicated pre-aggregation | AggregatingMergeTree + replication | `UseReplicatedAggregatingMergeTree(x => x.Timestamp)` |
| **ReplicatedCollapsingMergeTree** | Replicated ordered collapsing | CollapsingMergeTree + replication | `UseReplicatedCollapsingMergeTree(x => x.Sign, x => new { x.UserId })` |
| **ReplicatedVersionedCollapsingMergeTree** | Replicated unordered collapsing | VersionedCollapsingMergeTree + replication | `UseReplicatedVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => new { x.UserId })` |
| **Null** | Materialized view source | Data discarded after insert | `UseNullEngine()` |
| **Distributed** | Cross-shard queries | Fan-out queries to cluster nodes | `UseDistributed("cluster", "local_table")` |

## MergeTree Family (6 Engines)

### MergeTree

The default, general-purpose columnar storage engine. Data is stored sorted by the ORDER BY key, supporting partitioning, sampling, TTL, skip indices, and projections.

**When to use:** Append-only workloads like event logs, time series, clickstreams, and any table where you do not need deduplication or automatic aggregation.

```csharp
entity.UseMergeTree(x => new { x.Timestamp, x.Id })
    .HasPartitionByMonth(x => x.Timestamp);
```

See [MergeTree](mergetree.md) for full configuration options.

### ReplacingMergeTree

Extends MergeTree with row deduplication. During background merges, rows with the same ORDER BY key are collapsed to the row with the highest version value. Use `.Final()` at query time to get the deduplicated view without waiting for merges.

**When to use:** Tables that model mutable state (user profiles, product catalog, configuration) where you want "last write wins" semantics.

```csharp
entity.UseReplacingMergeTree(x => x.Version, x => new { x.UserId });
```

> **Note:** Without `.Final()`, queries may return duplicate rows from unmerged parts. Always use `.Final()` when you need the latest state.

See [ReplacingMergeTree](replacing-mergetree.md) for version columns and isDeleted support.

### SummingMergeTree

During background merges, rows with the same ORDER BY key are collapsed by summing all numeric columns. Non-numeric, non-key columns take the value from the first row encountered.

**When to use:** Counters, metrics, and running totals where you want automatic aggregation at the storage level (e.g., page view counts per URL per hour).

```csharp
entity.UseSummingMergeTree(x => new { x.Hour, x.PageUrl });
```

See [SummingMergeTree](summing-mergetree.md) for numeric column behavior.

### AggregatingMergeTree

Stores intermediate aggregate function states (not final values). Used as the target table for materialized views that pre-compute aggregates. Query with `-Merge` combinators to finalize the aggregation.

**When to use:** Dashboard backing tables where you pre-aggregate data from a source table via materialized views and need to combine partial aggregates across time periods.

```csharp
entity.UseAggregatingMergeTree(x => x.Timestamp);
```

See [AggregatingMergeTree](aggregating-mergetree.md) for State/Merge combinator usage.

### CollapsingMergeTree

Uses a sign column (`+1` for insert, `-1` for cancel) to track state changes. During merges, rows with the same ORDER BY key and opposite signs cancel each other out. Requires inserts to arrive in the correct order: insert the cancellation row before the new state row.

**When to use:** State tracking where you control the insert order (e.g., session state, inventory levels with ordered event processing).

```csharp
entity.UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId });
```

See [CollapsingMergeTree](collapsing-mergetree.md) for the insert protocol.

### VersionedCollapsingMergeTree

Like CollapsingMergeTree, but adds a version column that allows cancellation rows to arrive in any order. During merges, the engine uses the version to correctly pair insert and cancel rows regardless of insertion sequence.

**When to use:** State tracking in distributed systems where inserts may arrive out of order (e.g., event sourcing with multiple producers, replicated clusters).

```csharp
entity.UseVersionedCollapsingMergeTree(
    x => x.Sign,
    x => x.Version,
    x => new { x.UserId }
);
```

See [VersionedCollapsingMergeTree](versioned-collapsing.md) for version-based collapsing.

## Replicated Variants (6 Engines)

Each MergeTree family engine has a replicated counterpart that adds multi-node data replication via ClickHouse Keeper (or ZooKeeper). Replicated engines ensure data is copied to multiple nodes for fault tolerance.

All replicated variants return a `ReplicatedEngineBuilder<T>` that supports chaining with `.WithCluster()`, `.WithReplication()`, and `.WithTableGroup()`.

```csharp
// Basic replicated MergeTree
entity.UseReplicatedMergeTree(x => new { x.Timestamp, x.Id })
    .WithCluster("production_cluster")
    .WithReplication("/clickhouse/{database}/{table}", "{replica}");

// Replicated ReplacingMergeTree
entity.UseReplicatedReplacingMergeTree(x => x.Version, x => new { x.UserId })
    .WithCluster("production_cluster");
```

**When to use:** Any production deployment where you need data redundancy across multiple ClickHouse nodes. Requires ClickHouse Keeper or ZooKeeper for coordination.

> **Note:** The replicated variants have the same merge behavior as their non-replicated counterparts. `ReplicatedReplacingMergeTree` deduplicates the same way as `ReplacingMergeTree`, but across all replicas.

The six replicated engines are:

| Replicated Engine | Base Engine |
|-------------------|-------------|
| ReplicatedMergeTree | MergeTree |
| ReplicatedReplacingMergeTree | ReplacingMergeTree |
| ReplicatedSummingMergeTree | SummingMergeTree |
| ReplicatedAggregatingMergeTree | AggregatingMergeTree |
| ReplicatedCollapsingMergeTree | CollapsingMergeTree |
| ReplicatedVersionedCollapsingMergeTree | VersionedCollapsingMergeTree |

## Specialized Engines (2 Engines)

### Null Engine

Data written to a Null engine table is discarded immediately. The table accepts inserts but stores nothing. This is primarily used as the source table for materialized views, where the view captures and transforms the data before it is thrown away.

**When to use:** Materialized view pipelines where the raw source data is not needed after the view processes it.

```csharp
// Source table: data passes through to the materialized view, then is discarded
entity.UseNullEngine();

// The materialized view captures and transforms the data
targetEntity.AsMaterializedView<DailySummary, RawEvent>(
    query: events => events
        .GroupBy(e => e.EventDate)
        .Select(g => new DailySummary
        {
            Date = g.Key,
            Count = g.Count()
        })
);
```

See [Null Engine](null-engine.md) for materialized view patterns.

### Distributed Engine

The Distributed engine does not store data itself. It acts as a query proxy that fans out queries to local tables on multiple cluster nodes and merges the results. Typically paired with a Replicated MergeTree variant on each shard.

**When to use:** Multi-shard deployments where data is partitioned across nodes and you need a single query interface.

```csharp
entity.UseDistributed("production_cluster", "events_local")
    .WithShardingKey(x => x.UserId)
    .WithPolicy("ssd");
```

The sharding key determines how data is distributed across shards during inserts through the Distributed table.

See [Distributed Engine](distributed.md) for cluster topology and sharding.

## Engine Comparison by Capability

| Capability | MergeTree | Replacing | Summing | Aggregating | Collapsing | VersionedCollapsing | Null | Distributed |
|------------|-----------|-----------|---------|-------------|------------|---------------------|------|-------------|
| Stores data | Yes | Yes | Yes | Yes | Yes | Yes | No | No (proxy) |
| Deduplication | No | Yes | No | No | No | No | N/A | N/A |
| Auto-aggregation | No | No | Sums numerics | Merges states | No | No | N/A | N/A |
| State cancellation | No | No | No | No | Yes (ordered) | Yes (any order) | N/A | N/A |
| PARTITION BY | Yes | Yes | Yes | Yes | Yes | Yes | No | No |
| TTL | Yes | Yes | Yes | Yes | Yes | Yes | No | No |
| Skip indices | Yes | Yes | Yes | Yes | Yes | Yes | No | No |
| Projections | Yes | Yes | Yes | Yes | Yes | Yes | No | No |
| Replication | Via Replicated* | Via Replicated* | Via Replicated* | Via Replicated* | Via Replicated* | Via Replicated* | No | Built-in |

## Common Configuration Shared by All MergeTree Engines

All MergeTree family engines (including replicated variants) support these table-level options:

```csharp
entity.UseMergeTree(x => new { x.Date, x.Id })           // ORDER BY (required)
    .HasPartitionByMonth(x => x.Date)                      // PARTITION BY
    .HasSampleBy(x => x.UserId)                            // SAMPLE BY
    .HasTtl(x => x.Date, TimeSpan.FromDays(90))           // TTL
    .HasEngineSettings(new Dictionary<string, string>       // SETTINGS
    {
        ["index_granularity"] = "4096"
    });
```

See [MergeTree](mergetree.md) for detailed documentation of ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, TTL, and SETTINGS.

## See Also

- [Getting Started](../getting-started.md) -- installation and first project walkthrough
- [ClickHouse for EF Developers](../clickhouse-for-ef-developers.md) -- mental model differences from traditional RDBMS
- [MergeTree](mergetree.md) -- standard columnar storage engine
- [ReplacingMergeTree](replacing-mergetree.md) -- row deduplication by version
- [SummingMergeTree](summing-mergetree.md) -- automatic numeric column summation
- [AggregatingMergeTree](aggregating-mergetree.md) -- intermediate aggregate state storage
- [CollapsingMergeTree](collapsing-mergetree.md) -- state tracking via sign column
- [VersionedCollapsingMergeTree](versioned-collapsing.md) -- out-of-order collapsing
- [Null Engine](null-engine.md) -- write-only, data discarded
- [Distributed Engine](distributed.md) -- cross-cluster query fan-out
