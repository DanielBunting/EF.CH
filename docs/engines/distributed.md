# Distributed Engine

The Distributed engine provides a unified view over data sharded across multiple ClickHouse servers. It does not store data itself -- it acts as a proxy that routes queries to the underlying local tables on each cluster node.

## Basic Configuration

### Two-parameter overload (uses current database)

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseDistributed("my_cluster", "events_local");
});
```

```sql
ENGINE = Distributed('my_cluster', currentDatabase(), 'events_local')
```

### Three-parameter overload (explicit database)

```csharp
entity.UseDistributed("my_cluster", "default", "events_local");
```

```sql
ENGINE = Distributed('my_cluster', 'default', 'events_local')
```

> **Note:** The Distributed engine does not generate ORDER BY, PARTITION BY, or other MergeTree clauses. These belong on the underlying local table, not on the distributed proxy.

## Sharding Key

The sharding key determines which shard receives each row during inserts. Without a sharding key, inserts are distributed randomly.

### Property selector

```csharp
entity.UseDistributed("my_cluster", "events_local")
    .WithShardingKey(x => x.UserId);
```

```sql
ENGINE = Distributed('my_cluster', currentDatabase(), 'events_local', UserId)
```

### Raw expression

Use a raw string for hash-based sharding:

```csharp
entity.UseDistributed("my_cluster", "events_local")
    .WithShardingKey("cityHash64(UserId)");
```

```sql
ENGINE = Distributed('my_cluster', currentDatabase(), 'events_local', cityHash64(UserId))
```

## Storage Policy

The storage policy controls how the Distributed engine buffers data locally before sending it to shards.

```csharp
entity.UseDistributed("my_cluster", "events_local")
    .WithShardingKey(x => x.UserId)
    .WithPolicy("ssd_policy");
```

```sql
ENGINE = Distributed('my_cluster', currentDatabase(), 'events_local', UserId, 'ssd_policy')
```

## Local Table Relationship

The Distributed table must reference an existing local table on each cluster node. Typically you define both:

```csharp
// Local table (exists on each node)
modelBuilder.Entity<EventLocal>(entity =>
{
    entity.ToTable("events_local");
    entity.HasNoKey();

    entity.UseMergeTree(x => new { x.EventTime, x.Id })
        .HasPartitionByMonth(x => x.EventTime);
});

// Distributed table (proxy across cluster)
modelBuilder.Entity<Event>(entity =>
{
    entity.ToTable("events_distributed");
    entity.HasNoKey();

    entity.UseDistributed("my_cluster", "events_local")
        .WithShardingKey(x => x.UserId);
});
```

The local and distributed tables share the same column schema. Queries against the distributed table fan out to all local tables, and inserts are routed by the sharding key.

## Fluent Chaining

The `DistributedEngineBuilder` supports fluent chaining. Use `.And()` to return to the `EntityTypeBuilder` for additional configuration:

```csharp
entity.UseDistributed("my_cluster", "default", "events_local")
    .WithShardingKey(x => x.UserId)
    .WithPolicy("ssd_policy")
    .And()
    .HasComment("Distributed events table");
```

## Complete Example

```csharp
public class ClickEvent
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public long UserId { get; set; }
    public string Action { get; set; } = string.Empty;
}

// Local table on each shard
modelBuilder.Entity<ClickEvent>(entity =>
{
    entity.ToTable("click_events_local");
    entity.HasNoKey();

    entity.UseMergeTree(x => new { x.EventTime, x.Id })
        .HasPartitionByMonth(x => x.EventTime)
        .HasTtl(x => x.EventTime, ClickHouseInterval.Months(3));
});

// Distributed proxy
modelBuilder.Entity<ClickEventDistributed>(entity =>
{
    entity.ToTable("click_events");
    entity.HasNoKey();

    entity.UseDistributed("analytics_cluster", "click_events_local")
        .WithShardingKey("cityHash64(UserId)")
        .WithPolicy("ssd");
});
```

Local table DDL:

```sql
CREATE TABLE "click_events_local" (
    "Id" UUID,
    "EventTime" DateTime64(3),
    "UserId" Int64,
    "Action" String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM("EventTime")
ORDER BY ("EventTime", "Id")
TTL "EventTime" + INTERVAL 3 MONTH
```

Distributed table DDL:

```sql
CREATE TABLE "click_events" (
    "Id" UUID,
    "EventTime" DateTime64(3),
    "UserId" Int64,
    "Action" String
)
ENGINE = Distributed('analytics_cluster', currentDatabase(), 'click_events_local', cityHash64(UserId), 'ssd')
```

## Cross-Cluster Queries

Queries against the distributed table are executed in parallel across all shards:

```csharp
var results = await context.ClickEvents
    .Where(e => e.EventTime >= cutoff)
    .GroupBy(e => e.Action)
    .Select(g => new { Action = g.Key, Count = g.Count() })
    .ToListAsync();
```

Each shard processes its local portion of the data, and the results are merged by the coordinating node.

> **Note:** The underlying local table must exist on each cluster node before creating the distributed table. Use `ON CLUSTER` DDL (via `.UseCluster()`) to create local tables across all nodes simultaneously.

## See Also

- [MergeTree](mergetree.md) -- the typical engine for local tables behind a Distributed proxy
- [Null Engine](null-engine.md) -- can be used with materialized views in distributed setups
- [ReplacingMergeTree](replacing-mergetree.md) -- common local table engine for deduplication in distributed setups
