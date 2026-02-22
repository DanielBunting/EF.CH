# Partitioning

ClickHouse partitions organize data into independent physical segments based on a PARTITION BY expression. Each partition is stored as a separate set of files on disk, enabling efficient data management operations such as dropping old partitions, detaching cold data, and optimizing queries that filter on the partition key.

---

## Partition Strategies

### By Month

The most common strategy for time-series data. Groups rows into monthly partitions using `toYYYYMM()`.

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.EventType });

    entity.HasPartitionByMonth(x => x.Timestamp);
});
```

**Generated SQL:**

```sql
CREATE TABLE Events (
    ...
) ENGINE = MergeTree()
ORDER BY (Timestamp, EventType)
PARTITION BY toYYYYMM("Timestamp")
```

Produces partition IDs like `202601`, `202602`, `202603`.

### By Day

Finer-grained partitioning for high-volume tables where daily data management is needed.

```csharp
entity.HasPartitionByDay(x => x.EventDate);
```

**Generated SQL:**

```sql
PARTITION BY toYYYYMMDD("EventDate")
```

Produces partition IDs like `20260101`, `20260102`, `20260103`.

### By Year

Coarser partitioning for tables with lower volume or multi-year retention.

```csharp
entity.HasPartitionByYear(x => x.OrderDate);
```

**Generated SQL:**

```sql
PARTITION BY toYear("OrderDate")
```

Produces partition IDs like `2024`, `2025`, `2026`.

### By Column Value

Partition by a non-date column for categorical data distribution.

```csharp
entity.HasPartitionBy(x => x.Region);
```

**Generated SQL:**

```sql
PARTITION BY "Region"
```

### Custom Expression (Raw SQL)

For complex partitioning logic that combines multiple columns or uses ClickHouse functions.

```csharp
entity.HasPartitionBy("toYYYYMM(\"CreatedAt\")");
entity.HasPartitionBy("(\"Region\", toYYYYMM(\"CreatedAt\"))");
entity.HasPartitionBy("intDiv(toUInt32(\"UserId\"), 1000000)");
```

---

## Choosing a Partition Strategy

| Strategy | Method | Typical Row Volume | Use Case |
|---|---|---|---|
| Monthly | `HasPartitionByMonth()` | Millions-billions/month | Standard time-series, event logs, metrics |
| Daily | `HasPartitionByDay()` | Millions+/day | High-volume logs, real-time analytics |
| Yearly | `HasPartitionByYear()` | Any | Multi-year archives, low-volume tables |
| By column | `HasPartitionBy(x => x.Col)` | Varies | Multi-tenant, regional separation |
| Custom | `HasPartitionBy("expr")` | Varies | Composite keys, hash-based distribution |

### Guidelines

- Keep partition count manageable. ClickHouse recommends fewer than a few thousand partitions per table. Each partition has overhead for metadata and background merges.
- Monthly partitioning is the default recommendation for most time-series workloads.
- Daily partitioning works well when you need to drop individual days or when daily data volume is very high.
- Avoid partitioning by high-cardinality columns (e.g., user ID). This creates too many partitions and degrades performance.

---

## Partition Management

Partitions enable efficient data lifecycle operations through the `OptimizeTable` APIs and raw SQL.

### Drop Old Partitions

```csharp
// Drop a specific month
await context.Database.ExecuteSqlRawAsync(
    "ALTER TABLE Events DROP PARTITION 202401");

// Drop a specific day
await context.Database.ExecuteSqlRawAsync(
    "ALTER TABLE Events DROP PARTITION 20240115");
```

### Optimize a Specific Partition

```csharp
// Force merge of parts within a partition
await context.Database.OptimizeTablePartitionAsync<Event>("202401");

// Force merge with deduplication (FINAL)
await context.Database.OptimizeTablePartitionFinalAsync<Event>("202401");
```

### Detach and Attach

```csharp
// Move partition to detached directory (fast, reversible)
await context.Database.ExecuteSqlRawAsync(
    "ALTER TABLE Events DETACH PARTITION 202401");

// Reattach a previously detached partition
await context.Database.ExecuteSqlRawAsync(
    "ALTER TABLE Events ATTACH PARTITION 202401");
```

---

## Query Optimization

ClickHouse automatically prunes partitions when the WHERE clause filters on the partition key expression. Queries that include the partition column in their filter conditions skip all non-matching partitions entirely.

```csharp
// Monthly partition -- only reads the 202601 partition
var januaryEvents = await context.Events
    .Where(e => e.Timestamp >= new DateTime(2026, 1, 1)
             && e.Timestamp < new DateTime(2026, 2, 1))
    .ToListAsync();

// Daily partition -- reads only the single day partition
var todayEvents = await context.Events
    .Where(e => e.EventDate == DateOnly.FromDateTime(DateTime.Today))
    .ToListAsync();

// Column partition -- reads only the "us-east" partition
var regionalOrders = await context.Orders
    .Where(o => o.Region == "us-east")
    .ToListAsync();
```

Queries that do not filter on the partition key scan all partitions. If most queries filter by date, partition by date. If most queries filter by region, partition by region.

---

## Complete Example

```csharp
modelBuilder.Entity<MetricPoint>(entity =>
{
    entity.HasKey(x => x.Id);

    entity.UseMergeTree(x => new { x.Timestamp, x.MetricName })
        .HasPartitionByMonth(x => x.Timestamp);

    // Skip index on metric name for fast filtering within partitions
    entity.HasIndex(x => x.MetricName)
        .UseSet(maxRows: 500)
        .HasGranularity(3);

    // TTL to auto-expire old data
    entity.HasTtl(x => x.Timestamp, TimeSpan.FromDays(90));
});
```

This configuration creates monthly partitions, a set index for metric name filtering, and a 90-day TTL for automatic cleanup.

---

## See Also

- [TTL](ttl.md) -- automatic data expiration
- [Column Features](column-features.md) -- compression codecs, computed columns
- [Skip Indices](skip-indices.md) -- data skipping indices for query acceleration
- [Projections](projections.md) -- pre-computed aggregations and alternative sort orders
