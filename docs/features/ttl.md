# TTL (Time-To-Live)

TTL automatically expires and deletes old data. ClickHouse removes expired rows during background merges.

## Why Use TTL?

1. **Storage Management**: Automatically purge old data
2. **Compliance**: Enforce data retention policies
3. **Performance**: Keep tables at optimal size
4. **Cost Control**: Reduce storage costs over time

## Configuration

### Basic TTL

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasTtl("Timestamp + INTERVAL 90 DAY");
});
```

Generates:
```sql
TTL "Timestamp" + INTERVAL 90 DAY
```

### TTL with Partitioning

Combine TTL with partitioning for efficient data lifecycle:

```csharp
modelBuilder.Entity<AccessLog>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);
    entity.HasTtl("Timestamp + INTERVAL 1 YEAR");
});
```

## TTL Expressions

### Time Intervals

```csharp
// Days
entity.HasTtl("CreatedAt + INTERVAL 7 DAY");

// Months
entity.HasTtl("CreatedAt + INTERVAL 3 MONTH");

// Years
entity.HasTtl("CreatedAt + INTERVAL 1 YEAR");

// Hours (for high-frequency data)
entity.HasTtl("Timestamp + INTERVAL 24 HOUR");
```

### Column Reference

The TTL expression references your DateTime column:

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }  // TTL based on this
    public string EventType { get; set; } = string.Empty;
}

entity.HasTtl("Timestamp + INTERVAL 30 DAY");
```

## Complete Example

```csharp
public class MetricData
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string MetricName { get; set; } = string.Empty;
    public double Value { get; set; }
}

modelBuilder.Entity<MetricData>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Monthly partitions for efficient drops
    entity.HasPartitionByMonth(x => x.Timestamp);

    // Auto-expire after 90 days
    entity.HasTtl("Timestamp + INTERVAL 90 DAY");
});
```

## Generated DDL

```sql
CREATE TABLE "MetricData" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "MetricName" String NOT NULL,
    "Value" Float64 NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Id")
TTL "Timestamp" + INTERVAL 90 DAY
```

## How TTL Works

1. **Asynchronous**: Expiration happens during background merges
2. **Eventually Consistent**: Expired rows may be visible briefly after expiration
3. **Partition-Aware**: Whole partitions can be dropped when all data expires
4. **Non-Blocking**: Doesn't impact query performance

## Common Patterns

### Metrics/Telemetry

```csharp
// Keep metrics for 90 days
entity.HasPartitionByDay(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 90 DAY");
```

### Session Data

```csharp
// Expire sessions after 24 hours
entity.HasTtl("LastActivity + INTERVAL 24 HOUR");
```

### Audit Logs

```csharp
// Compliance: Keep for 7 years
entity.HasPartitionByMonth(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 7 YEAR");
```

### Temporary Cache

```csharp
// Short-lived cache data
entity.HasTtl("CreatedAt + INTERVAL 1 HOUR");
```

## Best Practices

### Match TTL to Partition Granularity

```csharp
// Good: 90-day TTL with monthly partitions
entity.HasPartitionByMonth(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 90 DAY");

// Good: 7-day TTL with daily partitions
entity.HasPartitionByDay(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 7 DAY");
```

### Use TTL Column in ORDER BY

```csharp
// Good: Timestamp in ORDER BY
entity.UseMergeTree(x => new { x.Timestamp, x.Id });
entity.HasTtl("Timestamp + INTERVAL 30 DAY");

// Less optimal: Timestamp not in ORDER BY
entity.UseMergeTree(x => x.Id);
entity.HasTtl("Timestamp + INTERVAL 30 DAY");
```

## Forcing TTL Evaluation

TTL is evaluated during merges. To force immediate cleanup:

```csharp
// Force merge to trigger TTL
await context.Database.ExecuteSqlRawAsync(
    @"OPTIMIZE TABLE ""MetricData"" FINAL");
```

**Note**: `OPTIMIZE FINAL` is resource-intensive on large tables.

## Checking TTL Status

```sql
SELECT
    table,
    partition,
    rows,
    delete_ttl_info_min,
    delete_ttl_info_max
FROM system.parts
WHERE table = 'MetricData' AND active
ORDER BY partition;
```

## Limitations

- **Asynchronous**: Not instant deletion
- **Merge-Dependent**: Requires background merge activity
- **No Precision**: Exact deletion time is non-deterministic
- **Immutable**: Can't change TTL expression without recreating table

## TTL vs DROP PARTITION

| Approach | When to Use |
|----------|-------------|
| TTL | Automatic, per-row expiration |
| DROP PARTITION | Manual, bulk deletion of time ranges |

Both can be used together:

```csharp
// TTL for automatic cleanup
entity.HasTtl("Timestamp + INTERVAL 90 DAY");

// Manual partition drop for immediate cleanup
await context.Database.ExecuteSqlRawAsync(
    @"ALTER TABLE ""Events"" DROP PARTITION '202301'");
```

## See Also

- [Partitioning](partitioning.md)
- [MergeTree Engine](../engines/mergetree.md)
- [ClickHouse TTL Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)
