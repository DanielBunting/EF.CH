# Partitioning

Partitioning divides table data into separate parts based on a partition key. This improves query performance and enables efficient data lifecycle management.

## Why Partition?

1. **Query Performance**: Queries filtering on partition key skip irrelevant partitions
2. **Data Management**: Drop old data instantly by dropping partitions
3. **Parallel Processing**: ClickHouse can process partitions in parallel
4. **Storage Optimization**: Old partitions can be moved to cheaper storage

## Configuration

### Partition by Month (Most Common)

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);
});
```

Generates:
```sql
PARTITION BY toYYYYMM("Timestamp")
```

### Partition by Day

```csharp
entity.HasPartitionByDay(x => x.EventDate);
```

Generates:
```sql
PARTITION BY toYYYYMMDD("EventDate")
```

### Partition by Year

```csharp
entity.HasPartitionByYear(x => x.OrderDate);
```

Generates:
```sql
PARTITION BY toYear("OrderDate")
```

### Custom Partition Expression

```csharp
entity.HasPartitionBy("toYYYYMM(\"CreatedAt\")");
entity.HasPartitionBy("toMonday(\"EventDate\")");  // Weekly
entity.HasPartitionBy("intDiv(UserId, 1000000)");  // By user ID range
```

## Choosing Partition Granularity

| Data Volume | Retention | Recommended |
|-------------|-----------|-------------|
| < 1M rows/day | Years | Monthly |
| 1M-100M rows/day | Months | Daily |
| > 100M rows/day | Weeks | Daily or custom |
| Low volume, long retention | 10+ years | Yearly |

**Rule of thumb:** Aim for partitions with 1M-100M rows each.

## Complete Example

```csharp
public class AccessLog
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public int ResponseTimeMs { get; set; }
}

modelBuilder.Entity<AccessLog>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Monthly partitions
    entity.HasPartitionByMonth(x => x.Timestamp);

    // Optional: TTL for automatic cleanup
    entity.HasTtl("Timestamp + INTERVAL 90 DAY");
});
```

## Generated DDL

```sql
CREATE TABLE "AccessLogs" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "Endpoint" String NOT NULL,
    "StatusCode" Int32 NOT NULL,
    "ResponseTimeMs" Int32 NOT NULL
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Id")
TTL "Timestamp" + INTERVAL 90 DAY
```

## Query Optimization

### Partition Pruning

Queries filtering on the partition key skip irrelevant partitions:

```csharp
// Fast: Only scans January 2024 partition
var januaryLogs = await context.AccessLogs
    .Where(l => l.Timestamp >= new DateTime(2024, 1, 1))
    .Where(l => l.Timestamp < new DateTime(2024, 2, 1))
    .ToListAsync();

// Slower: Scans all partitions (no partition filter)
var slowEndpoint = await context.AccessLogs
    .Where(l => l.Endpoint == "/api/slow")
    .ToListAsync();
```

### Best Query Patterns

```csharp
// Good: Filter on partition key first
context.Logs
    .Where(l => l.Timestamp > cutoffDate)  // Partition pruning
    .Where(l => l.StatusCode >= 500)       // Then other filters

// Less optimal: Non-partition filter first
context.Logs
    .Where(l => l.StatusCode >= 500)
    .Where(l => l.Timestamp > cutoffDate)
```

## Data Lifecycle Management

### Dropping Old Partitions

```csharp
// Drop a specific partition (instant, unlike DELETE)
await context.Database.ExecuteSqlRawAsync(
    @"ALTER TABLE ""AccessLogs"" DROP PARTITION '202301'");  // January 2023
```

### With TTL (Automatic)

Combine partitioning with TTL for automatic cleanup:

```csharp
entity.HasPartitionByMonth(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 1 YEAR");
```

ClickHouse automatically drops expired data during merges.

### Moving Partitions

Move old data to cheaper storage:

```csharp
// Move to cold storage
await context.Database.ExecuteSqlRawAsync(
    @"ALTER TABLE ""AccessLogs"" MOVE PARTITION '202301' TO DISK 'cold'");
```

## Multi-Column Partitioning

You can partition by multiple expressions:

```csharp
// Custom: Partition by tenant and month
entity.HasPartitionBy("(TenantId, toYYYYMM(\"CreatedAt\"))");
```

**Caution:** More partition keys = more partitions. Avoid too many small partitions.

## Partition Information

Query partition metadata:

```csharp
var partitions = await context.Database.SqlQueryRaw<PartitionInfo>(@"
    SELECT
        partition AS Partition,
        sum(rows) AS Rows,
        formatReadableSize(sum(bytes_on_disk)) AS Size
    FROM system.parts
    WHERE table = 'AccessLogs' AND active
    GROUP BY partition
    ORDER BY partition DESC
").ToListAsync();

public record PartitionInfo(string Partition, ulong Rows, string Size);
```

## Common Patterns

### Time-Series Events

```csharp
entity.UseMergeTree(x => new { x.Timestamp, x.EventType });
entity.HasPartitionByMonth(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 90 DAY");
```

### Multi-Tenant Data

```csharp
// Partition by tenant for isolation
entity.HasPartitionBy("TenantId");

// Or tenant + time
entity.HasPartitionBy("(TenantId, toYYYYMM(\"CreatedAt\"))");
```

### Audit Logs

```csharp
entity.UseMergeTree(x => new { x.Timestamp, x.Id });
entity.HasPartitionByMonth(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 7 YEAR");  // Compliance retention
```

## Limitations

- **Partition Key Immutable**: Can't change partition key after table creation
- **Too Many Partitions**: Hurts performance (aim for < 10,000 active partitions)
- **Non-Partition Queries**: Full table scans if not filtering on partition key

## Best Practices

### Match Partitioning to Retention

```csharp
// 90-day retention → Monthly partitions (3 active partitions)
entity.HasPartitionByMonth(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 90 DAY");

// 7-day retention → Daily partitions (7 active partitions)
entity.HasPartitionByDay(x => x.Timestamp);
entity.HasTtl("Timestamp + INTERVAL 7 DAY");
```

### Include Time in ORDER BY

```csharp
// Good: Partition key column in ORDER BY
entity.UseMergeTree(x => new { x.Timestamp, x.Id });
entity.HasPartitionByMonth(x => x.Timestamp);

// Less optimal: Partition column not in ORDER BY
entity.UseMergeTree(x => x.Id);
entity.HasPartitionByMonth(x => x.Timestamp);
```

### Monitor Partition Count

Too many partitions impacts performance:

```sql
SELECT count() as partition_count
FROM system.parts
WHERE table = 'AccessLogs' AND active
```

## See Also

- [TTL (Time-To-Live)](ttl.md)
- [MergeTree Engine](../engines/mergetree.md)
- [ClickHouse Partitioning Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/custom-partitioning-key)
