# MergeTree Engine

MergeTree is the foundational engine for ClickHouse. Use it for general-purpose, append-only data storage.

## When to Use

- Event logs and analytics data
- Time-series metrics
- Append-only audit trails
- Any data that doesn't need updates

## Configuration

### Basic Setup

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public string? Data { get; set; }
}

public class MyDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });
    }
}
```

### With Partitioning

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);  // One partition per month
});
```

### With TTL

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);
    entity.HasTtl("Timestamp + INTERVAL 90 DAY");  // Auto-delete after 90 days
});
```

### Keyless (Append-Only)

For pure append-only data without entity tracking:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasNoKey();  // No primary key
    entity.UseMergeTree(x => new { x.Timestamp, x.EventType });
});
```

## Generated DDL

The configuration above generates:

```sql
CREATE TABLE "Events" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "EventType" String NOT NULL,
    "UserId" String NOT NULL,
    "Data" Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "Id")
TTL "Timestamp" + INTERVAL 90 DAY
```

## Usage Examples

### Inserting Data

```csharp
// Single insert
context.Events.Add(new Event
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventType = "page_view",
    UserId = "user-123"
});
await context.SaveChangesAsync();

// Batch insert (more efficient)
var events = Enumerable.Range(0, 1000)
    .Select(i => new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow,
        EventType = "page_view",
        UserId = $"user-{i % 100}"
    });

context.Events.AddRange(events);
await context.SaveChangesAsync();
```

### Querying Data

```csharp
// Filter on ORDER BY columns (efficient)
var recentEvents = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddHours(-1))
    .OrderByDescending(e => e.Timestamp)
    .Take(100)
    .ToListAsync();

// Aggregation
var eventCounts = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-7))
    .GroupBy(e => e.EventType)
    .Select(g => new { EventType = g.Key, Count = g.Count() })
    .ToListAsync();
```

### Deleting Data

```csharp
// Single delete
var toDelete = await context.Events.FirstAsync(e => e.Id == targetId);
context.Events.Remove(toDelete);
await context.SaveChangesAsync();

// Bulk delete
await context.Events
    .Where(e => e.Timestamp < DateTime.UtcNow.AddDays(-365))
    .ExecuteDeleteAsync();
```

## ORDER BY Best Practices

The ORDER BY columns are critical for query performance:

### Good: Filter columns first

```csharp
// Queries on Timestamp are fast
entity.UseMergeTree(x => new { x.Timestamp, x.EventType, x.Id });

// This query uses the primary index efficiently
context.Events.Where(e => e.Timestamp > cutoff)
```

### Bad: High-cardinality first

```csharp
// Id has too many unique values - queries can't use index efficiently
entity.UseMergeTree(x => new { x.Id, x.Timestamp });
```

### Typical Patterns

| Use Case | ORDER BY |
|----------|----------|
| Time-series events | `{ Timestamp, Id }` |
| User activity | `{ UserId, Timestamp }` |
| Multi-tenant | `{ TenantId, Timestamp, Id }` |
| Logs by service | `{ ServiceName, Timestamp }` |

## Partitioning Strategies

| Data Volume | Retention | Partition By |
|-------------|-----------|--------------|
| < 1M rows/day | Years | Month |
| 1M-100M rows/day | Months | Day |
| > 100M rows/day | Weeks | Day (consider hourly) |

```csharp
// Monthly partitions (most common)
entity.HasPartitionByMonth(x => x.Timestamp);

// Daily partitions (high volume)
entity.HasPartitionByDay(x => x.EventDate);

// Yearly partitions (low volume, long retention)
entity.HasPartitionByYear(x => x.OrderDate);
```

## Limitations

- **No UPDATE**: Attempting to update throws `NotSupportedException`
- **Duplicates possible**: If you insert the same Id twice, both rows exist
- **Delete is async**: Deleted rows aren't immediately removed from disk

## When Not to Use MergeTree

Use a different engine if you need:

| Need | Use Instead |
|------|-------------|
| Deduplicate by key | [ReplacingMergeTree](replacing-mergetree.md) |
| Auto-sum columns | [SummingMergeTree](summing-mergetree.md) |
| Track state changes | [CollapsingMergeTree](collapsing-mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [Partitioning](../features/partitioning.md)
- [TTL](../features/ttl.md)
- [ClickHouse MergeTree Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
