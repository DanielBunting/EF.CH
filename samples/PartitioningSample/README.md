# PartitioningSample

Demonstrates ClickHouse partitioning strategies for efficient data management.

## What This Shows

- Monthly partitioning with `HasPartitionByMonth()`
- Daily partitioning with `HasPartitionByDay()`
- TTL for automatic data expiration with `HasTtl()`
- Query partition pruning for performance

## Why Partition?

1. **Query Performance**: Queries filtering on partition key skip irrelevant partitions
2. **Data Management**: Drop old data instantly by dropping partitions
3. **Storage Optimization**: Old partitions can be moved to cheaper storage
4. **TTL Efficiency**: Whole partitions can be dropped when all data expires

## Partitioning Strategies

| Strategy | Use When |
|----------|----------|
| Monthly | Data retained for months/years, moderate volume |
| Daily | High volume, short retention (days/weeks) |
| Yearly | Very long retention, low-moderate volume |

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Partitioning Sample
===================

Creating database and tables...
Inserting access logs...

Inserted 6 access logs.

Inserting metrics...

Inserted 50 metrics across 5 days.

--- Query: December 2024 logs only (partition pruning) ---
  2024-12-01 10:30 | /api/users | 200
  2024-12-15 14:22 | /api/orders | 201
  2024-12-20 09:15 | /api/users | 404

Found 3 December logs.

--- Query: Today's metrics only (partition pruning) ---
  2025-01-15 03:00 | cpu_usage | 45.23% | server-1
  2025-01-15 08:00 | memory_usage | 67.89% | server-2
  ...

Found 10 today's metrics.

--- Aggregation: Average response time by endpoint (January) ---
  /api/products: 89.0ms avg
  /api/orders: 250.0ms avg

Done!
```

## Key Code

### Monthly Partitioning (Access Logs)

```csharp
modelBuilder.Entity<AccessLog>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Monthly partitions
    entity.HasPartitionByMonth(x => x.Timestamp);

    // Auto-expire after 1 year
    entity.HasTtl("Timestamp + INTERVAL 1 YEAR");
});
```

Generates:
```sql
PARTITION BY toYYYYMM("Timestamp")
TTL "Timestamp" + INTERVAL 1 YEAR
```

### Daily Partitioning (Metrics)

```csharp
modelBuilder.Entity<Metric>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Daily partitions - finer granularity
    entity.HasPartitionByDay(x => x.Timestamp);

    // Short retention
    entity.HasTtl("Timestamp + INTERVAL 30 DAY");
});
```

Generates:
```sql
PARTITION BY toYYYYMMDD("Timestamp")
TTL "Timestamp" + INTERVAL 30 DAY
```

## Partition Pruning

When filtering on the partition key, ClickHouse only reads relevant partitions:

```csharp
// Only reads December 2024 partition
var decemberLogs = await context.AccessLogs
    .Where(l => l.Timestamp >= new DateTime(2024, 12, 1))
    .Where(l => l.Timestamp < new DateTime(2025, 1, 1))
    .ToListAsync();
```

## Choosing Granularity

| Data Volume | Retention | Recommended |
|-------------|-----------|-------------|
| < 1M rows/day | Years | Monthly |
| 1M-100M rows/day | Months | Daily |
| > 100M rows/day | Weeks | Daily |

**Rule of thumb:** Aim for partitions with 1M-100M rows each.

## Learn More

- [Partitioning Documentation](../../docs/features/partitioning.md)
- [TTL Documentation](../../docs/features/ttl.md)
