# OptimizeTableSample

Demonstrates forcing merges in ClickHouse using `OPTIMIZE TABLE ... FINAL`.

## What This Shows

- Forcing deduplication with `OptimizeTableFinalAsync<T>()`
- Optimizing specific partitions with `OptimizeTablePartitionFinalAsync<T>()`
- Using advanced options with `OptimizeTableAsync<T>(options)`
- The difference between querying with and without `Final()`

## Why OPTIMIZE TABLE?

ClickHouse merges data parts in the background. Until merging completes:
- **ReplacingMergeTree**: Duplicate rows may be visible
- **SummingMergeTree**: Rows may not be aggregated
- **CollapsingMergeTree**: +1/-1 pairs may not be collapsed
- **TTL**: Expired data may still be visible

`OPTIMIZE TABLE FINAL` forces immediate merging.

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
OPTIMIZE TABLE Sample
=====================

Creating database and tables...

Inserting user profile...
Inserted user: Alice (Id: ...)

Updating user (inserting new version)...
Inserted updated version: Alice Smith

--- Query without FINAL (may show duplicates) ---
Row count: 2
  Alice (updated: 10:30:00)
  Alice Smith (updated: 12:30:00)

--- Query with FINAL (shows latest version only) ---
Row count: 1
  Alice Smith (updated: 12:30:00)

--- Running OPTIMIZE TABLE FINAL ---
This forces ClickHouse to merge data parts and deduplicate...
Optimization complete!

--- Query without FINAL (after optimization) ---
Row count: 1
  Alice Smith (updated: 12:30:00)

--- Demonstrating partition optimization ---
Inserted events in January and February partitions.
Optimizing January 2024 partition only...
Partition 202401 optimized.

--- Using advanced options ---
Partition 202402 optimized with DEDUPLICATE.

Done!
```

## Key Code

### Force Deduplication (Whole Table)

```csharp
// Insert updates as new rows (ReplacingMergeTree pattern)
context.Users.Add(new UserProfile
{
    Id = existingId,      // Same ID
    Name = "New Name",    // Updated data
    UpdatedAt = DateTime.UtcNow  // Newer version
});
await context.SaveChangesAsync();

// Force merge to deduplicate
await context.Database.OptimizeTableFinalAsync<UserProfile>();
```

### Optimize Specific Partition

```csharp
// Optimize only January 2024 partition
await context.Database.OptimizeTablePartitionFinalAsync<Event>("202401");

// For daily partitions
await context.Database.OptimizeTablePartitionFinalAsync<Event>("20240115");
```

### Advanced Options

```csharp
await context.Database.OptimizeTableAsync<Event>(o => o
    .WithPartition("202401")
    .WithFinal()
    .WithDeduplicate("Id", "Timestamp"));
```

## FINAL Query vs OPTIMIZE TABLE

| Approach | Effect | Performance |
|----------|--------|-------------|
| `.Final()` query | On-the-fly merge during query | Slower queries, no storage change |
| `OPTIMIZE TABLE FINAL` | Persists merged data to disk | One-time cost, faster future queries |

### When to Use Each

**Use `.Final()` when:**
- You need occasional consistent reads
- Data volume is small
- You can't afford OPTIMIZE overhead

**Use `OPTIMIZE TABLE FINAL` when:**
- You need fast queries without FINAL
- After bulk data loads
- Before taking backups
- As part of maintenance jobs

## Performance Considerations

`OPTIMIZE TABLE FINAL` can be resource-intensive:
- Rewrites all data parts
- CPU and I/O intensive
- May take a long time on large tables

Best practices:
- Run during low-traffic periods
- Optimize partitions individually for large tables
- Consider scheduling as a background job

## Learn More

- [OPTIMIZE TABLE Documentation](../../docs/features/optimize.md)
- [ReplacingMergeTree](../../docs/engines/replacing-mergetree.md)
- [Query Modifiers (Final)](../../docs/features/query-modifiers.md)
