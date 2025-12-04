# OPTIMIZE TABLE

`OPTIMIZE TABLE` forces ClickHouse to merge data parts immediately. This is useful for triggering deduplication, aggregation, or TTL evaluation without waiting for background merges.

## When to Use

| Engine | Effect of OPTIMIZE FINAL |
|--------|--------------------------|
| ReplacingMergeTree | Deduplicates rows, keeping latest version |
| SummingMergeTree | Aggregates numeric columns |
| CollapsingMergeTree | Collapses +1/-1 sign pairs |
| AggregatingMergeTree | Merges aggregate states |
| Any with TTL | Deletes expired rows |

## Basic Usage

### Optimize Entire Table

```csharp
// Trigger merge (background-like behavior)
await context.Database.OptimizeTableAsync<Event>();

// Force complete merge
await context.Database.OptimizeTableFinalAsync<Event>();
```

### Optimize Specific Partition

```csharp
// Monthly partition (PARTITION BY toYYYYMM)
await context.Database.OptimizeTablePartitionFinalAsync<Event>("202401");

// Daily partition (PARTITION BY toYYYYMMDD)
await context.Database.OptimizeTablePartitionFinalAsync<Event>("20240115");
```

### String-Based Overloads

```csharp
// By table name
await context.Database.OptimizeTableFinalAsync("Events");
await context.Database.OptimizeTablePartitionFinalAsync("Events", "202401");
```

## Advanced Options

Use the options builder for complex scenarios:

```csharp
await context.Database.OptimizeTableAsync<Event>(o => o
    .WithPartition("202401")
    .WithFinal()
    .WithDeduplicate());

// DEDUPLICATE BY specific columns
await context.Database.OptimizeTableAsync<Event>(o => o
    .WithFinal()
    .WithDeduplicate("Id", "Timestamp"));
```

## Generated SQL

```sql
-- Basic
OPTIMIZE TABLE "Events"

-- With FINAL
OPTIMIZE TABLE "Events" FINAL

-- Specific partition
OPTIMIZE TABLE "Events" PARTITION '202401' FINAL

-- With DEDUPLICATE
OPTIMIZE TABLE "Events" FINAL DEDUPLICATE

-- DEDUPLICATE BY columns
OPTIMIZE TABLE "Events" FINAL DEDUPLICATE BY "Id", "Timestamp"
```

## Complete Example

### ReplacingMergeTree Deduplication

```csharp
public class UserProfile
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }
}

modelBuilder.Entity<UserProfile>(entity =>
{
    entity.UseReplacingMergeTree(x => x.UpdatedAt, x => x.Id);
});
```

Usage:

```csharp
// Insert initial version
context.Users.Add(new UserProfile
{
    Id = userId,
    Name = "Alice",
    UpdatedAt = DateTime.UtcNow
});
await context.SaveChangesAsync();

// "Update" by inserting new version
context.Users.Add(new UserProfile
{
    Id = userId,          // Same ID
    Name = "Alice Smith", // Updated name
    UpdatedAt = DateTime.UtcNow
});
await context.SaveChangesAsync();

// Before OPTIMIZE: Both rows may be visible
var before = await context.Users.CountAsync();  // May be 2

// Force deduplication
await context.Database.OptimizeTableFinalAsync<UserProfile>();

// After OPTIMIZE: Only latest row
var after = await context.Users.CountAsync();   // Will be 1
```

## FINAL Query vs OPTIMIZE TABLE

Two ways to see deduplicated data:

### 1. Query with Final()

```csharp
var users = await context.Users
    .Final()
    .ToListAsync();
```

- Merges on-the-fly during query
- No storage change
- Each query pays the cost

### 2. OPTIMIZE TABLE FINAL

```csharp
await context.Database.OptimizeTableFinalAsync<UserProfile>();
var users = await context.Users.ToListAsync();  // No Final() needed
```

- One-time merge to disk
- Persists merged state
- Future queries are faster

### When to Use Each

| Scenario | Recommendation |
|----------|----------------|
| Occasional consistent reads | Use `.Final()` query |
| After bulk imports | Use `OPTIMIZE TABLE FINAL` |
| Before backups | Use `OPTIMIZE TABLE FINAL` |
| Production maintenance | Use `OPTIMIZE TABLE FINAL` |
| Small tables, simple queries | Either works |

## Performance Considerations

`OPTIMIZE TABLE FINAL` can be resource-intensive:

- **CPU/IO**: Rewrites all data parts
- **Duration**: Can take minutes to hours on large tables
- **Blocking**: May impact concurrent queries

### Best Practices

```csharp
// Good: Optimize specific partition (faster)
await context.Database.OptimizeTablePartitionFinalAsync<Event>("202401");

// Slower: Optimize entire table
await context.Database.OptimizeTableFinalAsync<Event>();
```

### For Large Tables

1. **Optimize by partition**: Process one partition at a time
2. **Schedule off-peak**: Run during low-traffic periods
3. **Monitor progress**: Check system.mutations for status

```csharp
// Optimize partitions sequentially
foreach (var partition in new[] { "202401", "202402", "202403" })
{
    await context.Database.OptimizeTablePartitionFinalAsync<Event>(partition);
}
```

## Checking Optimization Status

Query system tables to monitor:

```sql
-- View running/pending mutations
SELECT * FROM system.mutations
WHERE table = 'Events' AND is_done = 0;

-- View part count (should decrease after optimize)
SELECT count() FROM system.parts
WHERE table = 'Events' AND active;
```

## TTL Evaluation

Force TTL to delete expired data immediately:

```csharp
// Table with TTL
entity.HasTtl("Timestamp + INTERVAL 90 DAY");

// Force TTL evaluation
await context.Database.OptimizeTableFinalAsync<Event>();
```

## Limitations

- **No row count**: Returns 0 (operation is async in ClickHouse)
- **Non-transactional**: Can't be rolled back
- **Resource intensive**: Plan for CPU/IO impact
- **No progress tracking**: Use system tables to monitor

## See Also

- [ReplacingMergeTree](../engines/replacing-mergetree.md)
- [Query Modifiers (Final)](query-modifiers.md)
- [TTL](ttl.md)
- [Partitioning](partitioning.md)
- [ClickHouse OPTIMIZE Docs](https://clickhouse.com/docs/en/sql-reference/statements/optimize)
