# ClickHouse Concepts for EF Core Developers

This guide explains ClickHouse's architecture and behavior for developers familiar with traditional relational databases like SQL Server or PostgreSQL.

## Why ClickHouse is Different

ClickHouse is a **columnar OLAP database** designed for:
- Analytical queries over billions of rows
- High-throughput data ingestion
- Real-time aggregations and reporting

It is **not** designed for:
- OLTP workloads (frequent small updates)
- Row-level transactions
- Complex joins across many tables

Understanding these design goals explains most of the differences you'll encounter.

## Table Engines: The Heart of ClickHouse

Every ClickHouse table has an **engine** that determines how data is stored and queried. There's no default - you must choose one.

### MergeTree Family

The MergeTree engine family is used for 99% of production tables:

| Engine | Purpose |
|--------|---------|
| **MergeTree** | General purpose, append-only data |
| **ReplacingMergeTree** | Deduplicate rows by key, keep latest |
| **SummingMergeTree** | Auto-sum numeric columns during merges |
| **AggregatingMergeTree** | Store pre-aggregated state |
| **CollapsingMergeTree** | Track state changes with +1/-1 signs |

In EF.CH, you configure the engine using fluent API:

```csharp
entity.UseMergeTree(x => new { x.Timestamp, x.Id });
```

### ORDER BY is Required

Every MergeTree table needs an `ORDER BY` clause. This defines:
1. The physical sort order of data on disk
2. The primary index for efficient queries
3. The deduplication key (for ReplacingMergeTree)

```csharp
// Data is sorted by Timestamp, then Id
entity.UseMergeTree(x => new { x.Timestamp, x.Id });
```

**Choose ORDER BY columns carefully:**
- Put frequently-filtered columns first
- Put high-cardinality columns last
- Don't include too many columns (impacts write performance)

## No ACID Transactions

ClickHouse prioritizes throughput over consistency:

- **No rollback**: If an insert fails partway through, some rows may be written
- **Eventual consistency**: Data is immediately visible but merges happen asynchronously
- **No cross-table transactions**: Each table is independent

**EF Core Implications:**

```csharp
// This works, but there's no transaction
context.Events.Add(event1);
context.Events.Add(event2);
await context.SaveChangesAsync();  // Both inserted, but no rollback if one fails
```

**Design Pattern:** Use idempotent operations and design for eventual consistency.

## No Row-Level UPDATE

ClickHouse's columnar storage makes row-level tracked updates inefficient. `SaveChanges()` with modified entities throws an exception. However, bulk updates via `ExecuteUpdateAsync` are supported.

### Option 1: ExecuteUpdateAsync (Bulk Updates)

For updating many rows at once, use EF Core's `ExecuteUpdateAsync` which generates `ALTER TABLE ... UPDATE`:

```csharp
// Bulk update — efficient for many rows
await context.Users
    .Where(u => u.Status == "inactive")
    .ExecuteUpdateAsync(s => s.SetProperty(u => u.Status, "archived"));

// Expression-based update
await context.Products
    .Where(p => p.Category == "electronics")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, p => p.Price * 1.1m));
```

See [Update Operations](features/update-operations.md) for full documentation.

### Option 2: ReplacingMergeTree (Recommended for Row-Level)

Insert a new row with the same key and a higher version. ClickHouse merges them eventually:

```csharp
modelBuilder.Entity<User>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseReplacingMergeTree(
        versionColumn: x => x.UpdatedAt,  // Higher = newer
        orderByColumn: x => x.Id);
});

// "Update" by inserting a new version
var user = await context.Users.FindAsync(id);
user.Name = "New Name";
user.UpdatedAt = DateTime.UtcNow;

// This actually inserts a new row
context.Users.Add(user);
await context.SaveChangesAsync();

// Query with FINAL to see deduplicated results
var currentUser = await context.Users
    .Final()
    .FirstAsync(u => u.Id == id);
```

### Option 3: Delete and Re-insert

For infrequent updates:

```csharp
var user = await context.Users.FindAsync(id);
context.Users.Remove(user);
await context.SaveChangesAsync();

user.Name = "New Name";
context.Users.Add(user);
await context.SaveChangesAsync();
```

### Option 4: Append-Only Design

Don't update at all - append new facts:

```csharp
// Instead of updating user.Email
context.UserEmailChanges.Add(new UserEmailChange
{
    UserId = userId,
    NewEmail = "new@example.com",
    ChangedAt = DateTime.UtcNow
});

// Query the latest email
var currentEmail = await context.UserEmailChanges
    .Where(c => c.UserId == userId)
    .OrderByDescending(c => c.ChangedAt)
    .Select(c => c.NewEmail)
    .FirstAsync();
```

## DELETE Strategies

ClickHouse offers two delete approaches:

### Lightweight Delete (Default)

```csharp
context.Orders.Remove(order);
await context.SaveChangesAsync();
// Generates: DELETE FROM Orders WHERE Id = '...'
```

- Marks rows as deleted (not physically removed)
- Queries immediately exclude deleted rows
- Physical deletion happens during background merges
- Fast, but disk space isn't immediately freed

### Mutation Delete

```csharp
options.UseClickHouse("...", o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

- Rewrites data parts to physically remove rows
- Async operation (may take time on large tables)
- Use for bulk maintenance, not frequent deletes

## Partitioning

Partitioning divides tables into manageable chunks:

```csharp
entity.HasPartitionByMonth(x => x.CreatedAt);  // One partition per month
entity.HasPartitionByDay(x => x.EventDate);    // One partition per day
```

**Benefits:**
- Queries on partitioned columns skip irrelevant partitions
- Drop old data by dropping partitions (instant)
- Parallel processing of partitions

**Best Practices:**
- Partition by time for time-series data
- Aim for 1,000-10,000 rows per partition minimum
- Don't over-partition (too many small partitions hurts performance)

## TTL: Automatic Data Expiration

Automatically delete old data:

```csharp
entity.HasTtl("CreatedAt + INTERVAL 90 DAY");
```

ClickHouse periodically removes expired rows during merges. Combined with partitioning, you can efficiently manage data lifecycle.

## Batch-Oriented Inserts

ClickHouse is optimized for bulk inserts:

| Insert Pattern | Rows/Second | Notes |
|---------------|-------------|-------|
| Single row | ~100 | Very inefficient |
| Batch (1,000) | ~10,000 | Better |
| Batch (10,000+) | ~100,000+ | Optimal |

**EF Core Implications:**

```csharp
// Inefficient: one insert per SaveChanges
foreach (var item in items)
{
    context.Items.Add(item);
    await context.SaveChangesAsync();  // Don't do this!
}

// Better: batch then save
context.Items.AddRange(items);  // Add all at once
await context.SaveChangesAsync();  // One batch insert
```

The provider batches inserts automatically. Configure the batch size:

```csharp
options.UseClickHouse("...", o => o.MaxBatchSize(10000));
```

## Materialized Views

In ClickHouse, materialized views are **INSERT triggers**, not cached query results:

```csharp
entity.AsMaterializedView<HourlySummary, Event>(
    query: events => events
        .GroupBy(e => new { Hour = e.Timestamp.Date, e.EventType })
        .Select(g => new HourlySummary
        {
            Hour = g.Key.Hour,
            EventType = g.Key.EventType,
            Count = g.Count()
        }),
    populate: false);
```

When you insert into the source table (`Events`), ClickHouse automatically:
1. Runs the query against the new rows
2. Inserts the results into the materialized view table

This is incremental - only new data is processed.

## The FINAL Modifier

For ReplacingMergeTree tables, rows with the same key aren't immediately merged. Use `FINAL` to see deduplicated results:

```csharp
// May return duplicate rows (pre-merge)
var users = await context.Users.ToListAsync();

// Guaranteed deduplicated (slower)
var users = await context.Users.Final().ToListAsync();
```

**Note:** `FINAL` adds overhead. For real-time dashboards, consider accepting slightly stale data.

## No Auto-Increment

ClickHouse doesn't have `IDENTITY` columns. Use:

### UUID (Recommended)

```csharp
public class Event
{
    public Guid Id { get; set; } = Guid.NewGuid();
}
```

### Application-Generated IDs

```csharp
public class Event
{
    public long Id { get; set; }  // Set by your application
}
```

### Snowflake IDs

For distributed systems, use Snowflake or ULID libraries.

## No Foreign Key Enforcement

ClickHouse doesn't enforce referential integrity:

```csharp
// This won't be enforced by the database
modelBuilder.Entity<Order>()
    .HasOne<Customer>()
    .WithMany()
    .HasForeignKey(o => o.CustomerId);
```

The navigation property still works for LINQ queries, but:
- No cascade delete
- No constraint violation errors
- Orphaned records are possible

**Design Pattern:** Validate relationships in your application layer.

## Query Optimization Tips

### Filter on ORDER BY Columns

```csharp
// Fast: Timestamp is first ORDER BY column
context.Events.Where(e => e.Timestamp > cutoff)

// Slower: UserId isn't in ORDER BY
context.Events.Where(e => e.UserId == userId)
```

### Use Partitions

```csharp
// Fast: Partition pruning kicks in
context.Events.Where(e => e.Timestamp >= startOfMonth)

// Full scan if partitioned by month but filtering by UserId
context.Events.Where(e => e.UserId == userId)
```

### Sampling for Large Datasets

```csharp
// Approximate count (much faster on huge tables)
var approxCount = await context.Events
    .Sample(0.01)  // 1% sample
    .CountAsync() * 100;
```

## Summary

| SQL Server/PostgreSQL | ClickHouse |
|----------------------|------------|
| Transactions | Eventual consistency |
| UPDATE/DELETE | Bulk mutations, append, merge, or rewrite |
| IDENTITY | UUID or app-generated |
| Foreign keys enforced | Application-level only |
| Row-at-a-time OK | Batch thousands of rows |
| Index on any column | ORDER BY is the index |
| OLTP | OLAP |

Embrace ClickHouse's strengths (analytics, throughput, columnar storage) and design around its limitations for best results.
