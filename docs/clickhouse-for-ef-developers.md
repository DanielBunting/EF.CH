# ClickHouse for EF Developers

If you are coming from SQL Server, PostgreSQL, or MySQL with Entity Framework Core, ClickHouse will feel fundamentally different. This page explains the mental model shifts, maps familiar patterns to ClickHouse equivalents, and identifies anti-patterns that will cause problems.

## The Core Mental Model Shift

Traditional RDBMS databases are built for **transactional workloads**: individual row inserts, updates, deletes, and lookups by primary key. ClickHouse is built for **analytical workloads**: scanning billions of rows across columns, aggregating data, and returning results fast.

| Aspect | SQL Server / PostgreSQL | ClickHouse |
|--------|------------------------|------------|
| Optimized for | Row-level CRUD | Column scans and aggregation |
| Data model | Normalized, relational | Denormalized, append-only |
| Write pattern | Single-row inserts and updates | Bulk inserts of thousands+ rows |
| Update model | In-place updates | Append-only; mutations are async background operations |
| Consistency | ACID transactions | Eventual consistency for mutations |
| Index model | B-tree per column | Sparse primary index on ORDER BY key |
| Primary key | Uniqueness constraint + clustered index | Sort order hint; no uniqueness guarantee |

## Append-Only, Not Update-in-Place

In SQL Server, `UPDATE Users SET Name = 'Alice' WHERE Id = 1` modifies the row in place. The old value is gone (unless you have temporal tables).

In ClickHouse, there is no in-place update. When you "update" a row, ClickHouse creates an `ALTER TABLE UPDATE` mutation that rewrites entire data parts in the background. This is:

- **Asynchronous** -- the mutation may take seconds or minutes to complete
- **Expensive** -- it rewrites all parts that contain matching rows
- **Not transactional** -- other queries may see old data until the mutation finishes

```csharp
// This works, but triggers an async ALTER TABLE mutation
await context.Users
    .Where(u => u.Id == userId)
    .ExecuteUpdateAsync(s => s.SetProperty(u => u.Name, "Alice"));
```

```sql
ALTER TABLE "Users" UPDATE "Name" = 'Alice' WHERE "Id" = userId
```

> **Note:** Standard `SaveChanges()` does not support updates or deletes in EF.CH. Use `ExecuteUpdateAsync()` and `ExecuteDeleteAsync()` instead. These map to ClickHouse's `ALTER TABLE UPDATE` and lightweight `DELETE` operations.

## ENGINE Is Required

Every ClickHouse table must declare a storage engine. There is no default. If you forget to call `UseMergeTree()` (or another engine method), the table creation will fail.

```csharp
// SQL Server -- no engine to think about
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(o => o.Id);
});

// ClickHouse -- ENGINE is mandatory
modelBuilder.Entity<Order>(entity =>
{
    entity.HasNoKey();
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
});
```

The engine you choose determines how data is stored, merged, and queried. See [Engines Overview](engines/overview.md) for guidance on choosing the right engine.

## No Auto-Increment, No Foreign Keys, No Transactions

### No auto-increment IDs

ClickHouse has no `IDENTITY` or `SERIAL` column type. Use UUIDs instead:

```csharp
public class Event
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
}
```

Or generate UUIDs server-side:

```csharp
entity.Property(x => x.Id).HasDefaultExpression("generateUUIDv4()");
```

### No foreign keys

ClickHouse does not enforce foreign key constraints. There are no `HasOne`, `HasMany`, or `Include` relationships. If you need to join data from a lookup table, use ClickHouse dictionaries:

```csharp
// Instead of navigation properties and Include()
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromTable(projection: c => new CountryLookup { Id = c.Id, Name = c.Name })
    .UseHashedLayout()
);
```

### No transactions

ClickHouse does not support multi-statement transactions. EF.CH returns a no-op transaction object to satisfy the EF Core interface, but no isolation is provided. Each statement executes independently.

## ORDER BY Is Your "Primary Key"

In SQL Server, the clustered index (usually the primary key) determines physical row order. In ClickHouse, the `ORDER BY` clause in the engine definition serves the same purpose:

```csharp
// SQL Server mental model: primary key = clustered index
entity.HasKey(o => o.Id);

// ClickHouse mental model: ORDER BY = data sort order and sparse index
entity.UseMergeTree(x => new { x.OrderDate, x.CustomerId });
```

The ORDER BY key determines:

- How data is physically sorted on disk
- Which columns appear in the sparse primary index
- Which queries can skip granules (blocks of rows) efficiently

Choose ORDER BY columns based on your most frequent `WHERE` and `GROUP BY` patterns, not based on uniqueness.

## Pattern Comparison Table

| SQL Server / EF Core Pattern | ClickHouse Equivalent | EF.CH API |
|-----------------------------|-----------------------|-----------|
| `entity.HasKey(x => x.Id)` | ORDER BY key (not a uniqueness constraint) | `entity.UseMergeTree(x => x.Id)` |
| `IDENTITY` / auto-increment | UUID generation | `Guid.NewGuid()` or `HasDefaultExpression("generateUUIDv4()")` |
| `entity.HasIndex(x => x.Col)` | Skip index (bloom filter, minmax, set) | `entity.HasIndex(x => x.Col).UseBloomFilter()` |
| `Include(x => x.Related)` | Denormalize or use dictionaries | `entity.AsDictionary<TLookup, TSource>(...)` |
| `SaveChanges()` for updates | `ExecuteUpdateAsync()` | `query.ExecuteUpdateAsync(s => s.SetProperty(...))` |
| `SaveChanges()` for deletes | `ExecuteDeleteAsync()` | `query.ExecuteDeleteAsync()` |
| `SaveChanges()` for inserts | `SaveChanges()` (small) or `BulkInsertAsync()` (large) | `context.BulkInsertAsync(entities)` |
| `DbContext.Database.BeginTransaction()` | Not supported (no-op) | N/A |
| Partitioned table (SQL Server Enterprise) | `PARTITION BY` (every engine) | `entity.HasPartitionByMonth(x => x.Date)` |
| Table-valued parameters | Temporary tables | `query.ToTempTableAsync(context)` |
| Materialized/indexed views | Materialized views (first-class) | `entity.AsMaterializedView<TView, TSource>(query)` |
| `COUNT(DISTINCT col)` | `uniq()` (approximate) or `uniqExact()` (exact) | `group.Uniq(x => x.Col)` or `group.UniqExact(x => x.Col)` |
| `ROW_NUMBER() OVER(...)` | `LIMIT N BY columns` | `.LimitBy(5, x => x.Category)` |

## Anti-Patterns to Avoid

### Frequent small inserts

ClickHouse is optimized for bulk writes. Each insert creates a new data part that must eventually be merged. Inserting one row at a time creates excessive parts and degrades performance.

```csharp
// Bad: inserting one row at a time in a loop
foreach (var item in items)
{
    context.Events.Add(item);
    await context.SaveChangesAsync();  // creates a new part each time
}

// Good: batch all inserts together
await context.BulkInsertAsync(items);
```

A good rule of thumb: insert at least 1,000 rows per batch. For high-throughput pipelines, batch 10,000-100,000 rows.

### Row-level updates

Do not model ClickHouse tables as mutable entities that get updated frequently. Each update triggers an `ALTER TABLE UPDATE` that rewrites data parts.

```csharp
// Bad: updating individual rows frequently
await context.Users
    .Where(u => u.Id == id)
    .ExecuteUpdateAsync(s => s.SetProperty(u => u.LastSeen, DateTime.UtcNow));
// This rewrites entire data parts containing that row

// Good: append new events, query the latest state
context.UserEvents.Add(new UserEvent
{
    UserId = id,
    EventType = "seen",
    Timestamp = DateTime.UtcNow
});
await context.SaveChangesAsync();
```

If you need "latest state" semantics, use `ReplacingMergeTree` with `.Final()`:

```csharp
entity.UseReplacingMergeTree(x => x.Version, x => new { x.UserId });

// Query the deduplicated view
var latestStates = await context.UserStates.Final().ToListAsync();
```

### Relying on transactions

Do not design workflows that depend on multi-statement atomicity. ClickHouse does not support transactions. If a batch of operations partially fails, there is no rollback.

```csharp
// Bad: assuming transactional behavior
using var transaction = await context.Database.BeginTransactionAsync();
// This returns a no-op transaction -- no isolation is provided
```

Design for idempotency instead. Use `ReplacingMergeTree` with version columns so that duplicate inserts resolve to the latest version.

### Using SaveChanges for updates/deletes

EF Core's change tracker does not support ClickHouse mutations. `SaveChanges()` works for inserts only.

```csharp
// Bad: modifying tracked entities and saving
var user = await context.Users.FirstAsync(u => u.Id == id);
user.Name = "Alice";
await context.SaveChangesAsync();  // will not work for updates

// Good: use ExecuteUpdateAsync
await context.Users
    .Where(u => u.Id == id)
    .ExecuteUpdateAsync(s => s.SetProperty(u => u.Name, "Alice"));
```

## Best Practices

### Batch inserts

Always insert data in batches. Use `BulkInsertAsync` for large volumes and `SaveChangesAsync` only for small batches.

```csharp
await context.BulkInsertAsync(records, cfg => cfg.BatchSize(50_000));
```

### Design for immutability

Model your data as append-only event streams. Instead of updating rows, insert new events and derive current state through queries.

### Use FINAL for ReplacingMergeTree

When you need the latest version of deduplicated rows, always query with `.Final()`. Without it, you may see duplicate rows from different data parts that have not yet been merged.

```csharp
var current = await context.UserProfiles.Final().ToListAsync();
```

### Choose ORDER BY for your queries

The ORDER BY key is the most important performance decision. Put your most-filtered columns first:

```csharp
// If most queries filter by tenant_id and date range:
entity.UseMergeTree(x => new { x.TenantId, x.EventDate, x.Id });
```

### Use PREWHERE for wide tables

For tables with many columns where your filter touches only a few, `PreWhere` reads only the filter columns before loading the rest:

```csharp
var results = await context.WideTable
    .PreWhere(x => x.Status == "active")
    .Where(x => x.Value > 100)
    .ToListAsync();
```

### Partition by time

Time-based partitioning enables efficient data lifecycle management (TTL, partition drops) and query pruning:

```csharp
entity.UseMergeTree(x => new { x.EventDate, x.Id })
    .HasPartitionByMonth(x => x.EventDate)
    .HasTtl(x => x.EventDate, TimeSpan.FromDays(90));
```

## See Also

- [Getting Started](getting-started.md) -- installation and first project walkthrough
- [Limitations](limitations.md) -- unsupported EF Core features and workarounds
- [Engines Overview](engines/overview.md) -- choosing the right table engine
- [MergeTree Engine](engines/mergetree.md) -- standard columnar storage configuration
- [ReplacingMergeTree Engine](engines/replacing-mergetree.md) -- deduplication by version column
