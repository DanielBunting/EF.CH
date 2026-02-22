# Limitations

EF.CH maps Entity Framework Core concepts onto ClickHouse, but ClickHouse is a columnar analytics database, not a transactional RDBMS. Several EF Core features are unsupported or behave differently. This page catalogs each limitation and its workaround.

## Unsupported EF Core Features

### SaveChanges for Updates and Deletes

EF Core's change tracker detects modified and deleted entities and emits `UPDATE` / `DELETE` statements on `SaveChanges()`. ClickHouse does not support standard SQL `UPDATE` or `DELETE` -- it uses `ALTER TABLE UPDATE` (async mutation) and lightweight `DELETE` instead.

`SaveChanges()` works for **inserts only**. Attempting to update or delete tracked entities through the change tracker will not work.

**Workaround:** Use `ExecuteUpdateAsync` and `ExecuteDeleteAsync`:

```csharp
// Update
await context.Events
    .Where(e => e.Status == "pending")
    .ExecuteUpdateAsync(s => s.SetProperty(e => e.Status, "processed"));
```

```sql
ALTER TABLE "Events" UPDATE "Status" = 'processed' WHERE "Status" = 'pending'
```

```csharp
// Delete
await context.Events
    .Where(e => e.Timestamp < cutoff)
    .ExecuteDeleteAsync();
```

> **Note:** Mutations are asynchronous. The `ALTER TABLE UPDATE` command returns immediately, but the actual data rewrite happens in the background. Allow time for completion or call `OPTIMIZE TABLE ... FINAL` to force it.

### Transactions

ClickHouse does not support multi-statement transactions. EF.CH provides a no-op transaction object to satisfy the `IDbContextTransaction` interface, but no isolation or rollback capability exists.

```csharp
// This compiles and runs, but provides no transactional guarantees
using var transaction = await context.Database.BeginTransactionAsync();
// ... operations execute independently ...
await transaction.CommitAsync(); // no-op
```

**Workaround:** Design for idempotency. Use `ReplacingMergeTree` with version columns so duplicate inserts resolve correctly. If you need atomic multi-table writes, consider writing to a staging table and using `INSERT ... SELECT` to move data.

### Foreign Keys and Navigation Properties

ClickHouse does not enforce foreign key constraints. EF Core navigation properties (`HasOne`, `HasMany`) and eager loading (`Include`) are not supported.

**Workaround:** Denormalize your data at write time, or use ClickHouse dictionaries for lookup joins:

```csharp
// Define a dictionary for lookups
entity.AsDictionary<ProductLookup, Product>(cfg => cfg
    .HasKey(x => x.Id)
    .FromTable(projection: p => new ProductLookup { Id = p.Id, Name = p.Name })
    .UseHashedLayout()
    .HasLifetime(minSeconds: 60, maxSeconds: 300)
);

// Query using dictGet
var name = dict.Get<string>(productId, x => x.Name);
```

### Database-Generated Values (IDENTITY / Sequences)

ClickHouse has no `IDENTITY`, `SERIAL`, `AUTO_INCREMENT`, or sequence support. There are no server-generated incrementing IDs.

**Workaround:** Generate identifiers on the client:

```csharp
public class Event
{
    public Guid Id { get; set; } = Guid.NewGuid();
    // ...
}
```

Or use a server-side UUID default:

```csharp
entity.Property(x => x.Id).HasDefaultExpression("generateUUIDv4()");
```

### Eager Loading with Include

`Include()` and `ThenInclude()` rely on foreign key relationships, which ClickHouse does not support.

**Workaround:** Execute separate queries and join the results in application code, or denormalize the data into a single table. For lookup data, use dictionaries.

### Database-Generated Timestamps

EF Core's `ValueGeneratedOnAdd` and `ValueGeneratedOnUpdate` rely on database triggers or output clauses that ClickHouse does not support.

**Workaround:** Use `HasDefaultExpression` for insert-time defaults:

```csharp
entity.Property(x => x.CreatedAt).HasDefaultExpression("now()");
```

This generates a `DEFAULT now()` clause in the column definition. The value is computed on insert if no explicit value is provided.

### Migrations: Limited ALTER TABLE Support

ClickHouse's `ALTER TABLE` supports adding columns, dropping columns, modifying columns, and renaming columns. It does not support:

- Renaming tables (use `RENAME TABLE` via raw SQL)
- Changing the ORDER BY or ENGINE of an existing table
- Adding or removing partitioning from an existing table

**Workaround:** For schema changes that ClickHouse does not support via `ALTER TABLE`, create a new table with the desired schema and use `INSERT ... SELECT` to migrate data.

## ClickHouse Version-Specific Limitations

### JSON Type (requires ClickHouse 24.8+)

The `JSON` column type is only available in ClickHouse 24.8 and later. Earlier versions will return an error when creating tables with JSON columns.

```csharp
// Requires ClickHouse 24.8+
entity.Property(x => x.Metadata).HasColumnType("JSON");
```

**Workaround for older versions:** Store JSON as a `String` column and parse it in application code.

### isDeleted Column in ReplacingMergeTree (requires ClickHouse 23.2+)

The `is_deleted` column parameter in `ReplacingMergeTree` (which allows marking rows as deleted during background merges) requires ClickHouse 23.2 or later.

```csharp
// Requires ClickHouse 23.2+
entity.UseReplacingMergeTree(
    x => x.Version,
    x => new { x.Id },
    isDeletedColumn: x => x.IsDeleted
);
```

**Workaround for older versions:** Use `CollapsingMergeTree` with a sign column for state cancellation, or filter out deleted rows at query time.

### Lightweight DELETE (requires ClickHouse 23.3+)

The lightweight `DELETE FROM` syntax (as opposed to `ALTER TABLE DELETE`) was stabilized in ClickHouse 23.3.

**Workaround for older versions:** Configure the mutation-based delete strategy:

```csharp
options.UseClickHouse(connectionString, o => o
    .UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

### Date32 Extended Range (requires ClickHouse 21.9+)

`Date32` supports the extended date range 1900-2299. The standard `Date` type covers 1970-2149.

### Parameterized Views (requires ClickHouse 22.6+)

Parameterized views using `CREATE VIEW ... AS SELECT ... WHERE col = {param:Type}` syntax require ClickHouse 22.6 or later.

## Behavioral Differences from Standard EF Core

### OrNull Aggregate Translation

EF.CH translates standard LINQ aggregates differently from other EF Core providers:

| LINQ Method | SQL Server | ClickHouse (EF.CH) |
|-------------|-----------|---------------------|
| `Sum()` | `SUM(col)` | `sumOrNull(col)` |
| `Average()` | `AVG(col)` | `avgOrNull(CAST(col AS Float64))` |
| `Min()` | `MIN(col)` | `minOrNull(col)` |
| `Max()` | `MAX(col)` | `maxOrNull(col)` |

The `OrNull` variants return `NULL` for empty result sets instead of zero or throwing, which is safer for analytical queries.

### Set Operations Require Explicit Type

ClickHouse does not support bare `UNION` -- it requires `UNION ALL` or `UNION DISTINCT`. EF.CH provides explicit extension methods:

```csharp
var combined = query1.UnionAll(query2);
var distinct = query1.UnionDistinct(query2);
```

### DELETE/UPDATE Without Table Aliases

ClickHouse mutation syntax does not support table aliases in the column references. EF.CH automatically suppresses table qualifiers when generating `ALTER TABLE UPDATE` and `ALTER TABLE DELETE` statements.

### WHERE Required for UPDATE Mutations

`ALTER TABLE UPDATE` requires a `WHERE` clause. If no predicate is specified, EF.CH emits `WHERE 1` to satisfy the syntax requirement.

## Known Issues

### Async Mutation Visibility

After `ExecuteUpdateAsync` or `ExecuteDeleteAsync`, the changes may not be immediately visible to subsequent queries. ClickHouse processes mutations asynchronously in the background.

**Mitigation:** If you need to read your own writes after a mutation, add a delay or force a merge:

```csharp
await context.Events
    .Where(e => e.Old)
    .ExecuteDeleteAsync();

// Option 1: wait for mutation processing
await Task.Delay(500);

// Option 2: force merge
await context.Database.OptimizeTableFinalAsync<Event>();
```

### Large Result Sets and Memory

ClickHouse can return very large result sets. Unlike SQL Server which streams rows, the ClickHouse driver may buffer significant amounts of data in memory.

**Mitigation:** Always use `Take()` to limit result sets, or use the streaming export methods:

```csharp
// Stream large results to a file
await using var stream = File.Create("export.parquet");
await context.Events
    .Where(e => e.Date > cutoff)
    .ToFormatStreamAsync(context, "Parquet", stream);
```

### Empty Table Aggregation

Aggregating over an empty table in ClickHouse may return different results than SQL Server. For example, `COUNT()` on an empty table returns 0, but `SUM()` returns NULL (via `sumOrNull`). EF.CH's `OrNull` translation handles this correctly, but be aware of the difference if you use raw SQL.

## See Also

- [Getting Started](getting-started.md) -- installation and first project walkthrough
- [ClickHouse for EF Developers](clickhouse-for-ef-developers.md) -- mental model differences and best practices
- [Engines Overview](engines/overview.md) -- choosing the right table engine
