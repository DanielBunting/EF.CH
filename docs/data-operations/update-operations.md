# Update Operations

ClickHouse does not support standard SQL `UPDATE` statements. Instead, updates are performed via `ALTER TABLE ... UPDATE` mutations, which asynchronously rewrite data parts in the background.

EF.CH translates `ExecuteUpdateAsync` to ClickHouse's mutation syntax automatically.

## Basic Update

```csharp
await context.Events
    .Where(e => e.Status == "pending")
    .ExecuteUpdateAsync(s => s
        .SetProperty(e => e.Status, "processed")
        .SetProperty(e => e.ProcessedAt, DateTime.UtcNow));
```

Generated SQL:

```sql
ALTER TABLE "Events" UPDATE
    "Status" = 'processed', "ProcessedAt" = now('UTC')
WHERE "Status" = 'pending'
```

## WHERE 1 Requirement

ClickHouse requires a WHERE clause on all `ALTER TABLE UPDATE` statements. If your `ExecuteUpdateAsync` call has no `Where()` filter, EF.CH emits `WHERE 1` automatically:

```csharp
await context.Events
    .ExecuteUpdateAsync(s => s
        .SetProperty(e => e.Version, e => e.Version + 1));
```

Generated SQL:

```sql
ALTER TABLE "Events" UPDATE
    "Version" = "Version" + 1
WHERE 1
```

You do not need to add a dummy predicate -- the provider handles this transparently.

## Computed Set Expressions

The `SetProperty` lambda can reference other columns and use expressions:

```csharp
await context.Orders
    .Where(o => o.Status == "shipped")
    .ExecuteUpdateAsync(s => s
        .SetProperty(o => o.Total, o => o.Amount * o.Quantity)
        .SetProperty(o => o.UpdatedAt, DateTime.UtcNow));
```

Generated SQL:

```sql
ALTER TABLE "Orders" UPDATE
    "Total" = "Amount" * "Quantity", "UpdatedAt" = now('UTC')
WHERE "Status" = 'shipped'
```

## No Table Aliases

ClickHouse mutations do not support table aliases. EF.CH automatically uses unqualified column names in the generated SQL. Write your LINQ normally -- the provider strips table qualifiers during SQL generation:

```csharp
await context.Events
    .Where(e => e.EventType == "click" && e.Amount > 100)
    .ExecuteUpdateAsync(s => s
        .SetProperty(e => e.Processed, true));
```

Generated SQL:

```sql
ALTER TABLE "Events" UPDATE
    "Processed" = true
WHERE "EventType" = 'click' AND "Amount" > 100
```

## Asynchronous Processing

Mutations are asynchronous by nature. The `ExecuteUpdateAsync` call returns after ClickHouse has accepted the mutation, but the actual data rewrite happens in the background. If subsequent queries depend on the updated values, add a delay:

```csharp
await context.Events
    .Where(e => e.Status == "pending")
    .ExecuteUpdateAsync(s => s
        .SetProperty(e => e.Status, "processed"));

// Allow time for the mutation to process
await Task.Delay(500);

var pending = await context.Events
    .Where(e => e.Status == "pending")
    .CountAsync();
```

In production systems, consider checking `system.mutations` to verify mutation completion rather than relying on a fixed delay.

## Limitations

- **Single table only.** ClickHouse mutations operate on one table at a time. Queries that join multiple tables in the WHERE clause are not supported and will throw an exception.
- **No transactions.** Mutations cannot be rolled back. Design updates to be idempotent where possible.
- **Performance cost.** Each mutation rewrites affected data parts entirely. Frequent small updates on large tables can cause excessive I/O. Batch updates together when possible.

## See Also

- [Delete Operations](delete-operations.md) -- Lightweight and mutation DELETE strategies
- [OPTIMIZE TABLE](optimize.md) -- Force merges after mutations
- [INSERT...SELECT](insert-select.md) -- Alternative: insert transformed data into a new table
