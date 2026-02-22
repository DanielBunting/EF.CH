# Delete Operations

ClickHouse supports two fundamentally different delete strategies. EF.CH maps `ExecuteDeleteAsync` to the configured strategy, defaulting to lightweight deletes.

## Delete Strategies

| Strategy | SQL | Behavior |
|----------|-----|----------|
| **Lightweight** (default) | `DELETE FROM "Table" WHERE ...` | Marks rows as deleted immediately; physical deletion happens during background merges |
| **Mutation** | `ALTER TABLE "Table" DELETE WHERE ...` | Schedules an asynchronous rewrite of affected data parts |

### Lightweight DELETE

The default strategy. Rows are logically deleted on execution and excluded from subsequent queries. Physical removal occurs during background merges.

```csharp
var deleted = await context.Events
    .Where(e => e.Timestamp < cutoff)
    .ExecuteDeleteAsync();
```

Generated SQL:

```sql
DELETE FROM "Events"
WHERE "Timestamp" < '2024-01-01 00:00:00'
```

Lightweight deletes return the number of affected rows and are the recommended approach for most use cases.

### Mutation DELETE

Mutation deletes rewrite entire data parts in the background. They are asynchronous -- the SQL statement returns before the data is physically removed.

```csharp
var deleted = await context.Events
    .Where(e => e.Timestamp < cutoff)
    .ExecuteDeleteAsync();
```

Generated SQL (when configured for mutations):

```sql
ALTER TABLE "Events" DELETE
WHERE "Timestamp" < '2024-01-01 00:00:00'
```

**Important:** Mutations are asynchronous. After the call returns, the data may still be visible in queries for a short period while ClickHouse rewrites the affected parts. If your subsequent logic depends on the data being gone, add a delay:

```csharp
await context.Events
    .Where(e => e.Timestamp < cutoff)
    .ExecuteDeleteAsync();

// Allow time for the mutation to process
await Task.Delay(500);

var remaining = await context.Events.CountAsync();
```

## Configuring the Delete Strategy

Set the strategy in `UseClickHouse` options:

```csharp
options.UseClickHouse(connectionString, o => o
    .UseDeleteStrategy(ClickHouseDeleteStrategy.Lightweight));
```

Or for mutation-based deletes:

```csharp
options.UseClickHouse(connectionString, o => o
    .UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

The strategy applies globally to all `ExecuteDeleteAsync` calls on the context.

## No Table Aliases in Mutations

ClickHouse mutations do not support table aliases. EF.CH automatically suppresses table qualifiers in column references when generating DELETE statements. You do not need to do anything special -- just write standard LINQ predicates:

```csharp
await context.Events
    .Where(e => e.EventType == "expired" && e.Amount < 10)
    .ExecuteDeleteAsync();
```

Generated SQL (lightweight):

```sql
DELETE FROM "Events"
WHERE "EventType" = 'expired' AND "Amount" < 10
```

Notice that column references are unqualified (`"EventType"` instead of `e."EventType"`) because ClickHouse does not allow aliases in this context.

## Combining with LINQ

Standard LINQ operators can be used to build the predicate:

```csharp
// Delete by date range
await context.Events
    .Where(e => e.Timestamp >= start && e.Timestamp < end)
    .ExecuteDeleteAsync();

// Delete by list of IDs
var idsToDelete = new[] { id1, id2, id3 };
await context.Events
    .Where(e => idsToDelete.Contains(e.Id))
    .ExecuteDeleteAsync();
```

## Lightweight vs. Mutation: When to Use Which

| Consideration | Lightweight | Mutation |
|---------------|-------------|----------|
| Returns row count | Yes | No |
| Data visibility | Immediate logical delete | Async; data visible briefly after call |
| Performance impact | Minimal; piggybacks on merges | Triggers a rewrite of affected parts |
| Use case | Normal application deletes | Bulk maintenance, TTL-like cleanup |
| ClickHouse version | 23.3+ | All versions |

For most applications, the default lightweight strategy is the right choice. Use mutations only for bulk maintenance operations where you need to physically reclaim disk space and can tolerate the asynchronous behavior.

## See Also

- [Update Operations](update-operations.md) -- ALTER TABLE UPDATE mutations
- [INSERT...SELECT](insert-select.md) -- Archive data before deleting
- [OPTIMIZE TABLE](optimize.md) -- Force merges to physically remove deleted data
