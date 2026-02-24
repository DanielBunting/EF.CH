# INSERT...SELECT

Server-side `INSERT INTO ... SELECT` operations move data between tables entirely within ClickHouse. No data is transferred to the client and back -- the server reads from the source query and writes directly to the target table. This is ideal for archiving, aggregation rollups, and data transformations.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Same Entity Type

When source and target tables share the same schema, pass the source query directly:

```csharp
var cutoff = DateTime.UtcNow.AddDays(-90);

var result = await context.Events
    .Where(e => e.Timestamp < cutoff)
    .InsertIntoAsync(context.ArchivedEvents);
```

Generated SQL:

```sql
INSERT INTO "ArchivedEvents" ("Id", "Timestamp", "EventType", "Amount")
SELECT "Id", "Timestamp", "EventType", "Amount" FROM (
    SELECT e."Id", e."Timestamp", e."EventType", e."Amount"
    FROM "Events" AS e
    WHERE e."Timestamp" < '2024-10-15 00:00:00'
) AS __subquery
```

## Mapping Between Types

When source and target have different schemas, provide a mapping expression to transform the data:

```csharp
var result = await context.Orders
    .Where(o => o.Status == "completed")
    .InsertIntoAsync(context.OrderSummaries, order => new OrderSummary
    {
        OrderId = order.Id,
        Total = order.Amount * order.Quantity,
        CompletedAt = order.UpdatedAt
    });
```

Generated SQL:

```sql
INSERT INTO "OrderSummaries" ("OrderId", "Total", "CompletedAt")
SELECT "OrderId", "Total", "CompletedAt" FROM (
    SELECT o."Id" AS "OrderId", o."Amount" * o."Quantity" AS "Total",
           o."UpdatedAt" AS "CompletedAt"
    FROM "Orders" AS o
    WHERE o."Status" = 'completed'
) AS __subquery
```

The mapping expression is translated to SQL, so all standard LINQ operators and ClickHouse functions are available in the projection.

## Reverse Fluent API

The same operations are available starting from the target DbSet instead of the source query:

```csharp
// Same entity type
await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Timestamp < cutoff));

// With mapping
await context.OrderSummaries.ExecuteInsertFromQueryAsync(
    context.Orders.Where(o => o.Status == "completed"),
    order => new OrderSummary
    {
        OrderId = order.Id,
        Total = order.Amount * order.Quantity,
        CompletedAt = order.UpdatedAt
    });
```

Both styles produce identical SQL. Choose whichever reads better for your use case.

## Composing with Query Features

The source query supports all standard LINQ operators and ClickHouse-specific extensions:

```csharp
var result = await context.Events
    .Final()
    .Where(e => e.Timestamp >= startOfMonth)
    .GroupBy(e => new { e.EventType, Date = e.Timestamp.Date })
    .Select(g => new DailyAggregate
    {
        EventType = g.Key.EventType,
        Date = g.Key.Date,
        Count = g.Count(),
        TotalAmount = g.Sum(e => e.Amount)
    })
    .InsertIntoAsync(context.DailyAggregates);
```

## Result Object

Both `InsertIntoAsync` and `ExecuteInsertFromQueryAsync` return a `ClickHouseInsertSelectResult`:

| Property | Type | Description |
|----------|------|-------------|
| `RowsAffected` | `long` | Number of rows inserted |
| `Elapsed` | `TimeSpan` | Total wall-clock time |
| `Sql` | `string` | The generated SQL statement (useful for debugging) |

```csharp
var result = await sourceQuery.InsertIntoAsync(context.TargetTable);
Console.WriteLine($"Moved {result.RowsAffected} rows in {result.Elapsed.TotalSeconds:F2}s");
Console.WriteLine($"SQL: {result.Sql}");
```

**Note:** ClickHouse may not always return an accurate row count for INSERT...SELECT operations. The `RowsAffected` value should be treated as informational.

## See Also

- [Bulk Insert](bulk-insert.md) -- Client-side high-throughput inserts
- [Temporary Tables](temp-tables.md) -- Staging data for multi-step workflows
- [Delete Operations](delete-operations.md) -- Removing data after archiving
