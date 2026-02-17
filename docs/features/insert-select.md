# INSERT ... SELECT

EF.CH provides server-side INSERT ... SELECT operations that move or copy data between tables without transferring data to the client. This is ideal for ETL operations, data archival, and table-to-table transformations.

## Why INSERT ... SELECT?

Standard EF Core data movement patterns have significant overhead:

- Fetch data to client memory
- Deserialize entities
- Re-serialize for insert
- Network round-trip latency
- Memory pressure from large datasets

Server-side INSERT ... SELECT bypasses these bottlenecks:

- Data never leaves the ClickHouse server
- No client memory usage for data
- Single SQL statement execution
- Leverages ClickHouse's distributed processing

**Typical performance improvement: Orders of magnitude faster for large datasets.**

## Basic Usage

### Same-Type Insert

Copy data between tables of the same entity type:

```csharp
using EF.CH.Extensions;

// Copy all events from Events to ArchivedEvents
var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Status == "Completed"));

Console.WriteLine($"Inserted rows in {result.Elapsed.TotalMilliseconds}ms");
Console.WriteLine($"SQL: {result.Sql}");
```

### Cross-Type Insert with Mapping

Copy data between different entity types using a mapping expression:

```csharp
// Copy from Event to ArchivedEvent (different types)
var result = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Category == "Important"),
    e => new ArchivedEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Category = e.Category,
        Amount = e.Amount
    });
```

### Reverse Fluent API

Use the `.InsertIntoAsync()` extension on any `IQueryable`:

```csharp
// More fluent syntax
var result = await context.Events
    .Where(e => e.Category == "Electronics")
    .InsertIntoAsync(context.ArchivedEvents,
        e => new ArchivedEvent
        {
            Id = e.Id,
            Timestamp = e.Timestamp,
            Category = e.Category,
            Amount = e.Amount
        });
```

## Result Object

The `ClickHouseInsertSelectResult` contains:

```csharp
public record ClickHouseInsertSelectResult
{
    public long RowsAffected { get; }     // Number of rows inserted (may be 0 for some operations)
    public TimeSpan Elapsed { get; }       // Execution time
    public string Sql { get; }             // Generated SQL for debugging
}
```

## Use Cases

### Data Archival

Move old records to an archive table:

```csharp
var cutoffDate = DateTime.UtcNow.AddMonths(-6);

await context.ArchivedOrders.ExecuteInsertFromQueryAsync(
    context.Orders.Where(o => o.CreatedAt < cutoffDate),
    o => new ArchivedOrder
    {
        Id = o.Id,
        CreatedAt = o.CreatedAt,
        Total = o.Total,
        ArchivedAt = DateTime.UtcNow
    });

// Then delete the archived records from the source table
await context.Orders.Where(o => o.CreatedAt < cutoffDate)
    .ExecuteDeleteAsync();
```

### Filtered Copy

Copy specific records based on criteria:

```csharp
// Copy high-value orders to a separate table
await context.HighValueOrders.ExecuteInsertFromQueryAsync(
    context.Orders.Where(o => o.Total > 10000),
    o => new HighValueOrder
    {
        OrderId = o.Id,
        Amount = o.Total,
        Customer = o.CustomerName
    });
```

### Table Cloning

Copy all records from one table to another:

```csharp
// Clone entire table
await context.BackupEvents.ExecuteInsertFromQueryAsync(
    context.Events,
    e => new BackupEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Data = e.Data
    });
```

## Computed Columns

Computed columns (defined with `HasComputedColumnSql()`) are automatically excluded from the INSERT column list. They are calculated server-side based on other column values.

```csharp
// Entity with computed column
modelBuilder.Entity<Summary>(entity =>
{
    entity.Property(e => e.Total)
        .HasComputedColumnSql("Quantity * Price", stored: true);
});

// INSERT...SELECT will exclude Total from INSERT columns
// It will be computed automatically
await context.Summaries.ExecuteInsertFromQueryAsync(
    context.LineItems.Select(l => new Summary
    {
        Id = l.Id,
        Quantity = l.Quantity,
        Price = l.Price
        // Total is computed, not inserted
    }));
```

## Parameterized Queries

INSERT...SELECT fully supports captured variables in your LINQ queries:

```csharp
// DateTime parameters
var cutoff = DateTime.UtcNow.AddDays(-7);
await context.Archive.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Timestamp < cutoff),
    e => new ArchivedEvent { ... });

// Multiple captured variables
var category = "Important";
var minAmount = 100m;
await context.Archive.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Category == category && e.Amount > minAmount),
    e => new ArchivedEvent { ... });
```

Parameters are automatically extracted from your LINQ expressions and inlined as SQL literals in the generated INSERT...SELECT statement.

## Limitations

### Different Entity Types Require Mapping

When source and target are different entity types, you must provide a mapping expression:

```csharp
// Different types - mapping required
await context.TargetTable.ExecuteInsertFromQueryAsync(
    context.SourceTable,
    s => new TargetEntity { ... });  // Mapping required
```

## Generated SQL

The generated SQL wraps the source query in a subquery to ensure column alignment:

```sql
INSERT INTO "ArchivedEvents" ("Id", "Amount", "Category", "Timestamp")
SELECT "Id", "Amount", "Category", "Timestamp" FROM (
    SELECT "e"."Id", "e"."Timestamp", "e"."Category", "e"."Amount"
    FROM "Events" AS "e"
    WHERE "e"."Category" = 'Electronics'
) AS __subquery
```

This ensures:
- Only insertable columns are included (computed columns excluded)
- Column order matches the INSERT statement
- Complex queries (JOINs, aggregations) work correctly

## Best Practices

1. **Use for bulk operations** - INSERT...SELECT is most beneficial for thousands of rows or more
2. **Monitor execution time** - Check `result.Elapsed` for performance tuning
3. **Use transactions carefully** - ClickHouse has limited transaction support
4. **Consider partitioning** - INSERT...SELECT respects partition keys

## See Also

- [Bulk Insert](bulk-insert.md) - For inserting data from client
- [Delete Operations](delete-operations.md) - For removing data after archival
- [Computed Columns](computed-columns.md) - For auto-calculated values
