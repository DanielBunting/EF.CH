# MaterializedViewSample

Demonstrates ClickHouse materialized views as INSERT triggers for automatic data aggregation.

## What This Shows

- Defining a materialized view with `AsMaterializedViewRaw()`
- Using `SummingMergeTree` as the target engine
- Automatic aggregation when inserting into source table
- Querying aggregated data from the view

## How Materialized Views Work

In ClickHouse, materialized views are **INSERT triggers**, not cached query results:

1. You insert data into the source table (`Orders`)
2. ClickHouse runs the view's SELECT query on new rows
3. Results are inserted into the target table (`DailySales_MV`)

```
Orders (source)           Materialized View        DailySales_MV (target)
     │                          │                         │
     │  INSERT order            │                         │
     ├──────────────────────────►  GROUP BY + SUM         │
     │                          ├─────────────────────────►
     │                          │                   Aggregated row
```

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Materialized View Sample
========================

Creating database and tables...
Inserting orders...

Inserted 5 orders.
Materialized view automatically aggregated the data.

--- Daily Sales Summary (from materialized view) ---
  2024-01-15 | PROD-001: 8 units, $159.92
  2024-01-15 | PROD-002: 1 units, $149.99
  2024-01-14 | PROD-001: 10 units, $199.90
  2024-01-14 | PROD-002: 2 units, $299.98

--- Raw Orders (source table) ---
  2024-01-15 | PROD-001: 5 units, $99.95
  2024-01-15 | PROD-001: 3 units, $59.97
  2024-01-15 | PROD-002: 1 units, $149.99
  2024-01-14 | PROD-001: 10 units, $199.90
  2024-01-14 | PROD-002: 2 units, $299.98

--- Adding more orders ---
Added 1 more order.

--- Updated Daily Sales Summary ---
  2024-01-15 | PROD-001: 10 units, $199.90
  2024-01-15 | PROD-002: 1 units, $149.99
  2024-01-14 | PROD-001: 10 units, $199.90
  2024-01-14 | PROD-002: 2 units, $299.98

Done!
```

## Key Code

### Source Table

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
});
```

### Materialized View

```csharp
modelBuilder.Entity<DailySales>(entity =>
{
    entity.HasNoKey();
    entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });

    entity.AsMaterializedViewRaw(
        sourceTable: "Orders",
        selectSql: @"
            SELECT
                toDate(""OrderDate"") AS ""Date"",
                ""ProductId"",
                sum(""Quantity"") AS ""TotalQuantity"",
                sum(""Revenue"") AS ""TotalRevenue""
            FROM ""Orders""
            GROUP BY ""Date"", ""ProductId""
        ",
        populate: false);
});
```

## Engine Choice

`SummingMergeTree` is used because:
- ORDER BY columns (`Date`, `ProductId`) define the aggregation key
- Numeric columns (`TotalQuantity`, `TotalRevenue`) are automatically summed during merges
- Multiple inserts for the same key are eventually merged

## Learn More

- [Materialized Views Documentation](../../docs/features/materialized-views.md)
- [SummingMergeTree Documentation](../../docs/engines/summing-mergetree.md)
