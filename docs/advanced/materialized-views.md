# Materialized Views

Materialized views in ClickHouse automatically transform and aggregate data as it is inserted into a source table. EF.CH provides two approaches: a type-safe LINQ definition and a raw SQL definition.

## LINQ-Based Definition

Define the view using strongly-typed LINQ expressions. The provider translates the query to ClickHouse SQL at migration time.

```csharp
public class Order
{
    public int Id { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public decimal Amount { get; set; }
}

public class DailySummary
{
    public DateTime Date { get; set; }
    public int ProductId { get; set; }
    public decimal TotalAmount { get; set; }
    public int OrderCount { get; set; }
}
```

In `OnModelCreating`:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
});

modelBuilder.Entity<DailySummary>(entity =>
{
    entity.AsMaterializedView<DailySummary, Order>(
        query: orders => orders
            .GroupBy(o => new { o.OrderDate.Date, o.ProductId })
            .Select(g => new DailySummary
            {
                Date = g.Key.Date,
                ProductId = g.Key.ProductId,
                TotalAmount = g.Sum(o => o.Amount),
                OrderCount = g.Count()
            }),
        populate: false
    );
});
```

Generated DDL:

```sql
CREATE MATERIALIZED VIEW "DailySummary"
ENGINE = MergeTree() ORDER BY ("Date", "ProductId")
AS SELECT
    toDate("OrderDate") AS "Date",
    "ProductId",
    sum("Amount") AS "TotalAmount",
    count() AS "OrderCount"
FROM "Order"
GROUP BY toDate("OrderDate"), "ProductId";
```

## Raw SQL Definition

When the LINQ translation does not cover a specific ClickHouse feature, use `AsMaterializedViewRaw` to provide the SELECT statement directly.

```csharp
modelBuilder.Entity<DailySummary>(entity =>
{
    entity.AsMaterializedViewRaw(
        sourceTable: "Orders",
        selectSql: @"SELECT
            toDate(OrderDate) AS Date,
            ProductId,
            sum(Amount) AS TotalAmount,
            count() AS OrderCount
        FROM Orders
        GROUP BY Date, ProductId",
        populate: false
    );
});
```

## Null Engine Source Pattern

A common pattern pairs a Null engine source table with a materialized view. The Null engine discards inserted rows after the view processes them, saving storage when only the aggregated data is needed.

```csharp
// Source table: Null engine discards raw events after the view processes them
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.UseNullEngine();
});

// Materialized view: stores the aggregated result
modelBuilder.Entity<EventSummary>(entity =>
{
    entity.AsMaterializedView<EventSummary, RawEvent>(
        query: events => events
            .GroupBy(e => e.EventType)
            .Select(g => new EventSummary
            {
                EventType = g.Key,
                Count = g.Count()
            }),
        populate: false
    );
    entity.UseMergeTree(x => x.EventType);
});
```

Inserts into `RawEvent` flow through the materialized view into `EventSummary`. The raw rows are discarded by the Null engine.

## Populate Option

The `populate` parameter controls whether existing data in the source table is backfilled into the view when it is created.

| Value | Behavior |
|-------|----------|
| `false` (default) | Only new inserts after view creation are processed. |
| `true` | Existing rows in the source table are processed during CREATE. |

> **Note:** Using `populate: true` on a large source table blocks inserts during backfill and may cause duplicate data if rows are inserted while the view is being created. For large tables, create the view with `populate: false` and backfill manually with `INSERT INTO ... SELECT ...`.

## See Also

- [Migrations Overview](../migrations/overview.md) -- materialized views are created during migrations
- [Phase Ordering](../migrations/phase-ordering.md) -- how EF.CH orders view creation relative to tables
- [Dictionaries](dictionaries.md) -- another advanced DDL feature with similar lifecycle
