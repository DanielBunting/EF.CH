# Materialized Views

In ClickHouse, materialized views are **INSERT triggers** that transform data as it's written. Unlike traditional database views that cache query results, ClickHouse materialized views process incoming data in real-time.

## How They Work

1. You insert data into a **source table**
2. ClickHouse runs the materialized view's SELECT query against the new rows
3. Results are inserted into a **target table** (the view's storage)

```
Source Table (Orders)     Materialized View        Target Table (DailySales)
     │                         │                         │
     │  INSERT order           │                         │
     ├─────────────────────────►  SELECT + aggregate     │
     │                         ├─────────────────────────►
     │                         │                    Aggregated row
```

## Configuration Methods

EF.CH supports two ways to define materialized views:

### 1. LINQ-Based (Type-Safe)

```csharp
modelBuilder.Entity<DailySales>(entity =>
{
    entity.HasNoKey();
    entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
    entity.AsMaterializedView<DailySales, Order>(
        query: orders => orders
            .GroupBy(o => new { Date = o.OrderDate.Date, o.ProductId })
            .Select(g => new DailySales
            {
                Date = DateOnly.FromDateTime(g.Key.Date),
                ProductId = g.Key.ProductId,
                TotalQuantity = g.Sum(o => o.Quantity),
                TotalRevenue = g.Sum(o => o.Revenue)
            }),
        populate: false);
});
```

### 2. Simple Projection (Data Transformation)

For data transformation without aggregation, use a simple `Select()` without `GroupBy()`:

```csharp
modelBuilder.Entity<ProcessedRecord>(entity =>
{
    entity.ToTable("ProcessedRecords_MV");
    entity.UseReplacingMergeTree(
        versionColumn: x => x.Version,
        orderByColumn: x => new { x.NameId, x.EventTime });

    entity.AsMaterializedView<ProcessedRecord, RawRecord>(
        query: raw => raw.Select(r => new ProcessedRecord
        {
            NameId = r.Name.CityHash64(),            // Hash for efficient storage
            EventTime = r.EventTime,
            Version = r.EventTime.ToUnixTimestamp64Milli(),  // Use timestamp as version
            Value = r.Value,
            IsActive = 1                             // Constant value
        }),
        populate: false);
});
```

This pattern is useful for:
- **ID Hashing**: Convert strings to efficient UInt64 with `CityHash64()`
- **Version Generation**: Create version numbers from timestamps with `ToUnixTimestamp64Milli()`
- **Adding Computed Columns**: Set constants or derived values
- **Data Normalization**: Transform source data without aggregation

### 3. Raw SQL (Escape Hatch)

```csharp
modelBuilder.Entity<DailySales>(entity =>
{
    entity.HasNoKey();
    entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
    entity.AsMaterializedViewRaw(
        sourceTable: "Orders",
        selectSql: @"
            SELECT
                toDate(OrderDate) AS Date,
                ProductId,
                sum(Quantity) AS TotalQuantity,
                sum(Revenue) AS TotalRevenue
            FROM Orders
            GROUP BY Date, ProductId",
        populate: false);
});
```

## Complete Example

### Source Entity

```csharp
public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal Revenue { get; set; }
}
```

### Target Entity (Materialized View)

```csharp
public class DailySales
{
    public DateOnly Date { get; set; }
    public string ProductId { get; set; } = string.Empty;
    public long TotalQuantity { get; set; }
    public decimal TotalRevenue { get; set; }
}
```

### DbContext Configuration

```csharp
public class SalesDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<DailySales> DailySales => Set<DailySales>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });

        // Materialized view (target table)
        modelBuilder.Entity<DailySales>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("DailySales_MV");
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });

            entity.AsMaterializedView<DailySales, Order>(
                query: orders => orders
                    .GroupBy(o => new { Date = o.OrderDate.Date, o.ProductId })
                    .Select(g => new DailySales
                    {
                        Date = DateOnly.FromDateTime(g.Key.Date),
                        ProductId = g.Key.ProductId,
                        TotalQuantity = g.Sum(o => o.Quantity),
                        TotalRevenue = g.Sum(o => o.Revenue)
                    }),
                populate: false);
        });
    }
}
```

## Usage

### Insert Data (Automatic Aggregation)

```csharp
// Insert orders - materialized view updates automatically
context.Orders.AddRange(new[]
{
    new Order { Id = Guid.NewGuid(), OrderDate = DateTime.Today, ProductId = "PROD-1", Quantity = 5, Revenue = 99.95m },
    new Order { Id = Guid.NewGuid(), OrderDate = DateTime.Today, ProductId = "PROD-1", Quantity = 3, Revenue = 59.97m },
    new Order { Id = Guid.NewGuid(), OrderDate = DateTime.Today, ProductId = "PROD-2", Quantity = 1, Revenue = 149.99m },
});
await context.SaveChangesAsync();

// DailySales_MV is automatically populated with aggregated data
```

### Query Aggregated Data

```csharp
// Query the materialized view directly
var dailyTotals = await context.DailySales
    .Where(s => s.Date >= DateOnly.FromDateTime(DateTime.Today.AddDays(-7)))
    .GroupBy(s => s.Date)
    .Select(g => new
    {
        Date = g.Key,
        TotalRevenue = g.Sum(s => s.TotalRevenue)
    })
    .ToListAsync();
```

## The `populate` Parameter

```csharp
entity.AsMaterializedView<TTarget, TSource>(query, populate: false);
```

- `populate: false` (default) - Only new data triggers the view
- `populate: true` - Backfill existing source data when view is created

**Warning:** `populate: true` can be slow for large tables and may lock the source table.

## Engine Selection

Choose the target engine based on your aggregation needs:

| Aggregation | Engine | Example |
|-------------|--------|---------|
| Sum totals | SummingMergeTree | Daily revenue, counts |
| Averages, percentiles | AggregatingMergeTree | Average response time |
| Latest per key | ReplacingMergeTree | Current user state |
| Append-only | MergeTree | Event copies |

### SummingMergeTree (Most Common)

```csharp
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
entity.AsMaterializedView<DailySales, Order>(
    query: orders => orders
        .GroupBy(o => new { o.OrderDate.Date, o.ProductId })
        .Select(g => new DailySales
        {
            TotalQuantity = g.Sum(o => o.Quantity),  // Summed
            TotalRevenue = g.Sum(o => o.Revenue)     // Summed
        }),
    populate: false);
```

### AggregatingMergeTree (Complex Aggregates)

For averages, use raw SQL with aggregate state functions:

```csharp
entity.UseAggregatingMergeTree(x => new { x.Hour, x.Endpoint });
entity.AsMaterializedViewRaw(
    sourceTable: "ApiRequests",
    selectSql: @"
        SELECT
            toStartOfHour(Timestamp) AS Hour,
            Endpoint,
            countState() AS RequestCount,
            avgState(ResponseTimeMs) AS AvgResponseTime
        FROM ApiRequests
        GROUP BY Hour, Endpoint",
    populate: false);
```

## Supported LINQ Operations

In materialized view queries:

| Operation | Support | Notes |
|-----------|---------|-------|
| `Select` | ✅ | Project to target entity (with or without GroupBy) |
| `GroupBy` | ✅ | For aggregation patterns |
| `Sum`, `Count`, `Min`, `Max` | ✅ | Standard aggregates (requires GroupBy) |
| `Where` | ⚠️ | Limited to source filters |
| `Join` | ❌ | Use raw SQL instead |
| `OrderBy` | ❌ | Not applicable |

## ClickHouse Functions in LINQ

The following ClickHouse functions can be used in LINQ expressions:

| C# Method | ClickHouse SQL | Description |
|-----------|----------------|-------------|
| `str.CityHash64()` | `cityHash64(str)` | Fast non-cryptographic hash to UInt64 |
| `dt.ToUnixTimestamp64Milli()` | `toUnixTimestamp64Milli(dt)` | Milliseconds since epoch (Int64) |
| `dt.ToStartOfHour()` | `toStartOfHour(dt)` | Truncate to hour |
| `dt.ToStartOfDay()` | `toStartOfDay(dt)` | Truncate to day |
| `dt.ToStartOfMonth()` | `toStartOfMonth(dt)` | Truncate to month |
| `dt.ToYYYYMM()` | `toYYYYMM(dt)` | Year-month as UInt32 |
| `dt.Date` | `toDate(dt)` | Date portion only |

## Raw SQL for Complex Views

When LINQ isn't enough:

```csharp
entity.AsMaterializedViewRaw(
    sourceTable: "Events",
    selectSql: @"
        SELECT
            toStartOfHour(Timestamp) AS Hour,
            EventType,
            countIf(Status = 'success') AS SuccessCount,
            countIf(Status = 'error') AS ErrorCount,
            quantile(0.95)(ResponseTimeMs) AS P95ResponseTime
        FROM Events
        GROUP BY Hour, EventType",
    populate: false);
```

## DateTime Functions

Use ClickHouse functions for time bucketing:

```csharp
// LINQ with extension methods
.GroupBy(o => new { Hour = o.Timestamp.ToStartOfHour() })

// Or raw SQL
toStartOfHour(Timestamp)
toStartOfDay(Timestamp)
toStartOfMonth(Timestamp)
toYYYYMM(Timestamp)
```

## Multiple Views Per Source

You can have multiple materialized views on one source:

```csharp
// Hourly summary
modelBuilder.Entity<HourlySales>(entity =>
{
    entity.UseSummingMergeTree(x => new { x.Hour, x.ProductId });
    entity.AsMaterializedView<HourlySales, Order>(...);
});

// Daily summary
modelBuilder.Entity<DailySales>(entity =>
{
    entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
    entity.AsMaterializedView<DailySales, Order>(...);
});

// Product totals (all-time)
modelBuilder.Entity<ProductTotals>(entity =>
{
    entity.UseSummingMergeTree(x => x.ProductId);
    entity.AsMaterializedView<ProductTotals, Order>(...);
});
```

## Generated DDL

The configuration generates:

```sql
-- Target table
CREATE TABLE "DailySales_MV" (
    "Date" Date NOT NULL,
    "ProductId" String NOT NULL,
    "TotalQuantity" Int64 NOT NULL,
    "TotalRevenue" Decimal(18, 4) NOT NULL
)
ENGINE = SummingMergeTree
ORDER BY ("Date", "ProductId")

-- Materialized view
CREATE MATERIALIZED VIEW "DailySales_MV_view"
TO "DailySales_MV"
AS SELECT
    toDate("OrderDate") AS "Date",
    "ProductId",
    sum("Quantity") AS "TotalQuantity",
    sum("Revenue") AS "TotalRevenue"
FROM "Orders"
GROUP BY "Date", "ProductId"
```

## Limitations

- **Insert-Only**: Views trigger on INSERT, not UPDATE/DELETE
- **No Joins in LINQ**: Use raw SQL for multi-table views
- **Backfill Caution**: `populate: true` can be expensive
- **Schema Changes**: Altering views requires drop and recreate

## Best Practices

### Match ORDER BY to GROUP BY

```csharp
// Good: ORDER BY matches GROUP BY
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
// GROUP BY Date, ProductId

// Bad: Mismatch causes inefficient storage
entity.UseSummingMergeTree(x => x.Date);
// GROUP BY Date, ProductId  -- ProductId values won't merge correctly
```

### Use Appropriate Time Granularity

```csharp
// Hourly for high-volume, recent queries
.GroupBy(o => o.Timestamp.ToStartOfHour())

// Daily for dashboards and reports
.GroupBy(o => o.OrderDate.Date)

// Monthly for long-term trends
.GroupBy(o => o.OrderDate.ToStartOfMonth())
```

### Keep Views Simple

Complex views are harder to maintain. Consider cascading views:

```
Orders → HourlySales → DailySales → MonthlySales
```

## See Also

- [SummingMergeTree](../engines/summing-mergetree.md)
- [AggregatingMergeTree](../engines/aggregating-mergetree.md)
- [DateTime Functions](../types/datetime.md)
- [ClickHouse Materialized Views Docs](https://clickhouse.com/docs/en/guides/developer/cascading-materialized-views)
