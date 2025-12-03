# SummingMergeTree Engine

SummingMergeTree automatically sums numeric columns when rows with the same ORDER BY key are merged. This is ideal for pre-aggregated metrics and counters.

## When to Use

- Daily/hourly metrics aggregation
- Counter tables (page views, clicks, revenue)
- Materialized view targets
- Any data where you need running totals by key

## How It Works

1. You insert rows with numeric values
2. Rows with the same ORDER BY key accumulate during merges
3. Numeric columns are summed; non-numeric columns take an arbitrary value

```
Insert: Metrics(date="2024-01-01", product="A", sales=100)
Insert: Metrics(date="2024-01-01", product="A", sales=50)

Before merge: Two rows exist
After merge:  One row with sales=150
```

## Configuration

### Basic Setup

```csharp
public class DailySales
{
    public DateOnly Date { get; set; }
    public string ProductId { get; set; } = string.Empty;
    public long TotalQuantity { get; set; }   // Will be summed
    public decimal TotalRevenue { get; set; } // Will be summed
}

public class MyDbContext : DbContext
{
    public DbSet<DailySales> DailySales => Set<DailySales>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailySales>(entity =>
        {
            entity.HasNoKey();  // Typically keyless
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
        });
    }
}
```

### Specifying Columns to Sum

By default, all numeric columns are summed. To sum specific columns only:

```csharp
public class Metrics
{
    public DateOnly Date { get; set; }
    public string Category { get; set; } = string.Empty;
    public long Views { get; set; }      // Sum this
    public long Clicks { get; set; }     // Sum this
    public double AvgDuration { get; set; }  // Don't sum (use AggregatingMergeTree for averages)
}
```

**Note:** Averaging requires AggregatingMergeTree with aggregate functions. SummingMergeTree only sums.

## Generated DDL

```csharp
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
entity.HasPartitionByMonth(x => x.Date);
```

Generates:

```sql
CREATE TABLE "DailySales" (
    "Date" Date NOT NULL,
    "ProductId" String NOT NULL,
    "TotalQuantity" Int64 NOT NULL,
    "TotalRevenue" Decimal(18, 4) NOT NULL
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM("Date")
ORDER BY ("Date", "ProductId")
```

## Usage Examples

### Incrementing Counters

```csharp
// Each insert adds to the total
context.DailySales.Add(new DailySales
{
    Date = DateOnly.FromDateTime(DateTime.Today),
    ProductId = "PROD-001",
    TotalQuantity = 5,
    TotalRevenue = 99.95m
});

context.DailySales.Add(new DailySales
{
    Date = DateOnly.FromDateTime(DateTime.Today),
    ProductId = "PROD-001",
    TotalQuantity = 3,
    TotalRevenue = 59.97m
});

await context.SaveChangesAsync();

// After merge: TotalQuantity=8, TotalRevenue=159.92
```

### Querying Aggregates

```csharp
// Get totals by product (may need FINAL for accuracy)
var productTotals = await context.DailySales
    .GroupBy(s => s.ProductId)
    .Select(g => new
    {
        ProductId = g.Key,
        TotalQuantity = g.Sum(s => s.TotalQuantity),
        TotalRevenue = g.Sum(s => s.TotalRevenue)
    })
    .ToListAsync();
```

### With Materialized View

SummingMergeTree is commonly used as a materialized view target:

```csharp
// Source table (raw orders)
modelBuilder.Entity<Order>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.OrderDate, x.Id });
});

// Aggregation target (materialized view)
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
                TotalRevenue = g.Sum(o => o.Total)
            }),
        populate: false);
});
```

### Time-Based Aggregation

```csharp
public class HourlyMetrics
{
    public DateTime Hour { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public long RequestCount { get; set; }
    public long ErrorCount { get; set; }
    public long TotalResponseTimeMs { get; set; }
}

modelBuilder.Entity<HourlyMetrics>(entity =>
{
    entity.HasNoKey();
    entity.UseSummingMergeTree(x => new { x.Hour, x.Endpoint });
    entity.HasPartitionByMonth(x => x.Hour);
});

// Insert metrics
context.HourlyMetrics.Add(new HourlyMetrics
{
    Hour = new DateTime(2024, 1, 15, 14, 0, 0),  // Truncated to hour
    Endpoint = "/api/users",
    RequestCount = 1,
    ErrorCount = 0,
    TotalResponseTimeMs = 45
});
```

## Best Practices

### Always Group in ORDER BY

Include all grouping dimensions in ORDER BY:

```csharp
// Good: All dimensions in ORDER BY
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId, x.Region });

// Bad: Missing dimension - will merge incorrectly
entity.UseSummingMergeTree(x => new { x.Date });  // Products will be summed together!
```

### Use Integer Types for Counts

```csharp
public long ViewCount { get; set; }  // Good: Int64
public int ClickCount { get; set; }  // Good: Int32

// Avoid floats for counts (precision issues)
public double ViewCount { get; set; }  // Not ideal
```

### Pre-Truncate Time Values

```csharp
// Truncate to hour before inserting
var truncatedHour = new DateTime(
    timestamp.Year, timestamp.Month, timestamp.Day,
    timestamp.Hour, 0, 0);

context.HourlyMetrics.Add(new HourlyMetrics
{
    Hour = truncatedHour,  // Consistent grouping
    ...
});
```

Or use ClickHouse functions in materialized views:

```csharp
.Select(g => new HourlyMetrics
{
    Hour = g.Key.Timestamp.ToStartOfHour(),  // Uses toStartOfHour()
    ...
})
```

## Querying Considerations

### Pre-Merge vs Post-Merge

Before merging completes, queries may see unmerged rows:

```csharp
// May count rows multiple times before merge
var total = await context.DailySales
    .Where(s => s.Date == today)
    .SumAsync(s => s.TotalRevenue);

// To get accurate results, use GROUP BY
var total = await context.DailySales
    .Where(s => s.Date == today)
    .GroupBy(s => s.ProductId)
    .Select(g => g.Sum(s => s.TotalRevenue))
    .SumAsync();
```

### FINAL with SummingMergeTree

`FINAL` forces merge-like behavior:

```csharp
// Force immediate summation
var totals = await context.DailySales
    .Final()
    .ToListAsync();
```

## Limitations

- **Only Sums**: Can't compute averages, min, max (use AggregatingMergeTree)
- **Non-Numeric Columns**: Take arbitrary value from merged rows
- **No Subtraction**: Can't decrement (use CollapsingMergeTree for that)
- **Merge Timing**: Results may be approximate until merges complete

## When Not to Use

| Scenario | Use Instead |
|----------|-------------|
| Need averages/min/max | [AggregatingMergeTree](aggregating-mergetree.md) |
| Need to subtract/cancel | [CollapsingMergeTree](collapsing-mergetree.md) |
| Raw event storage | [MergeTree](mergetree.md) |
| Deduplication by key | [ReplacingMergeTree](replacing-mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [Materialized Views](../features/materialized-views.md)
- [AggregatingMergeTree](aggregating-mergetree.md) - For complex aggregates
- [ClickHouse SummingMergeTree Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/summingmergetree)
