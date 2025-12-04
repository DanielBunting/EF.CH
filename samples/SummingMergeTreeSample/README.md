# SummingMergeTree Sample

Demonstrates automatic aggregation using SummingMergeTree engine.

## What This Shows

- Configuring SummingMergeTree for automatic summing
- Inserting metrics that accumulate over time
- Querying aggregated data
- How rows merge to reduce storage

## The Use Case

You have metrics or counters that should be summed by key:
- Daily sales by product
- Hourly page views by URL
- Request counts by endpoint

Instead of updating a single row, you insert increments and ClickHouse sums them automatically.

## How It Works

```csharp
// Configure ORDER BY as the grouping key
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
```

When you insert:
```
| Date       | ProductId | Quantity | Revenue |
|------------|-----------|----------|---------|
| 2024-01-15 | PROD-001  | 5        | 99.95   |
| 2024-01-15 | PROD-001  | 3        | 59.97   |
```

After merge:
```
| Date       | ProductId | Quantity | Revenue |
|------------|-----------|----------|---------|
| 2024-01-15 | PROD-001  | 8        | 159.92  |
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
SummingMergeTree Sample
=======================

Creating database and tables...
Inserting sales data for 2024-01-15...

  PROD-001: qty=5, revenue=$99.95
  PROD-001: qty=3, revenue=$59.97
  PROD-001: qty=2, revenue=$39.98
  PROD-002: qty=10, revenue=$149.90
  PROD-002: qty=5, revenue=$74.95

--- Totals by Product ---
  PROD-001: qty=10, revenue=$199.90
  PROD-002: qty=15, revenue=$224.85

--- Adding more sales ---
  PROD-001: qty=7, revenue=$139.93
  PROD-003: qty=1, revenue=$999.99

--- Updated Totals ---
  PROD-001: qty=17, revenue=$339.83
  PROD-002: qty=15, revenue=$224.85
  PROD-003: qty=1, revenue=$999.99

  GRAND TOTAL: qty=33, revenue=$1564.67

--- Before OPTIMIZE ---
  Physical rows: 7

--- Running OPTIMIZE FINAL ---
  Physical rows after merge: 3

--- Final Data (one row per product) ---
  PROD-001: qty=17, revenue=$339.83
  PROD-002: qty=15, revenue=$224.85
  PROD-003: qty=1, revenue=$999.99

Done!
```

## Key Code

### Entity (Keyless)

```csharp
public class DailySales
{
    public DateOnly Date { get; set; }
    public string ProductId { get; set; }
    public long Quantity { get; set; }   // Summed
    public decimal Revenue { get; set; } // Summed
}
```

### Configuration

```csharp
entity.HasNoKey();  // No primary key needed
entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
entity.HasPartitionByMonth(x => x.Date);
```

### Inserting Data

```csharp
// Just insert - values will be summed automatically
context.DailySales.Add(new DailySales
{
    Date = today,
    ProductId = "PROD-001",
    Quantity = 5,
    Revenue = 99.95m
});
```

### Querying Totals

```csharp
var totals = await context.DailySales
    .Where(s => s.Date == today)
    .GroupBy(s => s.ProductId)
    .Select(g => new
    {
        ProductId = g.Key,
        TotalQuantity = g.Sum(s => s.Quantity),
        TotalRevenue = g.Sum(s => s.Revenue)
    })
    .ToListAsync();
```

## When to Use

- Daily/hourly aggregated metrics
- Counter tables (views, clicks, sales)
- Pre-aggregation for dashboards
- Materialized view targets

## When NOT to Use

- Need averages (use AggregatingMergeTree)
- Need to subtract values (use CollapsingMergeTree)
- Non-numeric aggregations

## Learn More

- [SummingMergeTree Documentation](../../docs/engines/summing-mergetree.md)
- [Materialized Views](../../docs/features/materialized-views.md)
