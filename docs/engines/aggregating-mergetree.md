# AggregatingMergeTree Engine

AggregatingMergeTree stores intermediate aggregation states that can be merged. Use it when you need complex aggregates like averages, distinct counts, or percentiles that SummingMergeTree can't handle.

## When to Use

- Pre-computed averages, medians, percentiles
- Distinct counts (count unique users)
- Complex aggregations in materialized views
- When SummingMergeTree's simple sums aren't enough

## How It Works

Instead of storing final values, AggregatingMergeTree stores **aggregate function states**:

1. Insert rows with `-State` aggregate function values
2. During merges, states are combined using `-Merge`
3. Query with `-Merge` to get final results

```
Insert: Stats(date="2024-01-01", avgState=avgState(100))
Insert: Stats(date="2024-01-01", avgState=avgState(200))

After merge: avgState contains combined state
Query with avgMerge(avgState) → 150
```

## Configuration

### Basic Setup

```csharp
public class DailyStats
{
    public DateOnly Date { get; set; }
    public string ProductId { get; set; } = string.Empty;

    // These store aggregate states, not final values
    public long TotalCount { get; set; }        // For count
    public decimal TotalRevenue { get; set; }   // For sum
    // Note: Averages require raw SQL or SimpleAggregateFunction
}

public class MyDbContext : DbContext
{
    public DbSet<DailyStats> DailyStats => Set<DailyStats>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailyStats>(entity =>
        {
            entity.HasNoKey();
            entity.UseAggregatingMergeTree(x => new { x.Date, x.ProductId });
        });
    }
}
```

### Using SimpleAggregateFunction

For simpler aggregates like max, min, any:

```csharp
public class ProductStats
{
    public string ProductId { get; set; } = string.Empty;
    public long TotalSales { get; set; }
    public decimal MaxPrice { get; set; }
    public decimal MinPrice { get; set; }
}

modelBuilder.Entity<ProductStats>(entity =>
{
    entity.HasNoKey();
    entity.UseAggregatingMergeTree(x => x.ProductId);

    // Configure SimpleAggregateFunction for each column
    entity.Property(e => e.TotalSales)
        .HasSimpleAggregateFunction("sum");
    entity.Property(e => e.MaxPrice)
        .HasSimpleAggregateFunction("max");
    entity.Property(e => e.MinPrice)
        .HasSimpleAggregateFunction("min");
});
```

## Generated DDL

```csharp
entity.UseAggregatingMergeTree(x => new { x.Date, x.ProductId });
entity.Property(e => e.MaxPrice).HasSimpleAggregateFunction("max");
```

Generates:

```sql
CREATE TABLE "ProductStats" (
    "Date" Date NOT NULL,
    "ProductId" String NOT NULL,
    "TotalSales" SimpleAggregateFunction(sum, Int64) NOT NULL,
    "MaxPrice" SimpleAggregateFunction(max, Decimal(18, 4)) NOT NULL
)
ENGINE = AggregatingMergeTree
ORDER BY ("Date", "ProductId")
```

## SimpleAggregateFunction Types

| Function | Description | Use Case |
|----------|-------------|----------|
| `sum` | Sum values | Totals, counts |
| `max` | Maximum value | High water marks |
| `min` | Minimum value | Low water marks |
| `any` | Any value (arbitrary) | Non-aggregated columns |
| `anyLast` | Last inserted value | Latest state |
| `groupBitAnd` | Bitwise AND | Flag combinations |
| `groupBitOr` | Bitwise OR | Flag combinations |
| `groupBitXor` | Bitwise XOR | Checksums |

## Usage Examples

### With Materialized View (Raw SQL)

For complex aggregates, use raw SQL materialized views:

```csharp
modelBuilder.Entity<HourlyStats>(entity =>
{
    entity.HasNoKey();
    entity.UseAggregatingMergeTree(x => new { x.Hour, x.Endpoint });
    entity.AsMaterializedViewRaw(
        sourceTable: "ApiRequests",
        selectSql: @"
            SELECT
                toStartOfHour(Timestamp) AS Hour,
                Endpoint,
                countState() AS RequestCountState,
                avgState(ResponseTimeMs) AS AvgResponseTimeState,
                maxState(ResponseTimeMs) AS MaxResponseTimeState
            FROM ApiRequests
            GROUP BY Hour, Endpoint",
        populate: false);
});
```

Query with merge functions:

```csharp
var stats = await context.Database.SqlQueryRaw<StatsResult>(@"
    SELECT
        Hour,
        Endpoint,
        countMerge(RequestCountState) AS RequestCount,
        avgMerge(AvgResponseTimeState) AS AvgResponseTime,
        maxMerge(MaxResponseTimeState) AS MaxResponseTime
    FROM HourlyStats
    GROUP BY Hour, Endpoint
").ToListAsync();
```

### Inserting Aggregate States

When inserting directly (not via materialized view):

```csharp
// For SimpleAggregateFunction columns, insert raw values
context.ProductStats.Add(new ProductStats
{
    ProductId = "PROD-001",
    TotalSales = 100,    // Will be summed
    MaxPrice = 29.99m,   // Will keep max
    MinPrice = 29.99m    // Will keep min
});

context.ProductStats.Add(new ProductStats
{
    ProductId = "PROD-001",
    TotalSales = 50,
    MaxPrice = 39.99m,
    MinPrice = 19.99m
});

await context.SaveChangesAsync();

// After merge: TotalSales=150, MaxPrice=39.99, MinPrice=19.99
```

### Querying Results

```csharp
// SimpleAggregateFunction columns return merged values directly
var stats = await context.ProductStats
    .Where(s => s.ProductId == "PROD-001")
    .FirstOrDefaultAsync();

// For full AggregateFunction columns, use raw SQL with -Merge
```

## AggregatingMergeTree vs SummingMergeTree

| Feature | SummingMergeTree | AggregatingMergeTree |
|---------|------------------|----------------------|
| Sum | Yes (automatic) | Yes (with sum) |
| Count | Via sum column | Yes (countState) |
| Average | No | Yes (avgState) |
| Max/Min | No | Yes |
| Percentiles | No | Yes (quantileState) |
| Distinct Count | No | Yes (uniqState) |
| Complexity | Simple | More complex |

## Best Practices

### Use SimpleAggregateFunction When Possible

SimpleAggregateFunction is easier to work with and doesn't require special query syntax:

```csharp
// Prefer this
entity.Property(e => e.MaxValue).HasSimpleAggregateFunction("max");

// Over raw AggregateFunction which requires -Merge in queries
```

### Combine with Materialized Views

AggregatingMergeTree shines when used as a materialized view target:

```csharp
// Source: Raw events
// Target: Pre-aggregated stats with AggregatingMergeTree
```

### Match ORDER BY to Query Patterns

```csharp
// If you always query by date range first
entity.UseAggregatingMergeTree(x => new { x.Date, x.ProductId });

// If you always filter by product first
entity.UseAggregatingMergeTree(x => new { x.ProductId, x.Date });
```

## Limitations

- **Complex Queries**: Full AggregateFunction requires `-Merge` syntax
- **LINQ Limited**: Some aggregates only work via raw SQL
- **State Storage**: Aggregate states take more space than final values
- **Not for Simple Sums**: Use SummingMergeTree for pure summation

## When Not to Use

| Scenario | Use Instead |
|----------|-------------|
| Only need sums | [SummingMergeTree](summing-mergetree.md) |
| Raw event storage | [MergeTree](mergetree.md) |
| Deduplication | [ReplacingMergeTree](replacing-mergetree.md) |
| State changes | [CollapsingMergeTree](collapsing-mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [SummingMergeTree](summing-mergetree.md) - Simpler aggregation
- [Materialized Views](../features/materialized-views.md)
- [ClickHouse AggregatingMergeTree Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/aggregatingmergetree)
