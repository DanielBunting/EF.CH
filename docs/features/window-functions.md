# Window Functions

Window functions perform calculations across a set of rows related to the current row, using the `OVER` clause. They're essential for analytics queries like ranking, running totals, and accessing previous/next row values.

## Why Window Functions?

1. **Ranking and Ordering**: Assign row numbers, ranks, or percentiles within partitions
2. **Running Calculations**: Compute running totals, averages, or counts
3. **Row Comparison**: Access values from previous or next rows (lag/lead)
4. **Partitioned Analytics**: Calculate per-group metrics without GROUP BY

## API Styles

EF.CH supports two equivalent API styles. Choose whichever you prefer:

### Lambda Style (Recommended)

```csharp
using EF.CH.Extensions;

var result = context.Orders.Select(o => new
{
    o.Id,
    RowNum = Window.RowNumber(w => w
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)),
    PrevAmount = Window.Lag(o.Amount, 1, w => w
        .OrderBy(o.OrderDate)),
    RunningTotal = Window.Sum(o.Amount, w => w
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Rows().UnboundedPreceding().CurrentRow())
});
```

**Advantages:**
- No `.Build()` required at end
- Cleaner syntax in anonymous types
- Clear separation of function and OVER clause

### Fluent Style

```csharp
var result = context.Orders.Select(o => new
{
    o.Id,
    RowNum = Window.RowNumber()
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Build(),  // Required for correct type inference
    RunningTotal = Window.Sum(o.Amount)
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Rows().UnboundedPreceding().CurrentRow()
        .Build()
});
```

**Note:** The fluent style requires `.Build()` at the end of the chain to ensure correct type inference in anonymous types.

## Available Functions

### Ranking Functions

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `Window.RowNumber()` | `row_number()` | Sequential number within partition, starting at 1 |
| `Window.Rank()` | `rank()` | Rank with gaps for ties |
| `Window.DenseRank()` | `dense_rank()` | Rank without gaps for ties |
| `Window.PercentRank()` | `percent_rank()` | Relative rank (0 to 1) |
| `Window.NTile(n)` | `ntile(n)` | Divide rows into n roughly equal buckets |

### Value Functions

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `Window.Lag(value)` | `lagInFrame(value, 1)` | Previous row's value |
| `Window.Lag(value, offset)` | `lagInFrame(value, offset)` | Value N rows back |
| `Window.Lag(value, offset, default)` | `lagInFrame(value, offset, default)` | With default for missing rows |
| `Window.Lead(value)` | `leadInFrame(value, 1)` | Next row's value |
| `Window.Lead(value, offset)` | `leadInFrame(value, offset)` | Value N rows ahead |
| `Window.Lead(value, offset, default)` | `leadInFrame(value, offset, default)` | With default for missing rows |
| `Window.FirstValue(value)` | `first_value(value)` | First value in the frame |
| `Window.LastValue(value)` | `last_value(value)` | Last value in the frame |
| `Window.NthValue(value, n)` | `nth_value(value, n)` | Nth value in the frame (1-based) |

### Aggregate Window Functions

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `Window.Sum(value)` | `sum(value)` | Sum over the window frame |
| `Window.Avg(value)` | `avg(value)` | Average over the window frame |
| `Window.Count(value)` | `count(value)` | Count non-null values in frame |
| `Window.Count()` | `count()` | Count all rows in frame |
| `Window.Min(value)` | `min(value)` | Minimum value in frame |
| `Window.Max(value)` | `max(value)` | Maximum value in frame |

## Builder Methods

Both API styles support the same builder methods:

### Partitioning and Ordering

```csharp
// Lambda style
Window.RowNumber(w => w
    .PartitionBy(o.Region)           // PARTITION BY (can chain multiple)
    .PartitionBy(o.Category)         // Multiple partition columns
    .OrderBy(o.OrderDate)            // ORDER BY ... ASC
    .OrderByDescending(o.Amount))    // ORDER BY ... DESC

// Fluent style
Window.RowNumber()
    .PartitionBy(o.Region)
    .OrderBy(o.OrderDate)
    .Build()
```

### Frame Specification

Control which rows are included in the window frame:

```csharp
// Frame type
.Rows()                    // ROWS - physical row boundaries
.Range()                   // RANGE - logical value boundaries

// Frame start boundary
.UnboundedPreceding()      // UNBOUNDED PRECEDING
.Preceding(n)              // N PRECEDING
.CurrentRow()              // CURRENT ROW

// Frame end boundary
.CurrentRow()              // CURRENT ROW
.Following(n)              // N FOLLOWING
.UnboundedFollowing()      // UNBOUNDED FOLLOWING
```

**Common Frame Patterns:**

```csharp
// Running total (all rows up to current)
.Rows().UnboundedPreceding().CurrentRow()
// → ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

// 3-row moving average
.Rows().Preceding(2).CurrentRow()
// → ROWS BETWEEN 2 PRECEDING AND CURRENT ROW

// Entire partition
.Rows().UnboundedPreceding().UnboundedFollowing()
// → ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
```

## Complete Example

```csharp
using EF.CH.Extensions;

public class SalesAnalytics
{
    public Guid OrderId { get; set; }
    public string Region { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Amount { get; set; }

    // Window function results
    public long RowNumber { get; set; }
    public long RegionRank { get; set; }
    public decimal? PreviousAmount { get; set; }
    public decimal? RunningTotal { get; set; }
    public double? MovingAverage { get; set; }
}

var analytics = await context.Orders
    .Select(o => new SalesAnalytics
    {
        OrderId = o.Id,
        Region = o.Region,
        OrderDate = o.OrderDate,
        Amount = o.Amount,

        // Row number within each region
        RowNumber = Window.RowNumber(w => w
            .PartitionBy(o.Region)
            .OrderBy(o.OrderDate)),

        // Rank by amount within region (highest = 1)
        RegionRank = Window.Rank(w => w
            .PartitionBy(o.Region)
            .OrderByDescending(o.Amount)),

        // Previous order amount
        PreviousAmount = Window.Lag(o.Amount, 1, w => w
            .PartitionBy(o.Region)
            .OrderBy(o.OrderDate)),

        // Running total within region
        RunningTotal = Window.Sum(o.Amount, w => w
            .PartitionBy(o.Region)
            .OrderBy(o.OrderDate)
            .Rows().UnboundedPreceding().CurrentRow()),

        // 3-order moving average
        MovingAverage = Window.Avg(o.Amount, w => w
            .PartitionBy(o.Region)
            .OrderBy(o.OrderDate)
            .Rows().Preceding(2).CurrentRow())
    })
    .ToListAsync();
```

## Generated SQL

The above query generates:

```sql
SELECT
    "o"."Id" AS "OrderId",
    "o"."Region",
    "o"."OrderDate",
    "o"."Amount",
    row_number() OVER (PARTITION BY "o"."Region" ORDER BY "o"."OrderDate" ASC) AS "RowNumber",
    rank() OVER (PARTITION BY "o"."Region" ORDER BY "o"."Amount" DESC) AS "RegionRank",
    lagInFrame("o"."Amount", 1) OVER (
        PARTITION BY "o"."Region" ORDER BY "o"."OrderDate" ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS "PreviousAmount",
    sum("o"."Amount") OVER (
        PARTITION BY "o"."Region" ORDER BY "o"."OrderDate" ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "RunningTotal",
    avg("o"."Amount") OVER (
        PARTITION BY "o"."Region" ORDER BY "o"."OrderDate" ASC
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS "MovingAverage"
FROM "orders" AS "o"
```

## ClickHouse-Specific Notes

### lagInFrame and leadInFrame

ClickHouse uses `lagInFrame` and `leadInFrame` instead of standard `LAG` and `LEAD`. These functions require a frame specification to work correctly, so EF.CH automatically adds `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` when no explicit frame is specified.

### Memory Considerations

Window functions are computed in memory. For very large result sets:
- Use `LIMIT` to restrict output
- Consider pre-aggregating with materialized views
- Partition data appropriately to reduce frame sizes

### No WINDOW Clause

ClickHouse doesn't support the SQL `WINDOW` clause for named window definitions. Each window function must specify its full OVER clause.

## Use Cases

### Running Totals

```csharp
var runningTotals = context.Transactions.Select(t => new
{
    t.Date,
    t.Amount,
    RunningBalance = Window.Sum(t.Amount, w => w
        .OrderBy(t.Date)
        .Rows().UnboundedPreceding().CurrentRow())
});
```

### Period-over-Period Comparison

```csharp
var comparison = context.Sales.Select(s => new
{
    s.Month,
    s.Revenue,
    PrevMonth = Window.Lag(s.Revenue, 1, w => w.OrderBy(s.Month)),
    Growth = s.Revenue - Window.Lag(s.Revenue, 1, 0m, w => w.OrderBy(s.Month))
});
```

### Top N Per Group

```csharp
var top3PerRegion = context.Orders
    .Select(o => new
    {
        o.Id,
        o.Region,
        o.Amount,
        Rank = Window.RowNumber(w => w
            .PartitionBy(o.Region)
            .OrderByDescending(o.Amount))
    })
    .Where(x => x.Rank <= 3);
```

### Moving Averages

```csharp
var smoothed = context.Metrics.Select(m => new
{
    m.Timestamp,
    m.Value,
    MovingAvg7Day = Window.Avg(m.Value, w => w
        .OrderBy(m.Timestamp)
        .Rows().Preceding(6).CurrentRow())
});
```

## Limitations

- Window functions cannot be used in `WHERE`, `GROUP BY`, or `HAVING` clauses
- Window functions in subqueries require the outer query to reference the result
- Combining multiple window functions with different partitions may impact performance

## See Also

- [Materialized Views](materialized-views.md) - Pre-aggregate data on INSERT
- [Projections](projections.md) - Table-level aggregation optimizations
- [ClickHouse Window Functions](https://clickhouse.com/docs/en/sql-reference/window-functions) - Official documentation
