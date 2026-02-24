# Window Functions

Window functions perform calculations across a set of rows related to the current row without collapsing the result set. EF.CH provides a fluent C# API that translates to ClickHouse window function SQL.

All types live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## API Styles

Two equivalent styles are available. Both produce the same SQL.

### Fluent style

Chain methods on the builder returned by `Window.*()`. Terminate with `.Build()` to get the result value.

```csharp
var results = await context.Orders.Select(o => new
{
    o.Id,
    o.Region,
    o.OrderDate,
    RowNum = Window.RowNumber()
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Build()
}).ToListAsync();
```

### Lambda style

Pass a lambda that configures the window specification. No `.Build()` call needed.

```csharp
var results = await context.Orders.Select(o => new
{
    o.Id,
    o.Region,
    o.OrderDate,
    RowNum = Window.RowNumber(w => w
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate))
}).ToListAsync();
```

Both generate:

```sql
SELECT o."Id", o."Region", o."OrderDate",
    row_number() OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC) AS "RowNum"
FROM "Orders" AS o
```

## Ranking Functions

### ROW_NUMBER

Sequential number within a partition, starting at 1.

```csharp
RowNum = Window.RowNumber()
    .PartitionBy(o.Region)
    .OrderBy(o.OrderDate)
    .Build()
```

```sql
row_number() OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC)
```

### RANK

Rank with gaps for ties. If two rows tie for rank 2, the next row gets rank 4.

```csharp
Rnk = Window.Rank()
    .PartitionBy(o.Category)
    .OrderByDescending(o.Amount)
    .Build()
```

```sql
rank() OVER (PARTITION BY o."Category" ORDER BY o."Amount" DESC)
```

### DENSE_RANK

Rank without gaps for ties. If two rows tie for rank 2, the next row gets rank 3.

```csharp
DRnk = Window.DenseRank()
    .OrderByDescending(o.Score)
    .Build()
```

```sql
dense_rank() OVER (ORDER BY o."Score" DESC)
```

### PERCENT_RANK

Relative rank: `(rank - 1) / (total rows in partition - 1)`. Returns a value between 0 and 1.

```csharp
PctRank = Window.PercentRank()
    .PartitionBy(o.Category)
    .OrderBy(o.Amount)
    .Build()
```

```sql
percent_rank() OVER (PARTITION BY o."Category" ORDER BY o."Amount" ASC)
```

### NTILE

Divides rows into a specified number of roughly equal buckets.

```csharp
Quartile = Window.NTile(4)
    .OrderBy(o.Amount)
    .Build()
```

```sql
ntile(4) OVER (ORDER BY o."Amount" ASC)
```

## Value Functions

### LAG

Returns the value from a previous row. Translates to ClickHouse's `lagInFrame`.

```csharp
// Previous row (offset = 1)
PrevAmount = Window.Lag(o.Amount)
    .PartitionBy(o.UserId)
    .OrderBy(o.OrderDate)
    .Build()

// 3 rows back
ThirdPrev = Window.Lag(o.Amount, 3)
    .PartitionBy(o.UserId)
    .OrderBy(o.OrderDate)
    .Build()

// With default value when no previous row exists
PrevOrZero = Window.Lag(o.Amount, 1, 0m)
    .PartitionBy(o.UserId)
    .OrderBy(o.OrderDate)
    .Build()
```

```sql
lagInFrame(o."Amount", 1) OVER (PARTITION BY o."UserId" ORDER BY o."OrderDate" ASC)
lagInFrame(o."Amount", 3) OVER (PARTITION BY o."UserId" ORDER BY o."OrderDate" ASC)
lagInFrame(o."Amount", 1, 0) OVER (PARTITION BY o."UserId" ORDER BY o."OrderDate" ASC)
```

### LEAD

Returns the value from a subsequent row. Translates to ClickHouse's `leadInFrame`.

```csharp
NextAmount = Window.Lead(o.Amount)
    .PartitionBy(o.UserId)
    .OrderBy(o.OrderDate)
    .Build()
```

```sql
leadInFrame(o."Amount", 1) OVER (PARTITION BY o."UserId" ORDER BY o."OrderDate" ASC)
```

### FIRST_VALUE, LAST_VALUE, NTH_VALUE

```csharp
First = Window.FirstValue(o.Amount)
    .PartitionBy(o.Region)
    .OrderBy(o.OrderDate)
    .Build()

Last = Window.LastValue(o.Amount)
    .PartitionBy(o.Region)
    .OrderBy(o.OrderDate)
    .Rows().UnboundedPreceding().UnboundedFollowing()
    .Build()

Third = Window.NthValue(o.Amount, 3)
    .PartitionBy(o.Region)
    .OrderBy(o.OrderDate)
    .Build()
```

```sql
first_value(o."Amount") OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC)
last_value(o."Amount") OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
nth_value(o."Amount", 3) OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC)
```

## Aggregate Window Functions

Standard aggregates can be used as window functions to compute running or partitioned values.

### Running total

```csharp
var results = await context.Orders.Select(o => new
{
    o.Id,
    o.Region,
    o.Amount,
    o.OrderDate,
    RunningTotal = Window.Sum(o.Amount)
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Rows().UnboundedPreceding().CurrentRow()
        .Build()
}).ToListAsync();
```

```sql
SELECT o."Id", o."Region", o."Amount", o."OrderDate",
    sum(o."Amount") OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "RunningTotal"
FROM "Orders" AS o
```

### Available aggregate window functions

| Method | SQL |
|--------|-----|
| `Window.Sum(value)` | `sum(value) OVER (...)` |
| `Window.Avg(value)` | `avg(value) OVER (...)` |
| `Window.Min(value)` | `min(value) OVER (...)` |
| `Window.Max(value)` | `max(value) OVER (...)` |
| `Window.Count(value)` | `count(value) OVER (...)` |
| `Window.Count()` | `count() OVER (...)` |

## Frame Specification

Control which rows are included in the window frame using `Rows()` or `Range()` followed by boundary methods.

### Frame types

| Method | SQL |
|--------|-----|
| `.Rows()` | `ROWS BETWEEN ...` (physical row offsets) |
| `.Range()` | `RANGE BETWEEN ...` (logical value range) |

### Frame boundaries

| Method | SQL |
|--------|-----|
| `.UnboundedPreceding()` | `UNBOUNDED PRECEDING` |
| `.CurrentRow()` | `CURRENT ROW` |
| `.UnboundedFollowing()` | `UNBOUNDED FOLLOWING` |
| `.Preceding(n)` | `n PRECEDING` |
| `.Following(n)` | `n FOLLOWING` |

### Example: 3-row moving average

```csharp
MovingAvg = Window.Avg(o.Amount)
    .PartitionBy(o.Region)
    .OrderBy(o.OrderDate)
    .Rows().Preceding(2).CurrentRow()
    .Build()
```

```sql
avg(o."Amount") OVER (PARTITION BY o."Region" ORDER BY o."OrderDate" ASC
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
```

## Multiple Window Functions in One Query

Multiple window functions can be used in the same Select projection:

```csharp
var results = await context.Orders.Select(o => new
{
    o.Id,
    o.Region,
    o.Amount,
    RowNum = Window.RowNumber()
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Build(),
    RunningTotal = Window.Sum(o.Amount)
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Rows().UnboundedPreceding().CurrentRow()
        .Build(),
    PrevAmount = Window.Lag(o.Amount, 1, 0m)
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Build()
}).ToListAsync();
```

## See Also

- [LIMIT BY](limit-by.md) -- Simpler top-N per group without window functions
- [GROUP BY Modifiers](group-by-modifiers.md) -- WITH ROLLUP, WITH CUBE, WITH TOTALS
- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
