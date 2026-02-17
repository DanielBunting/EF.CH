# Date Truncation & Bucketing

ClickHouse provides functions for rounding dates down to various intervals — essential for time-series analytics. EF.CH exposes these as `EF.Functions` extensions that translate directly to ClickHouse SQL.

## Why Date Truncation?

Time-series queries almost always need to group events into buckets:

```csharp
// Group page views by hour
var hourly = await context.PageViews
    .GroupBy(p => EF.Functions.ToStartOfHour(p.ViewedAt))
    .Select(g => new { Hour = g.Key, Count = g.Count() })
    .OrderBy(x => x.Hour)
    .ToListAsync();
```

Generates:
```sql
SELECT toStartOfHour("ViewedAt") AS "Hour", count() AS "Count"
FROM "PageViews"
GROUP BY toStartOfHour("ViewedAt")
ORDER BY toStartOfHour("ViewedAt") ASC
```

Without these functions you'd need raw SQL for every time-bucket query.

## Available Functions

### Truncation Functions

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `ToStartOfYear(dt)` | `toStartOfYear(dt)` | First day of the year |
| `ToStartOfQuarter(dt)` | `toStartOfQuarter(dt)` | First day of the quarter |
| `ToStartOfMonth(dt)` | `toStartOfMonth(dt)` | First day of the month |
| `ToStartOfWeek(dt)` | `toStartOfWeek(dt)` | Start of the week (Sunday) |
| `ToMonday(dt)` | `toMonday(dt)` | Most recent Monday |
| `ToStartOfDay(dt)` | `toStartOfDay(dt)` | Midnight of the day |
| `ToStartOfHour(dt)` | `toStartOfHour(dt)` | Start of the hour |
| `ToStartOfMinute(dt)` | `toStartOfMinute(dt)` | Start of the minute |
| `ToStartOfFiveMinutes(dt)` | `toStartOfFiveMinutes(dt)` | Nearest 5-minute boundary |
| `ToStartOfFifteenMinutes(dt)` | `toStartOfFifteenMinutes(dt)` | Nearest 15-minute boundary |

All functions take a `DateTime` and return a `DateTime`.

### DateDiff

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `DateDiff(unit, start, end)` | `dateDiff(unit, start, end)` | `long` | Difference in the given unit |

Valid units: `'second'`, `'minute'`, `'hour'`, `'day'`, `'week'`, `'month'`, `'quarter'`, `'year'`.

## Usage Examples

### Dashboard Time Bucketing

```csharp
using EF.CH.Extensions;

// 5-minute resolution for real-time dashboards
var metrics = await context.Events
    .GroupBy(e => EF.Functions.ToStartOfFiveMinutes(e.Timestamp))
    .Select(g => new
    {
        Bucket = g.Key,
        RequestCount = g.Count(),
        AvgLatency = g.Average(e => e.LatencyMs)
    })
    .OrderByDescending(x => x.Bucket)
    .Take(100)
    .ToListAsync();
```

### Weekly Aggregation (Monday-Based)

```csharp
// Group by ISO week (Monday start)
var weekly = await context.Orders
    .GroupBy(o => EF.Functions.ToMonday(o.OrderDate))
    .Select(g => new
    {
        WeekOf = g.Key,
        Revenue = g.Sum(o => o.Amount),
        OrderCount = g.Count()
    })
    .ToListAsync();
```

### Age Calculation with DateDiff

```csharp
// How many days since each order
var orderAges = await context.Orders
    .Select(o => new
    {
        o.Id,
        DaysAgo = EF.Functions.DateDiff("day", o.OrderDate, DateTime.UtcNow)
    })
    .Where(x => x.DaysAgo > 30)
    .ToListAsync();
```

Generates:
```sql
SELECT "Id", dateDiff('day', "OrderDate", now('UTC')) AS "DaysAgo"
FROM "Orders"
WHERE dateDiff('day', "OrderDate", now('UTC')) > 30
```

### Combining with Partitioning

Date truncation pairs naturally with ClickHouse partitioning:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);
});

// This query benefits from partition pruning when filtered by date range
var monthly = await context.Events
    .Where(e => e.Timestamp >= startDate)
    .GroupBy(e => EF.Functions.ToStartOfMonth(e.Timestamp))
    .Select(g => new { Month = g.Key, Count = g.Count() })
    .ToListAsync();
```

## Notes

- These functions run server-side — they don't pull data to the client for date manipulation.
- `ToStartOfWeek` uses Sunday as the first day of the week. Use `ToMonday` for ISO week (Monday start).
- `DateDiff` returns `long` (ClickHouse `Int64`).
- All truncation functions work with both `DateTime` columns and `DateTime.UtcNow`/`DateTime.Now`.

## Learn More

- [ClickHouse Date/Time Functions](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions)
- [Partitioning](partitioning.md)
