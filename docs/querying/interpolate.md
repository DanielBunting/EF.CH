# WITH FILL / INTERPOLATE

ClickHouse's `ORDER BY ... WITH FILL` clause fills gaps in time series or sequence data by inserting missing rows at regular intervals. EF.CH provides typed LINQ extensions that translate to `WITH FILL` and `INTERPOLATE` SQL clauses.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Gap Filling

Fill gaps in an ORDER BY column using a step value. The query must include an `OrderBy` on the same column.

### TimeSpan step

```csharp
var hourly = await context.Readings
    .GroupBy(r => r.Hour)
    .Select(g => new { Hour = g.Key, Count = g.Count() })
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1))
    .ToListAsync();
```

Generated SQL:

```sql
SELECT r."Hour", count() AS "Count"
FROM "Readings" AS r
GROUP BY r."Hour"
ORDER BY r."Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
```

Rows with missing hours are inserted with default values (0 for Count).

### ClickHouseInterval step

Use `ClickHouseInterval` for calendar-based units that `TimeSpan` cannot represent (months, quarters, years).

```csharp
var monthly = await context.Sales
    .GroupBy(s => s.Month)
    .Select(g => new { Month = g.Key, Total = g.Sum(s => s.Amount) })
    .OrderBy(x => x.Month)
    .Interpolate(x => x.Month, ClickHouseInterval.Months(1))
    .ToListAsync();
```

Generated SQL:

```sql
SELECT s."Month", sumOrNull(s."Amount") AS "Total"
FROM "Sales" AS s
GROUP BY s."Month"
ORDER BY s."Month" ASC WITH FILL STEP INTERVAL 1 MONTH
```

### Numeric step

For integer or other numeric ORDER BY columns.

```csharp
var filled = await context.Scores
    .OrderBy(s => s.Level)
    .Interpolate(s => s.Level, 1)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT s."Level", s."Value"
FROM "Scores" AS s
ORDER BY s."Level" ASC WITH FILL STEP 1
```

## Fill with Bounds

Specify explicit FROM and TO bounds to control the range of filled values, even beyond the existing data range.

```csharp
var startHour = new DateTime(2024, 1, 1, 0, 0, 0);
var endHour = new DateTime(2024, 1, 2, 0, 0, 0);

var complete = await context.Readings
    .GroupBy(r => r.Hour)
    .Select(g => new { Hour = g.Key, Count = g.Count() })
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1), from: startHour, to: endHour)
    .ToListAsync();
```

Generated SQL:

```sql
ORDER BY "Hour" ASC WITH FILL FROM '2024-01-01 00:00:00' TO '2024-01-02 00:00:00' STEP INTERVAL 1 HOUR
```

This ensures all 24 hours in the range are present in the output, even if the source data has no readings for some hours.

## Interpolating Non-ORDER BY Columns

When gaps are filled, non-ORDER BY columns receive default values (0, empty string, NULL). The `INTERPOLATE` clause lets you control how these columns are filled.

### InterpolateMode

The `InterpolateMode` enum offers two modes:

| Mode | Behavior | SQL |
|------|----------|-----|
| `InterpolateMode.Default` | Column type's default value (0, empty, NULL) | No INTERPOLATE clause |
| `InterpolateMode.Prev` | Forward-fill from the previous non-filled row | `INTERPOLATE (column AS column)` |

```csharp
var readings = await context.Readings
    .OrderBy(r => r.Hour)
    .Interpolate(
        r => r.Hour,
        TimeSpan.FromHours(1),
        r => r.Value,
        InterpolateMode.Prev)
    .ToListAsync();
```

Generated SQL:

```sql
ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
INTERPOLATE ("Value" AS "Value")
```

### Constant fill value

Fill a column with a specific constant instead of using a mode:

```csharp
var readings = await context.Readings
    .OrderBy(r => r.Hour)
    .Interpolate(
        r => r.Hour,
        TimeSpan.FromHours(1),
        r => r.Value,
        constantValue: 0)
    .ToListAsync();
```

Generated SQL:

```sql
ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
INTERPOLATE ("Value" AS 0)
```

## Multi-Column Builder

When multiple non-ORDER BY columns need different interpolation strategies, use the builder pattern:

```csharp
var readings = await context.Readings
    .OrderBy(r => r.Hour)
    .Interpolate(r => r.Hour, TimeSpan.FromHours(1), b =>
    {
        b.Fill(r => r.Temperature, InterpolateMode.Prev);
        b.Fill(r => r.Status, "unknown");
        b.Fill(r => r.Count, 0);
    })
    .ToListAsync();
```

Generated SQL:

```sql
ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
INTERPOLATE ("Temperature" AS "Temperature", "Status" AS 'unknown', "Count" AS 0)
```

The `InterpolateBuilder<T>` supports two `Fill` overloads:

| Method | Purpose |
|--------|---------|
| `Fill(column, InterpolateMode)` | Fill using Default or Prev mode |
| `Fill(column, constant)` | Fill with a specific constant value |

## Step Types Summary

| Step Type | Use Case | Example |
|-----------|----------|---------|
| `TimeSpan` | Fixed-duration intervals (hours, minutes, seconds) | `TimeSpan.FromHours(1)` |
| `ClickHouseInterval` | Calendar-based intervals (months, quarters, years) | `ClickHouseInterval.Months(1)` |
| `int` | Numeric sequence gaps | `1` |

`ClickHouseInterval` supports the following units: `Seconds`, `Minutes`, `Hours`, `Days`, `Weeks`, `Months`, `Quarters`, `Years`.

## Complete Example

A time series dashboard query that fills hourly gaps for a full day with forward-filled temperature and zero-filled event count:

```csharp
var start = new DateTime(2024, 6, 1);
var end = new DateTime(2024, 6, 2);

var dashboard = await context.SensorReadings
    .Where(r => r.SensorId == sensorId)
    .GroupBy(r => r.Hour)
    .Select(g => new
    {
        Hour = g.Key,
        AvgTemp = g.Average(r => r.Temperature),
        EventCount = g.Count()
    })
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1), from: start, to: end)
    .ToListAsync();
```

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [Window Functions](window-functions.md) -- LAG/LEAD for row-relative calculations
