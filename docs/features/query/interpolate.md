# Time Series Gap Filling (Interpolate)

EF.CH provides the `Interpolate()` extension method for ClickHouse's `WITH FILL` and `INTERPOLATE` clauses, enabling automatic gap filling in time series and sequence data.

## Overview

When querying time series data, you often encounter gaps - missing hours, days, or other intervals where no data exists. ClickHouse's `WITH FILL` clause automatically inserts rows to fill these gaps, and the `INTERPOLATE` clause specifies how to fill values in non-ORDER BY columns.

## Basic Gap Filling

Fill gaps in an ORDER BY column with a specified step:

```csharp
using EF.CH.Extensions;

// Fill hourly gaps in time series data
var hourlyData = await context.Readings
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1))
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Readings" ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
```

## Step Types

### TimeSpan (Hours, Minutes, Seconds, Days)

Use `TimeSpan` for sub-month intervals:

```csharp
// Every 15 minutes
.Interpolate(x => x.Timestamp, TimeSpan.FromMinutes(15))

// Every 6 hours
.Interpolate(x => x.Timestamp, TimeSpan.FromHours(6))

// Daily
.Interpolate(x => x.Timestamp, TimeSpan.FromDays(1))
```

### ClickHouseInterval (Months, Quarters, Years)

Use `ClickHouseInterval` for calendar-based units that `TimeSpan` cannot represent:

```csharp
// Monthly gaps
.Interpolate(x => x.Month, ClickHouseInterval.Months(1))

// Quarterly
.Interpolate(x => x.Quarter, ClickHouseInterval.Quarters(1))

// Yearly
.Interpolate(x => x.Year, ClickHouseInterval.Years(1))

// Also works for smaller units
.Interpolate(x => x.Timestamp, ClickHouseInterval.Hours(2))
```

### Numeric Step

For numeric ORDER BY columns:

```csharp
// Fill gaps in sequence numbers with step of 10
.Interpolate(x => x.SequenceNumber, 10)
```

Generates:
```sql
SELECT ... ORDER BY "SequenceNumber" ASC WITH FILL STEP 10
```

## FROM/TO Bounds

Specify explicit start and end bounds for gap filling:

```csharp
var start = new DateTime(2024, 1, 1);
var end = new DateTime(2024, 12, 31);

var fullYear = await context.DailySummary
    .OrderBy(x => x.Date)
    .Interpolate(x => x.Date, TimeSpan.FromDays(1), start, end)
    .ToListAsync();
```

Generates:
```sql
SELECT ... ORDER BY "Date" ASC
WITH FILL FROM toDateTime64('2024-01-01 00:00:00.000', 3)
         TO toDateTime64('2024-12-31 00:00:00.000', 3)
         STEP INTERVAL 1 DAY
```

This ensures you get rows for every day in the range, even if no data exists.

## Interpolating Column Values

When gaps are filled, non-ORDER BY columns default to their type's zero value (0, empty string, etc.). Use interpolation to specify how these columns should be filled.

### Single Column with Mode

Use `InterpolateMode` to specify fill behavior:

```csharp
// Forward-fill: use the previous row's value
var data = await context.Readings
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1),
                 x => x.Value, InterpolateMode.Prev)
    .ToListAsync();
```

Generates:
```sql
SELECT ... ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
INTERPOLATE ("Value" AS "Value")
```

**Available Modes:**

| Mode | SQL | Description |
|------|-----|-------------|
| `InterpolateMode.Default` | (column default) | Use the column's default value |
| `InterpolateMode.Prev` | `column AS column` | Forward-fill from previous row |

### Single Column with Constant

Fill gaps with a specific constant value:

```csharp
// Fill missing values with 0
.Interpolate(x => x.Hour, TimeSpan.FromHours(1),
             x => x.Value, 0)

// Fill missing strings with placeholder
.Interpolate(x => x.Hour, TimeSpan.FromHours(1),
             x => x.Status, "unknown")
```

Generates:
```sql
SELECT ... ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
INTERPOLATE ("Value" AS 0)
```

### Multiple Columns with Builder

For multiple columns, use the builder pattern:

```csharp
var data = await context.Readings
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1), i => i
        .Fill(x => x.Value, InterpolateMode.Prev)   // Forward-fill
        .Fill(x => x.Count, 0)                       // Constant 0
        .Fill(x => x.Status, "missing"))             // Constant string
    .ToListAsync();
```

Generates:
```sql
SELECT ... ORDER BY "Hour" ASC WITH FILL STEP INTERVAL 1 HOUR
INTERPOLATE ("Value" AS "Value", "Count" AS 0, "Status" AS 'missing')
```

## Complete Example

```csharp
public class SensorReading
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public int ReadingCount { get; set; }
}

// Get hourly averages with gaps filled
var hourlyReadings = await context.SensorReadings
    .GroupBy(r => r.Timestamp.ToStartOfHour())
    .Select(g => new
    {
        Hour = g.Key,
        AvgTemp = g.Average(r => r.Temperature),
        AvgHumidity = g.Average(r => r.Humidity),
        TotalReadings = g.Sum(r => r.ReadingCount)
    })
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1), i => i
        .Fill(x => x.AvgTemp, InterpolateMode.Prev)
        .Fill(x => x.AvgHumidity, InterpolateMode.Prev)
        .Fill(x => x.TotalReadings, 0))
    .ToListAsync();
```

## Use Cases

### Dashboard Charts

Ensure continuous data points for time series visualizations:

```csharp
var chartData = await context.Metrics
    .Where(m => m.Timestamp >= startDate && m.Timestamp < endDate)
    .GroupBy(m => m.Timestamp.ToStartOfHour())
    .Select(g => new { Hour = g.Key, Value = g.Sum(m => m.Value) })
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1), startDate, endDate)
    .ToListAsync();
```

### Time Series Analysis

Fill gaps for consistent interval analysis:

```csharp
var dailyStats = await context.Events
    .GroupBy(e => e.Timestamp.Date)
    .Select(g => new { Date = g.Key, Count = g.Count() })
    .OrderBy(x => x.Date)
    .Interpolate(x => x.Date, ClickHouseInterval.Days(1), i => i
        .Fill(x => x.Count, 0))
    .ToListAsync();
```

### Reporting with Complete Date Ranges

Ensure reports include all periods, even those without data:

```csharp
var monthlyReport = await context.Sales
    .GroupBy(s => new { s.Timestamp.Year, s.Timestamp.Month })
    .Select(g => new
    {
        Month = new DateTime(g.Key.Year, g.Key.Month, 1),
        Revenue = g.Sum(s => s.Amount)
    })
    .OrderBy(x => x.Month)
    .Interpolate(x => x.Month, ClickHouseInterval.Months(1), i => i
        .Fill(x => x.Revenue, 0m))
    .ToListAsync();
```

## Combining with Other Query Modifiers

Interpolate works with other EF.CH query modifiers:

```csharp
var data = await context.Metrics
    .WithSetting("max_threads", 4)
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1))
    .Take(1000)
    .ToListAsync();
```

## Requirements and Limitations

- **Requires ORDER BY**: `Interpolate()` must be called after `OrderBy()` or `OrderByDescending()`
- **Single ORDER BY column**: WITH FILL applies to the column specified in the Interpolate call
- **Step must match column type**: Use TimeSpan/ClickHouseInterval for datetime columns, int for numeric columns
- **Query execution**: Like `Sample()`, the translation works at query execution time; `ToQueryString()` may show parameterized values

## Generated SQL Reference

| API | Generated SQL |
|-----|---------------|
| `.Interpolate(x => x.Hour, TimeSpan.FromHours(1))` | `WITH FILL STEP INTERVAL 1 HOUR` |
| `.Interpolate(x => x.Hour, ClickHouseInterval.Days(1))` | `WITH FILL STEP INTERVAL 1 DAY` |
| `.Interpolate(x => x.Seq, 10)` | `WITH FILL STEP 10` |
| `.Interpolate(..., from, to)` | `WITH FILL FROM ... TO ... STEP ...` |
| `.Interpolate(..., x => x.Val, InterpolateMode.Prev)` | `INTERPOLATE ("Val" AS "Val")` |
| `.Interpolate(..., x => x.Val, 0)` | `INTERPOLATE ("Val" AS 0)` |

## See Also

- [Query Modifiers](query-modifiers.md) - Final(), Sample(), WithSettings()
- [ClickHouse WITH FILL Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier)
- [ClickHouse Time Series Gap Filling Guide](https://clickhouse.com/docs/en/guides/developer/time-series-filling-gaps)
