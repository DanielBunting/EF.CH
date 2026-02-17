# Type Checking Functions

ClickHouse provides functions for validating floating-point values. These are essential when working with analytics data that may contain `NaN` or `Inf` values. EF.CH exposes them as `EF.Functions` extensions.

## Available Functions

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `IsNaN(value)` | `isNaN(x)` | `bool` | True if value is Not-a-Number |
| `IsFinite(value)` | `isFinite(x)` | `bool` | True if value is not Inf and not NaN |
| `IsInfinite(value)` | `isInfinite(x)` | `bool` | True if value is positive or negative infinity |

All functions take `double` parameters.

## Usage Examples

### Filter Invalid Data

```csharp
using EF.CH.Extensions;

// Exclude NaN and Inf values from aggregations
var validMetrics = await context.Metrics
    .Where(m => EF.Functions.IsFinite(m.Value))
    .GroupBy(m => m.MetricName)
    .Select(g => new
    {
        Name = g.Key,
        Avg = g.Average(m => m.Value),
        Count = g.Count()
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "MetricName" AS "Name", avgOrNull("Value") AS "Avg", count() AS "Count"
FROM "Metrics"
WHERE isFinite("Value")
GROUP BY "MetricName"
```

### Data Quality Report

```csharp
var quality = await context.Measurements
    .GroupBy(m => 1)
    .Select(g => new
    {
        Total = g.Count(),
        NanCount = g.Count(m => EF.Functions.IsNaN(m.Value)),
        InfCount = g.Count(m => EF.Functions.IsInfinite(m.Value)),
        ValidCount = g.Count(m => EF.Functions.IsFinite(m.Value))
    })
    .FirstAsync();
```

### Conditional Replacement

```csharp
// Replace NaN with 0 using IfNull + NullIf pattern, or use with Coalesce
var cleaned = await context.Sensors
    .Select(s => new
    {
        s.SensorId,
        // Use a CASE WHEN via ternary — or combine with IfNull
        Reading = EF.Functions.IsNaN(s.Reading) ? 0.0 : s.Reading
    })
    .ToListAsync();
```

## When Do NaN/Inf Values Appear?

- Division by zero in Float columns produces `Inf`
- `0.0 / 0.0` produces `NaN`
- Imported data from external systems may contain IEEE 754 special values
- Mathematical functions (`log(0)`, `sqrt(-1)`) can produce `NaN` or `Inf`

Standard SQL aggregates (`AVG`, `SUM`) propagate `NaN` — a single `NaN` value makes the entire result `NaN`. Always filter with `IsFinite` before aggregating if your data might contain special values.

## Notes

- These functions only apply to `Float32`/`Float64` columns. Integer columns never contain `NaN` or `Inf`.
- `IsFinite(x)` is equivalent to `!IsNaN(x) && !IsInfinite(x)`.

## Learn More

- [ClickHouse Type Checking Functions](https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions)
