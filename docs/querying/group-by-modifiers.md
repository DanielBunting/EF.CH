# GROUP BY Modifiers

ClickHouse extends standard GROUP BY with three modifiers that add subtotal and total rows to aggregation results. These are applied after `GroupBy().Select()` in the LINQ chain.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## WITH ROLLUP

Generates hierarchical subtotals from right to left across the grouping columns. For `GROUP BY a, b WITH ROLLUP`, ClickHouse returns rows for `(a, b)`, `(a)`, and `()`.

```csharp
var report = await context.Sales
    .GroupBy(s => new { s.Region, s.Category })
    .Select(g => new
    {
        Region = g.Key.Region,
        Category = g.Key.Category,
        Total = g.Sum(s => s.Amount),
        Count = g.Count()
    })
    .WithRollup()
    .ToListAsync();
```

Generated SQL:

```sql
SELECT s."Region", s."Category", sumOrNull(s."Amount") AS "Total", count() AS "Count"
FROM "Sales" AS s
GROUP BY s."Region", s."Category" WITH ROLLUP
```

Example output:

| Region | Category | Total | Count |
|--------|----------|-------|-------|
| US | Electronics | 5000 | 10 |
| US | Clothing | 3000 | 8 |
| EU | Electronics | 4000 | 7 |
| EU | Clothing | 2000 | 5 |
| US | *null* | 8000 | 18 |
| EU | *null* | 6000 | 12 |
| *null* | *null* | 14000 | 30 |

The last three rows are subtotals: per-Region and grand total. Rolled-up columns contain NULL in the subtotal rows.

## WITH CUBE

Generates subtotals for all possible combinations of grouping columns. For `GROUP BY a, b WITH CUBE`, ClickHouse returns rows for `(a, b)`, `(a)`, `(b)`, and `()`.

```csharp
var analysis = await context.Sales
    .GroupBy(s => new { s.Region, s.Category })
    .Select(g => new
    {
        Region = g.Key.Region,
        Category = g.Key.Category,
        Count = g.Count()
    })
    .WithCube()
    .ToListAsync();
```

Generated SQL:

```sql
SELECT s."Region", s."Category", count() AS "Count"
FROM "Sales" AS s
GROUP BY s."Region", s."Category" WITH CUBE
```

Example output:

| Region | Category | Count |
|--------|----------|-------|
| US | Electronics | 10 |
| US | Clothing | 8 |
| EU | Electronics | 7 |
| EU | Clothing | 5 |
| US | *null* | 18 |
| EU | *null* | 12 |
| *null* | Electronics | 17 |
| *null* | Clothing | 13 |
| *null* | *null* | 30 |

Compared to ROLLUP, CUBE also includes Category-only subtotals (rows 7-8) that ROLLUP omits.

## WITH TOTALS

Adds a single grand total row at the end of the result set. Unlike ROLLUP and CUBE, WITH TOTALS does not generate intermediate subtotals.

```csharp
var summary = await context.Events
    .GroupBy(e => e.Category)
    .Select(g => new
    {
        Category = g.Key,
        Count = g.Count()
    })
    .WithTotals()
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Category", count() AS "Count"
FROM "Events" AS e
GROUP BY e."Category" WITH TOTALS
```

Example output:

| Category | Count |
|----------|-------|
| Electronics | 17 |
| Clothing | 13 |
| *null* | 30 |

The final row contains the grand total across all groups.

## Handling NULL in Subtotal Rows

Subtotal rows use NULL for the grouped columns that have been aggregated away. If your result type uses non-nullable strings or value types, consider using nullable types in the projection to distinguish subtotal rows from regular data:

```csharp
var report = await context.Sales
    .GroupBy(s => new { s.Region, s.Category })
    .Select(g => new
    {
        Region = (string?)g.Key.Region,
        Category = (string?)g.Key.Category,
        Total = g.Sum(s => s.Amount)
    })
    .WithRollup()
    .ToListAsync();

// Subtotal rows have null Region and/or Category
var grandTotal = report.Single(r => r.Region == null && r.Category == null);
var regionTotals = report.Where(r => r.Region != null && r.Category == null);
```

## Combining with Other Modifiers

GROUP BY modifiers compose with other query modifiers:

```csharp
var report = await context.Sales
    .Final()
    .PreWhere(s => s.IsActive)
    .Where(s => s.Amount > 0)
    .GroupBy(s => new { s.Region, s.Category })
    .Select(g => new
    {
        Region = (string?)g.Key.Region,
        Category = (string?)g.Key.Category,
        Total = g.Sum(s => s.Amount)
    })
    .WithRollup()
    .WithSetting("max_threads", 8)
    .ToListAsync();
```

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [LIMIT BY](limit-by.md) -- Top-N per group without aggregation
