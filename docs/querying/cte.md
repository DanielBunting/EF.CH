# Common Table Expressions

`AsCte` wraps a LINQ query as a Common Table Expression (CTE), generating a `WITH ... AS (...) SELECT ...` clause in the output SQL. CTEs allow naming a subquery and referencing it in the outer query, improving readability for complex analytical queries.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Usage

```csharp
var topProducts = context.Orders
    .GroupBy(o => o.ProductId)
    .Select(g => new
    {
        ProductId = g.Key,
        Total = g.Sum(o => o.Amount)
    })
    .AsCte("top_products");

var results = await topProducts
    .Where(tp => tp.Total > 1000)
    .OrderByDescending(tp => tp.Total)
    .ToListAsync();
```

Generated SQL:

```sql
WITH "top_products" AS (
    SELECT o."ProductId", sumOrNull(o."Amount") AS "Total"
    FROM "Orders" AS o
    GROUP BY o."ProductId"
)
SELECT t."ProductId", t."Total"
FROM "top_products" AS t
WHERE t."Total" > 1000
ORDER BY t."Total" DESC
```

## Composing with Other Queries

A CTE can be filtered, sorted, and projected like any other queryable. Apply additional LINQ operators after `AsCte`:

```csharp
var activeSummary = context.Users
    .Where(u => u.IsActive)
    .GroupBy(u => u.Department)
    .Select(g => new
    {
        Department = g.Key,
        Count = g.Count(),
        AvgSalary = g.Average(u => u.Salary)
    })
    .AsCte("dept_summary");

var largeDepts = await activeSummary
    .Where(d => d.Count > 10)
    .OrderBy(d => d.Department)
    .ToListAsync();
```

## Composing with Query Modifiers

CTEs work with other ClickHouse-specific modifiers:

```csharp
var cte = context.Events
    .Final()
    .PreWhere(e => e.IsActive)
    .GroupBy(e => e.Category)
    .Select(g => new
    {
        Category = g.Key,
        Total = g.Sum(e => e.Amount)
    })
    .AsCte("category_totals");

var results = await cte
    .Where(c => c.Total > 500)
    .WithSetting("max_threads", 4)
    .ToListAsync();
```

## Limitations

- **Single CTE per query.** Multi-CTE support (multiple WITH clauses) is not currently supported. If you need multiple named subqueries, consider using subqueries via standard LINQ composition.
- **No recursive CTEs.** ClickHouse has limited support for recursive CTEs, and this provider does not generate them.
- **Name must be non-empty.** Passing a null or empty string throws `ArgumentException`.

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [Set Operations](set-operations.md) -- UNION ALL, UNION DISTINCT
