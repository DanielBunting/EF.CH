# LIMIT BY

`LIMIT BY` is a ClickHouse-specific clause that limits the number of rows returned per group. It provides "top-N per category" functionality without window functions or subqueries.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Usage

Return at most N rows per group. Use `OrderBy` or `OrderByDescending` before `LimitBy` to control which rows are kept within each group.

```csharp
var topEvents = await context.Events
    .OrderByDescending(e => e.Score)
    .LimitBy(5, e => e.Category)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Category", e."Score"
FROM "Events" AS e
ORDER BY e."Score" DESC
LIMIT 5 BY e."Category"
```

This returns the 5 highest-scored events in each category.

## Paginated LIMIT BY

Skip rows before taking the limit within each group. The first parameter is the offset (rows to skip), the second is the limit (rows to take).

```csharp
var page2 = await context.Events
    .OrderByDescending(e => e.CreatedAt)
    .LimitBy(5, 5, e => e.UserId)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."UserId", e."CreatedAt"
FROM "Events" AS e
ORDER BY e."CreatedAt" DESC
LIMIT 5, 5 BY e."UserId"
```

This skips the first 5 events per user (page 1) and returns the next 5 (page 2).

## Compound Keys

Group by multiple columns using an anonymous type:

```csharp
var results = await context.Events
    .OrderByDescending(e => e.Score)
    .LimitBy(3, e => new { e.Category, e.Region })
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Category", e."Region", e."Score"
FROM "Events" AS e
ORDER BY e."Score" DESC
LIMIT 3 BY e."Category", e."Region"
```

This returns the top 3 events for each unique (Category, Region) pair.

## Combining with WHERE and LIMIT

`LIMIT BY` operates after WHERE filtering and before the outer LIMIT/TAKE. This lets you filter first, take the top-N per group, then cap the overall result count:

```csharp
var results = await context.Events
    .Where(e => e.IsActive)
    .OrderByDescending(e => e.Score)
    .LimitBy(3, e => e.Category)
    .Take(100)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Category", e."Score"
FROM "Events" AS e
WHERE e."IsActive"
ORDER BY e."Score" DESC
LIMIT 3 BY e."Category"
LIMIT 100
```

## Validation

- The `limit` parameter must be a positive integer. Passing zero or negative values throws `ArgumentOutOfRangeException`.
- The `offset` parameter (in the paginated overload) must be non-negative.

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [GROUP BY Modifiers](group-by-modifiers.md) -- WITH ROLLUP, WITH CUBE, WITH TOTALS
- [Window Functions](window-functions.md) -- ROW_NUMBER-based alternatives
