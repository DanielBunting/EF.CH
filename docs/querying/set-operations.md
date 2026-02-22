# Set Operations

ClickHouse requires explicit `UNION ALL` or `UNION DISTINCT` -- bare `UNION` is not supported. EF.CH maps EF Core's standard set operations to the correct ClickHouse syntax and provides convenience extensions for chaining multiple operations.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Standard EF Core Mappings

EF Core's built-in set operations translate as follows:

| EF Core Method | ClickHouse SQL |
|----------------|----------------|
| `Concat()` | `UNION ALL` |
| `Union()` | `UNION DISTINCT` |
| `Intersect()` | `INTERSECT DISTINCT` |
| `Except()` | `EXCEPT DISTINCT` |

```csharp
var clicks = context.Events.Where(e => e.Type == "click");
var views = context.Events.Where(e => e.Type == "view");

// UNION ALL via standard Concat
var all = await clicks.Concat(views).ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."Timestamp"
FROM "Events" AS e
WHERE e."Type" = 'click'
UNION ALL
SELECT e0."Id", e0."Type", e0."Timestamp"
FROM "Events" AS e0
WHERE e0."Type" = 'view'
```

## UnionAll

Combines multiple queries using `UNION ALL` (keeps duplicates). Accepts one or more queries as parameters.

```csharp
var clicks = context.Events.Where(e => e.Type == "click");
var views = context.Events.Where(e => e.Type == "view");
var hovers = context.Events.Where(e => e.Type == "hover");

var combined = await clicks
    .UnionAll(views, hovers)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."Timestamp"
FROM "Events" AS e
WHERE e."Type" = 'click'
UNION ALL
SELECT e0."Id", e0."Type", e0."Timestamp"
FROM "Events" AS e0
WHERE e0."Type" = 'view'
UNION ALL
SELECT e1."Id", e1."Type", e1."Timestamp"
FROM "Events" AS e1
WHERE e1."Type" = 'hover'
```

## UnionDistinct

Combines multiple queries using `UNION DISTINCT` (removes duplicates). Accepts one or more queries as parameters.

```csharp
var combined = await query1
    .UnionDistinct(query2)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT ...
FROM ...
UNION DISTINCT
SELECT ...
FROM ...
```

## Fluent Builder

For complex set operation chains involving different operations, use the `AsSetOperation()` builder:

```csharp
var allEvents = context.Events.Where(e => e.Year == 2024);
var clickEvents = context.Events.Where(e => e.Type == "click");
var botEvents = context.Events.Where(e => e.IsBot);

var results = await allEvents
    .AsSetOperation()
    .UnionAll(clickEvents)
    .Except(botEvents)
    .Build()
    .OrderBy(e => e.Timestamp)
    .ToListAsync();
```

The builder supports four operations:

| Builder Method | SQL |
|----------------|-----|
| `.UnionAll(query)` | `UNION ALL` |
| `.UnionDistinct(query)` | `UNION DISTINCT` |
| `.Intersect(query)` | `INTERSECT DISTINCT` |
| `.Except(query)` | `EXCEPT DISTINCT` |

Call `.Build()` to get back an `IQueryable<T>` for further LINQ operations.

## Combining with Other LINQ Operators

Set operation results can be sorted, filtered, and projected:

```csharp
var results = await query1
    .UnionAll(query2, query3)
    .Where(e => e.Amount > 100)
    .OrderByDescending(e => e.Timestamp)
    .Take(1000)
    .ToListAsync();
```

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [Common Table Expressions](cte.md) -- Named subqueries
