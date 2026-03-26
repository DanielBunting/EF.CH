# ASOF JOIN

Performs a closest-match join by an inequality condition on an ordered column, commonly used for time-series enrichment. For each row in the left table, ClickHouse finds the closest matching row from the right table that satisfies both the equi-join key and the ASOF inequality.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Usage

```csharp
var result = await context.Events
    .AsofJoin(context.Prices,
        e => e.Symbol, p => p.Symbol,
        (e, p) => e.Timestamp >= p.Timestamp,
        (e, p) => new { e.Name, e.Symbol, e.Timestamp, PriceAtEvent = p.Price })
    .OrderBy(x => x.Timestamp)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Name", e."Symbol", e."Timestamp", p."Price" AS "PriceAtEvent"
FROM "Events" AS e
ASOF JOIN "Prices" AS p ON e."Symbol" = p."Symbol" AND e."Timestamp" >= p."Timestamp"
ORDER BY e."Timestamp" ASC
```

This matches each event to the most recent price at or before the event's timestamp for the same symbol.

## LEFT ASOF JOIN

`AsofLeftJoin` preserves all rows from the left table, even when no matching row exists in the right table. Unmatched right-side columns receive ClickHouse's default values (0 for numbers, empty string for strings, epoch for DateTime).

```csharp
var result = await context.Events
    .AsofLeftJoin(context.Prices,
        e => e.Symbol, p => p.Symbol,
        (e, p) => e.Timestamp >= p.Timestamp,
        (e, p) => new { e.Name, e.Symbol, Price = p.Price })
    .OrderBy(x => x.Name)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Name", e."Symbol", p."Price"
FROM "Events" AS e
ASOF LEFT JOIN "Prices" AS p ON e."Symbol" = p."Symbol" AND e."Timestamp" >= p."Timestamp"
ORDER BY e."Name" ASC
```

## Filtering After Join

Chain `.Where()` after the ASOF JOIN to filter on any column from either side of the join:

```csharp
var result = await context.Events
    .AsofJoin(context.Prices,
        e => e.Symbol, p => p.Symbol,
        (e, p) => e.Timestamp >= p.Timestamp,
        (e, p) => new { e.Name, e.Symbol, PriceAtEvent = p.Price })
    .Where(x => x.PriceAtEvent > 100m)
    .ToListAsync();
```

## Combining with FINAL

Apply `.Final()` on the source queryable before the ASOF JOIN:

```csharp
var result = await context.Events
    .Final()
    .AsofJoin(context.Prices,
        e => e.Symbol, p => p.Symbol,
        (e, p) => e.Timestamp >= p.Timestamp,
        (e, p) => new { e.Name, PriceAtEvent = p.Price })
    .ToListAsync();
```

## Supported Operators

The ASOF condition must use one of four comparison operators:

| Operator | Meaning | Example |
|----------|---------|---------|
| `>=` | Closest match at or before | `e.Timestamp >= p.Timestamp` -- price at or before event |
| `>` | Closest match strictly before | `e.Timestamp > p.Timestamp` -- price strictly before event |
| `<=` | Closest match at or after | `e.Timestamp <= p.Timestamp` -- next price at or after event |
| `<` | Closest match strictly after | `e.Timestamp < p.Timestamp` -- next price strictly after event |

Using any other operator (e.g., `==`, `!=`) throws an `InvalidOperationException`.

## Requirements

- **ASOF column must be ordered.** The ASOF column (the column used in the inequality condition) must be part of the inner table's `ORDER BY` key. ClickHouse uses this ordering to efficiently find the closest match.
- **Equi-join key is required.** ASOF JOIN requires at least one equality condition alongside the inequality. The `outerKeySelector` and `innerKeySelector` parameters define this equi-join key.
- **One ASOF JOIN per query.** ClickHouse supports a single ASOF JOIN clause per query.

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [ARRAY JOIN](array-join.md) -- Unnesting array columns into rows
