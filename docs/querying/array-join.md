# ARRAY JOIN

Unnests array columns into individual rows using ClickHouse's `ARRAY JOIN` clause, which appears between `FROM` and `WHERE` in the generated SQL. Each array element becomes a separate row, enabling analytics on array-typed columns without client-side expansion.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Usage

```csharp
var result = await context.Events
    .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
    .OrderBy(x => x.Id)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Name", tag
FROM "Events" AS e
ARRAY JOIN e."Tags" AS tag
ORDER BY e."Id" ASC
```

Rows with empty arrays are excluded from the results. In the example above, an event with `Tags = []` would not appear in the output.

## LEFT ARRAY JOIN

`LeftArrayJoin` preserves rows that have empty arrays. The unnested column receives ClickHouse's default value for the element type (empty string for `String`, 0 for numeric types, etc.).

```csharp
var result = await context.Events
    .LeftArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
    .OrderBy(x => x.Id)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Name", tag
FROM "Events" AS e
LEFT ARRAY JOIN e."Tags" AS tag
ORDER BY e."Id" ASC
```

## Filtering on Unnested Columns

Chain `.Where()` after `ArrayJoin` to filter on the unnested values. The filter is applied after the array is expanded.

```csharp
var result = await context.Events
    .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
    .Where(x => x.Tag == "critical")
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Name", tag
FROM "Events" AS e
ARRAY JOIN e."Tags" AS tag
WHERE tag = 'critical'
```

## Multiple Arrays

The two-array overload unnests two array columns simultaneously. Arrays are joined positionally (element-wise), not as a cartesian product. If the arrays have different lengths, shorter arrays are padded with default values.

```csharp
var result = await context.Events
    .ArrayJoin(
        e => e.Tags,
        e => e.Scores,
        (e, tag, score) => new { e.Id, Tag = tag, Score = score })
    .OrderBy(x => x.Id)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", tag, score
FROM "Events" AS e
ARRAY JOIN e."Tags" AS tag, e."Scores" AS score
ORDER BY e."Id" ASC
```

## Combining with Other Modifiers

ARRAY JOIN can be chained with other ClickHouse-specific modifiers like FINAL:

```csharp
var result = await context.Events
    .Final()
    .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, Tag = tag })
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", tag
FROM "Events" FINAL AS e
ARRAY JOIN e."Tags" AS tag
```

**Performance note:** ARRAY JOIN runs entirely server-side -- ClickHouse expands each array into rows without transferring raw arrays to the client. LEFT ARRAY JOIN preserves rows with empty arrays by emitting a single row with the type's default value. The alias for the unnested column is derived from the parameter name in the result selector (e.g., `tag` in `(e, tag) => ...` produces `AS tag`).

## Limitations

- Only array-typed columns can be used with ARRAY JOIN. Map and Nested types are not supported through this extension method.
- A result selector is required -- there is no passthrough overload that returns the original entity with the array column replaced.

## See Also

- [Query Modifiers](query-modifiers.md) -- FINAL, SAMPLE, PREWHERE, SETTINGS
- [ASOF JOIN](asof-join.md) -- Closest-match joins for time-series data
