# Query Modifiers

ClickHouse-specific query modifiers that control how data is read, filtered, and processed. These are applied as LINQ extension methods on `IQueryable<T>` and translate directly to ClickHouse SQL clauses.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## FINAL

Forces on-the-fly deduplication for ReplacingMergeTree tables. Without FINAL, queries may return multiple versions of the same row if background merges have not yet collapsed them.

```csharp
var users = await context.Users
    .Final()
    .Where(u => u.IsActive)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT u."Id", u."Name", u."IsActive"
FROM "Users" FINAL AS u
WHERE u."IsActive"
```

**Performance note:** FINAL merges rows at query time, which adds CPU overhead proportional to the number of unmerged parts. For large tables with many recent inserts, consider running `OPTIMIZE TABLE` during off-peak hours instead of using FINAL on every query. For small-to-medium tables or infrequent reads, FINAL is the simplest approach.

## SAMPLE

Reads only a fraction of the data for approximate results. The table must have a `SAMPLE BY` clause in its engine definition.

### Basic sampling

```csharp
var estimate = await context.Events
    .Sample(0.1)
    .Where(e => e.Type == "click")
    .CountAsync();
```

Generated SQL:

```sql
SELECT count(*)
FROM "Events" SAMPLE 0.1 AS e
WHERE e."Type" = 'click'
```

### Reproducible sampling with offset

The offset parameter produces a deterministic sample starting at a specific position in the data. Different offset values yield different non-overlapping samples.

```csharp
var sample = await context.Events
    .Sample(0.1, 0.5)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."Timestamp"
FROM "Events" SAMPLE 0.1 OFFSET 0.5 AS e
```

**Performance note:** SAMPLE reads only the requested fraction of granules from disk. A `Sample(0.01)` on a billion-row table reads roughly 1% of the data, making exploratory queries orders of magnitude faster. The fraction must be between 0 (exclusive) and 1 (inclusive).

## PREWHERE

Filters rows before reading all columns. ClickHouse reads only the columns referenced in the PREWHERE predicate first, then reads the remaining columns only for matching rows.

```csharp
var recent = await context.Events
    .PreWhere(e => e.IsActive)
    .Where(e => e.Category == "important")
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Category", e."IsActive", e."Payload"
FROM "Events" AS e
PREWHERE e."IsActive"
WHERE e."Category" = 'important'
```

**Performance note:** PREWHERE is most effective on wide tables (many columns) with selective filters on small columns. If the table has 100 columns but the filter column is a single `Bool`, PREWHERE reads only that one column to eliminate rows before loading the other 99. Use it for predicates on ORDER BY key columns or other highly selective conditions.

## Query Settings

Applies ClickHouse `SETTINGS` to a single query without affecting the connection or other queries.

### Single setting

```csharp
var results = await context.Events
    .WithSetting("max_threads", 4)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."Timestamp"
FROM "Events" AS e
SETTINGS max_threads = 4
```

### Multiple settings

```csharp
var settings = new Dictionary<string, object>
{
    ["max_threads"] = 4,
    ["optimize_read_in_order"] = 1,
    ["max_block_size"] = 65505
};

var results = await context.Events
    .WithSettings(settings)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."Timestamp"
FROM "Events" AS e
SETTINGS max_threads = 4, optimize_read_in_order = 1, max_block_size = 65505
```

Commonly used settings:

| Setting | Purpose |
|---------|---------|
| `max_threads` | Maximum parallel threads for query execution |
| `optimize_read_in_order` | Read data in ORDER BY key order (avoids sorting) |
| `max_block_size` | Maximum block size for reading |
| `max_rows_to_read` | Query fails if this many rows would be read |
| `max_execution_time` | Query timeout in seconds |

## Raw Filter

Injects a raw SQL expression into the WHERE clause. Use this for ClickHouse-specific syntax that cannot be expressed through LINQ, such as array lambda predicates or special functions.

```csharp
var results = await context.Events
    .Where(e => e.Type == "click")
    .WithRawFilter("arrayExists(x -> x > 10, Tags)")
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."Tags"
FROM "Events" AS e
WHERE e."Type" = 'click' AND arrayExists(x -> x > 10, Tags)
```

The raw SQL condition is AND-ed with any existing WHERE conditions from LINQ. No parameterization or escaping is applied to the raw string -- the caller is responsible for preventing SQL injection if user input is involved.

## Combining Modifiers

Modifiers can be chained together. Each applies independently to the generated SQL.

```csharp
var results = await context.Events
    .Final()
    .Sample(0.5)
    .PreWhere(e => e.IsActive)
    .Where(e => e.Category == "important")
    .WithSetting("max_threads", 8)
    .OrderByDescending(e => e.Timestamp)
    .Take(100)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Category", e."IsActive", e."Timestamp"
FROM "Events" FINAL SAMPLE 0.5 AS e
PREWHERE e."IsActive"
WHERE e."Category" = 'important'
ORDER BY e."Timestamp" DESC
LIMIT 100
SETTINGS max_threads = 8
```

## See Also

- [GROUP BY Modifiers](group-by-modifiers.md) -- WITH ROLLUP, WITH CUBE, WITH TOTALS
- [LIMIT BY](limit-by.md) -- Top-N per group
- [Common Table Expressions](cte.md) -- Named subqueries
- [Raw SQL](raw-sql.md) -- Additional escape hatches
