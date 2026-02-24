# CteSample

Demonstrates ClickHouse Common Table Expressions (CTEs) via the `AsCte()` extension method.

## What This Shows

- Basic CTE with filter
- CTE with ordering and limiting
- CTE for analytical query patterns
- Generates `WITH "name" AS (...) SELECT ... FROM "name"`

## How It Works

`AsCte("name")` wraps the preceding query as a named CTE in a WITH clause. Operations after `AsCte()` operate on the CTE reference.

```
// LINQ
context.Events
    .Where(e => e.EventType == "purchase")
    .AsCte("purchases")
    .OrderByDescending(e => e.Amount)
    .Take(10)

// Generated SQL
WITH "purchases" AS (
    SELECT ... FROM "AnalyticsEvents" WHERE "EventType" = 'purchase'
)
SELECT ... FROM "purchases" ORDER BY "Amount" DESC LIMIT 10
```

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Key Code

### Basic CTE

```csharp
var result = await context.Events
    .Where(e => e.EventType == "purchase" && e.CreatedAt > cutoff)
    .AsCte("recent_purchases")
    .OrderByDescending(e => e.Amount)
    .Take(10)
    .ToListAsync();
```

### CTE with Complex Filter

```csharp
var result = await context.Events
    .Where(e => e.Amount > 200 && e.EventType != "logout")
    .AsCte("high_value")
    .OrderBy(e => e.Region)
    .ToListAsync();
```

## Limitations

- **Single CTE per query**: Multi-CTE support planned for future versions
- **No recursive CTEs**: ClickHouse has limited recursive CTE support
- **Single-use reference**: The CTE is referenced once in the outer query
