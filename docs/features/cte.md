# Common Table Expressions (CTEs)

EF.CH supports Common Table Expressions via the `AsCte()` extension method, which wraps a subquery as a named CTE in the `WITH` clause.

## Basic Usage

```csharp
using EF.CH.Extensions;

var result = await context.Events
    .Where(e => e.Category == "electronics")
    .AsCte("filtered")
    .OrderByDescending(e => e.Amount)
    .Take(10)
    .ToListAsync();
```

Generates:
```sql
WITH "filtered" AS (
    SELECT ... FROM "Events" WHERE "Category" = 'electronics'
)
SELECT ... FROM "filtered" ORDER BY "Amount" DESC LIMIT 10
```

## How It Works

1. **`AsCte("name")`** marks the preceding query as a CTE candidate
2. During query translation, the subquery is extracted and stored as a CTE definition
3. The SQL generator prepends the `WITH` clause and replaces the subquery with a CTE reference

Operations before `AsCte()` become the CTE body. Operations after `AsCte()` operate on the CTE reference.

## Examples

### Filter + Aggregate

```csharp
var recentPurchases = await context.Events
    .Where(e => e.EventType == "purchase" && e.CreatedAt > cutoff)
    .AsCte("recent_purchases")
    .OrderByDescending(e => e.Amount)
    .Take(10)
    .ToListAsync();
```

### Complex Filter as Named CTE

```csharp
var highValue = await context.Events
    .Where(e => e.Amount > 200 && e.EventType != "logout")
    .AsCte("high_value")
    .OrderBy(e => e.Region)
    .ThenByDescending(e => e.Amount)
    .ToListAsync();
```

### Direct Table as CTE

When `AsCte()` is called without preceding operators, the table itself becomes the CTE body:

```csharp
var all = await context.Events
    .AsCte("all_events")
    .OrderBy(e => e.CreatedAt)
    .Take(100)
    .ToListAsync();
```

Generates:
```sql
WITH "all_events" AS (
    SELECT * FROM "Events"
)
SELECT ... FROM "all_events" ORDER BY "CreatedAt" LIMIT 100
```

## Naming

CTE names must be non-null and non-empty:

```csharp
// Valid
context.Events.AsCte("recent_events")
context.Events.AsCte("filtered")

// Invalid - throws ArgumentException
context.Events.AsCte("")
context.Events.AsCte(null!)
```

## Scope and Limitations

- **Single CTE per query**: Each query supports one `AsCte()` call. Multi-CTE support (multiple WITH clauses) is planned for a future version
- **No recursive CTEs**: ClickHouse has limited recursive CTE support, and this is not currently implemented
- **No CTE reuse**: The CTE is referenced once in the FROM clause. Self-joins or multiple references to the same CTE are not supported yet
- **No joins in UPDATE**: CTEs cannot be used with `ExecuteUpdateAsync` or `ExecuteDeleteAsync`

## Complete Example

```csharp
public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
}

modelBuilder.Entity<AnalyticsEvent>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
    entity.HasPartitionByMonth(x => x.CreatedAt);
});
```

### Usage

```csharp
using EF.CH.Extensions;

// Wrap a filtered query as a CTE
var cutoff = DateTime.UtcNow.AddDays(-7);

var topPurchases = await context.Events
    .Where(e => e.EventType == "purchase" && e.CreatedAt > cutoff)
    .AsCte("recent_purchases")
    .OrderByDescending(e => e.Amount)
    .Take(10)
    .ToListAsync();

// CTE with complex filtering
var highValueByRegion = await context.Events
    .Where(e => e.Amount > 200 && e.EventType != "logout")
    .AsCte("high_value")
    .OrderBy(e => e.Region)
    .ThenByDescending(e => e.Amount)
    .ToListAsync();
```

## See Also

- [Set Operations](set-operations.md) - UNION, INTERSECT, EXCEPT
- [Query Modifiers](query-modifiers.md) - FINAL, SAMPLE, PREWHERE
- [ClickHouse WITH Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/with)
