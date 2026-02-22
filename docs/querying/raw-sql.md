# Raw SQL

Escape hatches for ClickHouse-specific SQL that cannot be expressed through LINQ. Three mechanisms are available: raw WHERE filters, raw SQL expressions in projections, and EF Core's `FromSqlRaw`.

## WithRawFilter

Injects a raw SQL condition into the WHERE clause. The condition is AND-ed with any existing LINQ-generated WHERE predicates.

```csharp
using EF.CH.Extensions;

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

This is useful for ClickHouse expressions that use lambda syntax in array functions, dictionary lookups, or other constructs that LINQ cannot represent.

### Multiple raw filters

Calling `WithRawFilter` multiple times appends each condition with AND:

```csharp
var results = await context.Events
    .WithRawFilter("has(Tags, 'urgent')")
    .WithRawFilter("length(Tags) > 3")
    .ToListAsync();
```

Generated SQL:

```sql
WHERE has(Tags, 'urgent') AND length(Tags) > 3
```

### Combining with LINQ predicates

Raw filters combine naturally with standard LINQ Where clauses:

```csharp
var results = await context.Events
    .Where(e => e.CreatedAt > cutoff)
    .Where(e => e.IsActive)
    .WithRawFilter("cityHash64(UserId) % 10 = 0")
    .ToListAsync();
```

Generated SQL:

```sql
WHERE e."CreatedAt" > @p0 AND e."IsActive" AND cityHash64(UserId) % 10 = 0
```

## RawSql in Projections

`ClickHouseFunctions.RawSql<T>(sql)` embeds a raw SQL expression in a LINQ Select projection. The SQL string is emitted verbatim in the SELECT clause.

```csharp
using EF.CH.Extensions;

var results = await context.Events
    .GroupBy(e => e.Category)
    .Select(g => new
    {
        Category = g.Key,
        Count = g.Count(),
        P95 = ClickHouseFunctions.RawSql<double>("quantile(0.95)(Amount)")
    })
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Category", count() AS "Count", quantile(0.95)(Amount) AS "P95"
FROM "Events" AS e
GROUP BY e."Category"
```

The generic type parameter `T` determines the CLR type of the returned value. The SQL string is not parameterized or validated -- it is the caller's responsibility to ensure correctness.

### Use cases for RawSql

- Aggregate functions with non-standard syntax (double parentheses like `quantile(0.95)(column)`)
- ClickHouse-specific expressions not covered by the provider's translators
- Complex computed columns that would be cumbersome to express in LINQ

```csharp
var results = await context.Users.Select(u => new
{
    u.Id,
    u.Name,
    NameHash = ClickHouseFunctions.RawSql<ulong>("sipHash64(Name)"),
    FormattedSize = ClickHouseFunctions.RawSql<string>("formatReadableSize(DataBytes)")
}).ToListAsync();
```

## FromSqlRaw

EF Core's built-in `FromSqlRaw` works with ClickHouse for cases where the entire query needs to be raw SQL.

```csharp
var results = await context.Events
    .FromSqlRaw("SELECT * FROM Events FINAL WHERE Type = {0}", "click")
    .Where(e => e.IsActive)
    .ToListAsync();
```

Generated SQL:

```sql
SELECT e."Id", e."Type", e."IsActive"
FROM (
    SELECT * FROM Events FINAL WHERE Type = 'click'
) AS e
WHERE e."IsActive"
```

`FromSqlRaw` wraps the raw SQL as a subquery. Additional LINQ operators (Where, OrderBy, Take) are applied to the outer query.

### Parameterized queries

Use indexed placeholders `{0}`, `{1}`, etc. for parameterized values:

```csharp
var results = await context.Events
    .FromSqlRaw(
        "SELECT * FROM Events WHERE Type = {0} AND CreatedAt > {1}",
        "click",
        cutoffDate)
    .ToListAsync();
```

### Limitations

- The raw SQL must return columns that match the entity's property names and types.
- `FromSqlRaw` cannot be used with keyless entity types unless they are explicitly configured.

## Security

Raw SQL methods (`WithRawFilter`, `RawSql`, `FromSqlRaw`) do not parameterize the SQL string (except `FromSqlRaw` with indexed placeholders). If user input is incorporated into the SQL string, the caller must sanitize it to prevent SQL injection. Prefer parameterized approaches when working with untrusted input.

## See Also

- [Query Modifiers](query-modifiers.md) -- Typed LINQ alternatives (FINAL, SAMPLE, PREWHERE, SETTINGS)
- [Text Search](text-search.md) -- Typed search functions that avoid raw SQL
- [Window Functions](window-functions.md) -- Typed window function API
