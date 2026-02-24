# Projections

Projections are pre-computed alternative data representations stored alongside the main table. ClickHouse automatically maintains projections during INSERT operations and the query optimizer transparently selects the best projection for each query without changes to the query itself.

EF.CH supports two projection types: sort-order projections (alternative ORDER BY) and aggregation projections (pre-computed GROUP BY results).

---

## Sort-Order Projections

Sort-order projections store all columns in an alternative ORDER BY, enabling fast lookups on columns that differ from the table's primary sort key.

### With Explicit Name

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });

    entity.HasProjection("proj_by_customer")
        .OrderBy(x => x.CustomerId)
        .ThenBy(x => x.OrderDate)
        .Build();
});
```

**Generated SQL:**

```sql
PROJECTION proj_by_customer (SELECT * ORDER BY ("CustomerId", "OrderDate"))
```

### With Auto-Generated Name

When no name is provided, the projection name is auto-generated from the table name and columns: `{TableName}_ord_{Column1}_{Column2}`.

```csharp
entity.HasProjection()
    .OrderBy(x => x.Region)
    .ThenBy(x => x.CreatedAt)
    .Build();
```

### Multiple Sort Columns

```csharp
entity.HasProjection("proj_by_category_date")
    .OrderBy(x => x.Category)
    .ThenBy(x => x.SubCategory)
    .ThenBy(x => x.CreatedAt)
    .Build();
```

---

## Aggregation Projections

Aggregation projections store pre-computed GROUP BY results. ClickHouse uses them to answer aggregate queries without scanning the full table.

### Basic Aggregation

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });

    entity.HasProjection("proj_daily_totals")
        .GroupBy(x => x.Category)
        .Select(g => new
        {
            Category = g.Key,
            Total = g.Sum(x => x.Amount),
            OrderCount = g.Count()
        })
        .Build();
});
```

**Generated SQL:**

```sql
PROJECTION proj_daily_totals (
    SELECT "Category", sum("Amount"), count()
    GROUP BY "Category"
)
```

### ClickHouse-Specific Aggregates

Aggregation projections support ClickHouse-specific aggregate functions beyond the standard LINQ aggregates.

```csharp
entity.HasProjection("proj_user_stats")
    .GroupBy(x => x.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        UniqueProducts = g.UniqExact(x => x.ProductId),
        TotalSpent = g.Sum(x => x.Amount),
        MedianOrder = g.Median(x => x.Amount),
        P95Order = g.Quantile(0.95, x => x.Amount),
        LastCategory = g.ArgMax(x => x.Category, x => x.OrderDate)
    })
    .Build();
```

Supported ClickHouse aggregates in projections:

| Method | ClickHouse Function | Description |
|---|---|---|
| `g.Sum(x => x.Col)` | `sum(Col)` | Sum of values |
| `g.Count()` | `count()` | Row count |
| `g.Min(x => x.Col)` | `min(Col)` | Minimum value |
| `g.Max(x => x.Col)` | `max(Col)` | Maximum value |
| `g.Uniq(x => x.Col)` | `uniq(Col)` | Approximate distinct count |
| `g.UniqExact(x => x.Col)` | `uniqExact(Col)` | Exact distinct count |
| `g.ArgMax(x => x.Arg, x => x.Val)` | `argMax(Arg, Val)` | Value of Arg at max Val |
| `g.Quantile(level, x => x.Col)` | `quantile(level)(Col)` | Quantile estimate |
| `g.Median(x => x.Col)` | `median(Col)` | Median (50th percentile) |

---

## Raw SQL Projections

For projections that cannot be expressed with the fluent API, use raw SQL.

```csharp
entity.HasProjection(
    "proj_by_region",
    "SELECT * ORDER BY (\"Region\", \"OrderDate\")");

entity.HasProjection(
    "proj_hourly_summary",
    @"SELECT toStartOfHour(""Timestamp"") AS Hour,
             sum(""Amount"") AS TotalAmount,
             count() AS EventCount
      GROUP BY Hour");
```

---

## How Projections Work

1. **Automatic maintenance**: ClickHouse populates projections automatically when data is inserted. No separate INSERT is required.

2. **Transparent query optimization**: The query optimizer evaluates whether a projection can satisfy a query more efficiently than scanning the main table. If so, it reads from the projection instead.

3. **No query changes needed**: Queries are written against the main table. The optimizer selects projections transparently.

4. **Storage cost**: Each projection stores a copy of the relevant data. Sort-order projections store all columns; aggregation projections store only the selected columns and aggregated results.

---

## When to Use Projections

| Scenario | Projection Type | Example |
|---|---|---|
| Frequent lookups by a non-primary column | Sort-order | Customer lookup in a date-ordered table |
| Dashboard aggregations | Aggregation | Daily/hourly summaries |
| Multiple access patterns on the same table | Sort-order | Query by region AND by date |
| Pre-computed statistics | Aggregation | Unique counts, totals, percentiles |

### Projections vs. Materialized Views

- **Projections** are part of the table definition, automatically maintained, and transparently used by the optimizer. They cannot filter data or join with other tables.
- **Materialized views** are separate tables populated by triggers on INSERT. They can transform data, filter rows, and target different engines. Use materialized views when you need data transformation beyond simple reordering or aggregation.

---

## Complete Example

```csharp
modelBuilder.Entity<PageView>(entity =>
{
    entity.HasKey(x => x.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.PagePath });

    // Fast lookup by user
    entity.HasProjection("proj_by_user")
        .OrderBy(x => x.UserId)
        .ThenBy(x => x.Timestamp)
        .Build();

    // Pre-computed hourly page view counts
    entity.HasProjection("proj_hourly_stats")
        .GroupBy(x => x.PagePath)
        .Select(g => new
        {
            PagePath = g.Key,
            ViewCount = g.Count(),
            UniqueVisitors = g.UniqExact(x => x.UserId)
        })
        .Build();
});
```

---

## See Also

- [Column Features](column-features.md) -- compression codecs, computed columns, aggregate function types
- [Skip Indices](skip-indices.md) -- data skipping indices for query acceleration
- [Partitioning](partitioning.md) -- PARTITION BY strategies
