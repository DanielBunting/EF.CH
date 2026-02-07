# Set Operations

EF.CH supports ClickHouse set operations: `UNION ALL`, `UNION DISTINCT`, `INTERSECT`, and `EXCEPT`. Standard EF Core LINQ methods work out of the box, and convenience extensions are provided for common patterns.

## ClickHouse Set Operation Syntax

ClickHouse requires explicit `ALL` or `DISTINCT` keywords — bare `UNION` is not valid:

| EF Core Method | ClickHouse SQL |
|----------------|----------------|
| `.Concat()` | `UNION ALL` |
| `.Union()` | `UNION DISTINCT` |
| `.Intersect()` | `INTERSECT DISTINCT` |
| `.Except()` | `EXCEPT DISTINCT` |

EF.CH automatically generates the correct ClickHouse syntax.

## Basic Usage

### UNION ALL (Concat)

Combines two queries, keeping all rows including duplicates:

```csharp
var q1Sales = context.Sales.Where(s => s.Quarter == "Q1");
var q2Sales = context.Sales.Where(s => s.Quarter == "Q2");

var combined = await q1Sales.Concat(q2Sales)
    .OrderBy(s => s.Product)
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Sales" WHERE "Quarter" = 'Q1'
UNION ALL
SELECT ... FROM "Sales" WHERE "Quarter" = 'Q2'
ORDER BY "Product"
```

### UNION DISTINCT (Union)

Combines two queries, removing duplicate rows:

```csharp
var northProducts = context.Sales.Where(s => s.Region == "North").Select(s => s.Product);
var southProducts = context.Sales.Where(s => s.Region == "South").Select(s => s.Product);

var allProducts = await northProducts.Union(southProducts).ToListAsync();
```

Generates:
```sql
SELECT "Product" FROM "Sales" WHERE "Region" = 'North'
UNION DISTINCT
SELECT "Product" FROM "Sales" WHERE "Region" = 'South'
```

### INTERSECT

Returns rows common to both queries:

```csharp
var bothRegions = await northProducts.Intersect(southProducts).ToListAsync();
```

Generates:
```sql
SELECT "Product" FROM "Sales" WHERE "Region" = 'North'
INTERSECT DISTINCT
SELECT "Product" FROM "Sales" WHERE "Region" = 'South'
```

### EXCEPT

Returns rows in the first query but not the second:

```csharp
var northOnly = await northProducts.Except(southProducts).ToListAsync();
```

Generates:
```sql
SELECT "Product" FROM "Sales" WHERE "Region" = 'North'
EXCEPT DISTINCT
SELECT "Product" FROM "Sales" WHERE "Region" = 'South'
```

## Convenience Extensions

### UnionAll — Chain Multiple Queries

```csharp
using EF.CH.Extensions;

var q1 = context.Sales.Where(s => s.Quarter == "Q1");
var q2 = context.Sales.Where(s => s.Quarter == "Q2");
var q3 = context.Sales.Where(s => s.Quarter == "Q3");

// Chain multiple UNION ALL operations
var allQuarters = await q1.UnionAll(q2, q3)
    .OrderBy(s => s.Quarter)
    .ToListAsync();
```

### UnionDistinct — Chain Multiple Distinct Unions

```csharp
var allProducts = await query1.UnionDistinct(query2, query3).ToListAsync();
```

### SetOperationBuilder — Fluent API

For complex set operation chains:

```csharp
using EF.CH.Extensions;

var result = await highValueSales
    .AsSetOperation()
    .UnionAll(northSales)       // Add North region sales
    .Except(lowValueSales)      // Remove low-value items
    .Build()
    .OrderByDescending(s => s.Amount)
    .Take(100)
    .ToListAsync();
```

The builder supports:
- `.UnionAll(query)` — UNION ALL
- `.UnionDistinct(query)` — UNION DISTINCT
- `.Intersect(query)` — INTERSECT DISTINCT
- `.Except(query)` — EXCEPT DISTINCT
- `.Build()` — Returns the composed `IQueryable<T>`

## Complete Example

```csharp
public class Sale
{
    public Guid Id { get; set; }
    public string Product { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Quarter { get; set; } = string.Empty;
}

modelBuilder.Entity<Sale>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => x.Id);
});
```

### Usage

```csharp
// Combine all quarter data
var q1 = context.Sales.Where(s => s.Quarter == "Q1");
var q2 = context.Sales.Where(s => s.Quarter == "Q2");
var q3 = context.Sales.Where(s => s.Quarter == "Q3");

var allSales = await q1.UnionAll(q2, q3)
    .OrderBy(s => s.Quarter)
    .ThenBy(s => s.Product)
    .ToListAsync();

// Find products sold in both regions
var northProducts = context.Sales.Where(s => s.Region == "North").Select(s => s.Product);
var southProducts = context.Sales.Where(s => s.Region == "South").Select(s => s.Product);
var commonProducts = await northProducts.Intersect(southProducts).ToListAsync();

// Complex set operation with builder
var premium = context.Sales.Where(s => s.Amount > 500);
var north = context.Sales.Where(s => s.Region == "North");
var budget = context.Sales.Where(s => s.Amount < 100);

var result = await premium
    .AsSetOperation()
    .UnionAll(north)
    .Except(budget)
    .Build()
    .OrderByDescending(s => s.Amount)
    .Take(50)
    .ToListAsync();
```

## Requirements

- Queries in a set operation must have the same column structure (same entity type or projection shape)
- Ordering (`.OrderBy()`) should be applied after the set operation, not within individual queries

## See Also

- [Query Modifiers](query-modifiers.md) - FINAL, SAMPLE, PREWHERE query hints
- [CTEs](cte.md) - Common Table Expressions
- [ClickHouse UNION Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/union)
