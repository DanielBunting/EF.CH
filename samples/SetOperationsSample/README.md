# SetOperationsSample

Demonstrates ClickHouse set operations (UNION, INTERSECT, EXCEPT) via LINQ.

## What This Shows

- `Concat()` — UNION ALL (keeps duplicates)
- `Union()` — UNION DISTINCT (removes duplicates)
- `Intersect()` — rows common to both queries
- `Except()` — rows in first query but not second
- `UnionAll()` / `UnionDistinct()` — convenience for multiple queries
- `AsSetOperation()` — fluent builder for chaining operations

## How It Works

EF Core's built-in set operations translate directly to ClickHouse SQL:

| LINQ Method | ClickHouse SQL |
|-------------|----------------|
| `Concat()` | `UNION ALL` |
| `Union()` | `UNION DISTINCT` |
| `Intersect()` | `INTERSECT DISTINCT` |
| `Except()` | `EXCEPT DISTINCT` |

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Key Code

### Basic Set Operations

```csharp
// UNION ALL - combine with duplicates
var all = await q1.Concat(q2).ToListAsync();

// UNION DISTINCT - combine without duplicates
var unique = await q1.Union(q2).ToListAsync();

// INTERSECT - common to both
var common = await q1.Intersect(q2).ToListAsync();

// EXCEPT - in first but not second
var exclusive = await q1.Except(q2).ToListAsync();
```

### Multiple Queries

```csharp
// Chain multiple UNION ALL operations
var all = await q1.UnionAll(q2, q3, q4).ToListAsync();

// Chain multiple UNION DISTINCT operations
var unique = await q1.UnionDistinct(q2, q3).ToListAsync();
```

### Fluent Builder

```csharp
var result = await query1
    .AsSetOperation()
    .UnionAll(query2)
    .Except(query3)
    .Build()
    .OrderBy(e => e.Timestamp)
    .Take(100)
    .ToListAsync();
```
