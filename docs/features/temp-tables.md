# Temporary Tables

EF.CH provides typed temporary tables for intermediate results, multi-step pipelines, and ETL staging. Tables use the Memory engine with auto-generated unique names and are automatically dropped when disposed.

## Why Temporary Tables?

Complex analytics workflows often need intermediate storage:

- **Multi-step pipelines**: Filter data, aggregate, then join back
- **ETL staging**: Load external data into a temp table before transforming
- **Query optimization**: Materialize expensive subqueries once, reference multiple times
- **Ad-hoc analysis**: Create throwaway tables for exploratory queries

Temp tables avoid the overhead of permanent table DDL and cleanup.

## Basic Usage

### Create Empty Temp Table

```csharp
using EF.CH.Extensions;

// Create from entity schema
await using var temp = await context.CreateTempTableAsync<Order>("temp_orders");

// Insert entities
await temp.InsertAsync(entities);

// Query with full LINQ
var results = await temp.Query()
    .Where(o => o.Amount > 100)
    .OrderByDescending(o => o.Amount)
    .ToListAsync();
```

### Create from Query

```csharp
// Create and populate in one step
await using var temp = await context.Orders
    .Where(o => o.Category == "Electronics")
    .ToTempTableAsync(context, "temp_electronics");

var count = await temp.Query().CountAsync();
```

### Insert from Query

```csharp
await using var temp = await context.CreateTempTableAsync<Order>("temp_combined");

// Insert from multiple queries
await context.Orders
    .Where(o => o.Category == "Books")
    .InsertIntoTempTableAsync(temp);

await context.Orders
    .Where(o => o.Category == "Clothing")
    .InsertIntoTempTableAsync(temp);
```

## How It Works

1. **`CreateTempTableAsync<T>()`** reads the entity's column names and types from EF Core metadata
2. Generates `CREATE TABLE _tmp_Order_a1b2c3d4 (...) ENGINE = Memory` DDL
3. Returns a `TempTableHandle<T>` for inserting and querying
4. **`Query()`** returns `context.Set<T>().FromSqlRaw("SELECT * FROM ...")` giving full LINQ composability
5. **`DisposeAsync()`** executes `DROP TABLE IF EXISTS` to clean up

Tables use the **Memory engine** (in-RAM, no disk I/O) for fast reads and writes. They are created as regular tables with unique names for compatibility with ClickHouse's stateless HTTP protocol.

## TempTableHandle

The handle returned by `CreateTempTableAsync` provides:

| Method | Description |
|--------|-------------|
| `Query()` | Returns `IQueryable<T>` with full LINQ support |
| `InsertAsync(entities)` | Inserts a collection of entities |
| `InsertFromQueryAsync(query)` | Server-side INSERT ... SELECT |
| `TableName` | The generated table name |
| `DisposeAsync()` | Drops the table |

## TempTableScope

Use `TempTableScope` to manage multiple temp tables with automatic cleanup:

```csharp
await using var scope = context.BeginTempTableScope();

// Create multiple temp tables
var highValue = await scope.CreateFromQueryAsync(
    context.Orders.Where(o => o.Amount > 200));

var recent = await scope.CreateFromQueryAsync(
    context.Orders.Where(o => o.OrderDate > cutoff));

// Query both
var highValueCount = await highValue.Query().CountAsync();
var recentCount = await recent.Query().CountAsync();

// All tables dropped when scope disposes (LIFO order)
```

## Naming

Table names are auto-generated when not specified:

```csharp
// Auto-generated: _tmp_Order_a1b2c3d4
await using var temp = await context.CreateTempTableAsync<Order>();

// Explicit name
await using var temp2 = await context.CreateTempTableAsync<Order>("my_staging_table");
```

Auto-generated names use the pattern `_tmp_{EntityName}_{8-char-guid}` to ensure uniqueness.

## LINQ Composition

`Query()` returns a standard `IQueryable<T>`, so all LINQ operators work:

```csharp
await using var temp = await context.CreateTempTableAsync<Order>("temp_analysis");
await temp.InsertAsync(orders);

// Where, OrderBy, Take
var top10 = await temp.Query()
    .Where(o => o.Amount > 50)
    .OrderByDescending(o => o.Amount)
    .Take(10)
    .ToListAsync();

// GroupBy + aggregation
var byCategory = await temp.Query()
    .GroupBy(o => o.Category)
    .Select(g => new { Category = g.Key, Total = g.Sum(o => o.Amount) })
    .ToListAsync();

// Count, Sum, Average
var avg = await temp.Query().AverageAsync(o => o.Amount);

// Select projection
var names = await temp.Query()
    .Select(o => o.Category)
    .Distinct()
    .ToListAsync();
```

## Extension Methods

| Method | On | Description |
|--------|----|-------------|
| `CreateTempTableAsync<T>()` | `DbContext` | Creates empty temp table |
| `BeginTempTableScope()` | `DbContext` | Creates a scope for multiple tables |
| `ToTempTableAsync()` | `IQueryable<T>` | Creates temp table from query results |
| `InsertIntoTempTableAsync()` | `IQueryable<T>` | Inserts query results into existing temp table |

## Complete Example

```csharp
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// Multi-step analytics pipeline
await using var scope = context.BeginTempTableScope();

// Step 1: Stage high-value recent orders
var staging = await scope.CreateFromQueryAsync(
    context.Orders
        .Where(o => o.Amount > 100 && o.OrderDate > DateTime.UtcNow.AddDays(-30)));

// Step 2: Add more data from another source
await context.Orders
    .Where(o => o.Category == "VIP")
    .InsertIntoTempTableAsync(staging);

// Step 3: Analyze the staged data
var summary = await staging.Query()
    .GroupBy(o => o.Category)
    .Select(g => new
    {
        Category = g.Key,
        Count = g.Count(),
        TotalAmount = g.Sum(o => o.Amount),
        AvgAmount = g.Average(o => o.Amount)
    })
    .OrderByDescending(x => x.TotalAmount)
    .ToListAsync();

foreach (var row in summary)
{
    Console.WriteLine($"{row.Category}: {row.Count} orders, ${row.TotalAmount:F2} total");
}

// All temp tables automatically cleaned up
```

## Limitations

- **Entity must be in the model**: The entity type must be registered in the DbContext via `DbSet<T>` or `OnModelCreating`
- **Memory engine**: Data is stored in RAM; very large temp tables may cause memory pressure
- **Not change-tracked**: Entities inserted via `InsertAsync` are not tracked by EF Core
- **No indexes**: Memory engine tables don't support secondary indexes
- **Globally visible**: Tables are regular Memory tables with unique names, not ClickHouse session-scoped temporary tables (for HTTP protocol compatibility)

## See Also

- [Temp Table Sample](../../samples/TempTableSample/Program.cs)
- [Bulk Insert](bulk-insert.md) - High-performance batch inserts
- [INSERT ... SELECT](insert-select.md) - Server-side data movement
- [CTEs](cte.md) - Common Table Expressions for single-query intermediates
- [ClickHouse Memory Engine](https://clickhouse.com/docs/en/engines/table-engines/special/memory)
