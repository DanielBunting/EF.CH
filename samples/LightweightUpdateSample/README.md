# LightweightUpdateSample

Demonstrates ClickHouse lightweight UPDATE mutations via `ExecuteUpdateAsync`.

## What This Shows

- Single column update via `ExecuteUpdateAsync`
- Multiple column updates in one mutation
- Expression-based updates (e.g., `Price * 1.1`)
- Update all rows (no WHERE filter)
- Why `SaveChanges()` tracked updates remain blocked

## How It Works

ClickHouse supports `ALTER TABLE ... UPDATE` mutations for bulk column updates. EF Core 7+ provides `ExecuteUpdateAsync` which we wire to generate this syntax.

| Operation | SQL Generated |
|-----------|---------------|
| Single column | `ALTER TABLE "Products" UPDATE "Status" = 'archived' WHERE "Status" = 'discontinued'` |
| Multiple columns | `ALTER TABLE "Products" UPDATE "Category" = 'lit', "Status" = 'reviewed' WHERE ...` |
| Expression-based | `ALTER TABLE "Products" UPDATE "Price" = "Price" * 1.1 WHERE ...` |
| All rows | `ALTER TABLE "Products" UPDATE "Status" = 'final' WHERE 1` |

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Key Code

### Single Column Update

```csharp
await context.Products
    .Where(p => p.Status == "discontinued")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "archived"));
```

### Multiple Column Update

```csharp
await context.Products
    .Where(p => p.Category == "books")
    .ExecuteUpdateAsync(s => s
        .SetProperty(p => p.Category, "literature")
        .SetProperty(p => p.Status, "reviewed"));
```

### Expression-Based Update

```csharp
await context.Products
    .Where(p => p.Category == "electronics")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, p => p.Price * 1.10m));
```

## Important Notes

- **Mutations are async**: ClickHouse processes mutations in the background. Results may not be immediately visible.
- **No joins**: ClickHouse mutations don't support multi-table updates.
- **SaveChanges blocked**: Single-row tracked updates via `SaveChanges()` remain intentionally blocked. Use `ExecuteUpdateAsync` for bulk operations.
- **WHERE required**: If no filter is specified, `WHERE 1` is generated (ClickHouse requires WHERE for mutations).
