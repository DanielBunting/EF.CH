# Update Operations

ClickHouse supports bulk mutations via `ALTER TABLE ... UPDATE`. EF.CH wires EF Core's `ExecuteUpdateAsync` to this mechanism.

## Important: Row-Level vs Bulk Updates

| Approach | Supported | Notes |
|----------|-----------|-------|
| `ExecuteUpdateAsync` | Yes | Generates `ALTER TABLE ... UPDATE` |
| `SaveChanges()` with tracked entities | No | Throws `ClickHouseUnsupportedOperationException` |

Row-level tracked updates via `SaveChanges()` remain intentionally blocked because single-row mutations are extremely inefficient in ClickHouse. Use `ExecuteUpdateAsync` for bulk operations instead.

## Basic Usage

### Single Column Update

```csharp
await context.Products
    .Where(p => p.Status == "discontinued")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "archived"));
```

Generates:
```sql
ALTER TABLE "Products" UPDATE "Status" = {p0:String}
WHERE "Status" = {p1:String}
```

### Multiple Column Update

```csharp
await context.Orders
    .Where(o => o.Region == "NA")
    .ExecuteUpdateAsync(s => s
        .SetProperty(o => o.Region, "North America")
        .SetProperty(o => o.UpdatedAt, DateTime.UtcNow));
```

Generates:
```sql
ALTER TABLE "Orders" UPDATE "Region" = {p0:String}, "UpdatedAt" = {p1:DateTime64(3)}
WHERE "Region" = {p2:String}
```

### Expression-Based Update (Computed Values)

```csharp
await context.Products
    .Where(p => p.Category == "electronics")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, p => p.Price * 1.1m));
```

Generates:
```sql
ALTER TABLE "Products" UPDATE "Price" = "Price" * 1.1
WHERE "Category" = {p0:String}
```

### Update All Rows

When no `Where` clause is specified, all rows are updated. ClickHouse requires a WHERE clause for mutations, so `WHERE 1` is emitted:

```csharp
await context.Products
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "final"));
```

Generates:
```sql
ALTER TABLE "Products" UPDATE "Status" = {p0:String}
WHERE 1
```

## How ClickHouse Mutations Work

ClickHouse mutations are **asynchronous** operations:

1. The `ALTER TABLE ... UPDATE` command is submitted and returns immediately
2. ClickHouse rewrites affected data parts in the background
3. Updated data becomes visible once the mutation completes

For testing or when you need to wait for the mutation to complete:

```csharp
await context.Products
    .Where(p => p.Status == "old")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "new"));

// Wait for mutation to process (only needed in tests or time-sensitive scenarios)
await Task.Delay(500);

// Now the updated data is visible
var updated = await context.Products.Where(p => p.Status == "new").ToListAsync();
```

## Limitations

- **No joins in UPDATE**: ClickHouse mutations don't support multi-table joins. Attempting to update with joins throws `InvalidOperationException`
- **No row-level tracking**: `SaveChanges()` with modified entities still throws — use `ExecuteUpdateAsync` instead
- **Asynchronous processing**: Mutations are eventually consistent; updated rows may not be immediately visible
- **Return value**: `ExecuteUpdateAsync` may return 0 even when rows are updated (ClickHouse HTTP interface limitation)

## When to Use

| Scenario | Approach |
|----------|----------|
| Bulk status changes | `ExecuteUpdateAsync` |
| Price adjustments across categories | `ExecuteUpdateAsync` |
| Data migration/cleanup | `ExecuteUpdateAsync` |
| Single entity modification | `ReplacingMergeTree` or delete-and-reinsert |
| Frequent per-row updates | Reconsider design (append-only, ReplacingMergeTree) |

## Complete Example

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Status { get; set; } = string.Empty;
}

modelBuilder.Entity<Product>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => x.Id);
});
```

### Usage

```csharp
// Mark old products as archived
await context.Products
    .Where(p => p.Status == "discontinued")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "archived"));

// 10% price increase for electronics
await context.Products
    .Where(p => p.Category == "electronics")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, p => p.Price * 1.10m));

// Update multiple columns at once
await context.Products
    .Where(p => p.Category == "books")
    .ExecuteUpdateAsync(s => s
        .SetProperty(p => p.Category, "literature")
        .SetProperty(p => p.Status, "reviewed"));
```

## See Also

- [Delete Operations](delete-operations.md) - DELETE strategies
- [ReplacingMergeTree](../engines/replacing-mergetree.md) - For row-level "update" semantics
- [Limitations](../limitations.md) - What doesn't work
- [ClickHouse ALTER UPDATE Docs](https://clickhouse.com/docs/en/sql-reference/statements/alter/update)
