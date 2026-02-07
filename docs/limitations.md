# Limitations

This page documents what doesn't work in EF.CH and provides workarounds.

## Database-Level Limitations

These are inherent to ClickHouse, not the EF Core provider:

### No ACID Transactions

ClickHouse doesn't support transactions. `SaveChanges()` batches operations but there's no rollback.

```csharp
// No transaction wrapping these operations
context.Orders.Add(order1);
context.Orders.Add(order2);
await context.SaveChangesAsync();  // If order2 fails, order1 is still inserted
```

**Workaround:** Design for idempotency. Use unique IDs to detect duplicates.

### No Row-Level UPDATE (via SaveChanges)

```csharp
var order = await context.Orders.FindAsync(id);
order.Status = "Completed";
await context.SaveChangesAsync();  // Throws ClickHouseUnsupportedOperationException
```

**Solutions:**

1. **`ExecuteUpdateAsync`** (recommended for bulk updates): Generates `ALTER TABLE ... UPDATE`
   ```csharp
   await context.Orders
       .Where(o => o.Status == "pending")
       .ExecuteUpdateAsync(s => s.SetProperty(o => o.Status, "Completed"));
   ```

2. **ReplacingMergeTree**: Insert new version, merge deduplicates
   ```csharp
   entity.UseReplacingMergeTree(x => x.Version, x => x.Id);
   ```

3. **Delete and re-insert**:
   ```csharp
   context.Orders.Remove(order);
   await context.SaveChangesAsync();

   order.Status = "Completed";
   context.Orders.Add(order);
   await context.SaveChangesAsync();
   ```

4. **Append-only design**: Track changes as events instead of updating state.

See [Update Operations](features/update-operations.md) for full documentation.

### No Auto-Increment / IDENTITY

```csharp
public class Order
{
    public int Id { get; set; }  // No IDENTITY support
}
```

**Workaround:** Use UUID or application-generated IDs:

```csharp
public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
}
```

### No Foreign Key Enforcement

```csharp
modelBuilder.Entity<Order>()
    .HasOne<Customer>()
    .WithMany()
    .HasForeignKey(o => o.CustomerId);  // Not enforced by database
```

**Workaround:** Validate relationships in application code:

```csharp
public async Task CreateOrder(Order order)
{
    var customerExists = await context.Customers.AnyAsync(c => c.Id == order.CustomerId);
    if (!customerExists)
        throw new InvalidOperationException("Customer not found");

    context.Orders.Add(order);
    await context.SaveChangesAsync();
}
```

### DELETE Returns 0 Rows Affected

`ExecuteDeleteAsync()` may return 0 even when rows are deleted. This is a ClickHouse HTTP interface limitation.

```csharp
var affected = await context.Orders
    .Where(o => o.CreatedAt < cutoff)
    .ExecuteDeleteAsync();
// affected may be 0 even if rows were deleted
```

**Workaround:** Don't rely on the return value. Query afterward if you need confirmation.

## EF Core Feature Limitations

### Migrations: No Column Rename

```csharp
// This migration operation throws NotSupportedException
migrationBuilder.RenameColumn(
    name: "OldName",
    table: "Orders",
    newName: "NewName");
```

**Workaround:** Add new column, migrate data, drop old column:

```csharp
migrationBuilder.AddColumn<string>(name: "NewName", table: "Orders");
// Manually migrate data via raw SQL
migrationBuilder.DropColumn(name: "OldName", table: "Orders");
```

### Migrations: No Primary Key Constraints

Primary keys in ClickHouse are defined by `ORDER BY`, not constraints:

```csharp
// This throws NotSupportedException
migrationBuilder.AddPrimaryKey(
    name: "PK_Orders",
    table: "Orders",
    column: "Id");
```

**Workaround:** Use `HasKey()` and engine configuration:

```csharp
entity.HasKey(e => e.Id);
entity.UseMergeTree(x => x.Id);
```

### Migrations: No Unique Constraints

```csharp
// This throws NotSupportedException
migrationBuilder.AddUniqueConstraint(
    name: "UQ_Email",
    table: "Users",
    column: "Email");
```

**Workaround:** Use `ReplacingMergeTree` for deduplication or enforce uniqueness in application code.

### Change Tracking: Limited for Keyless Entities

Keyless entities can't be tracked:

```csharp
entity.HasNoKey();

// Can insert
context.Events.Add(event);
await context.SaveChangesAsync();

// Cannot update or delete via tracking
context.Events.Remove(event);  // Throws
```

**Workaround:** Use bulk operations:

```csharp
await context.Events
    .Where(e => e.Id == targetId)
    .ExecuteDeleteAsync();
```

### Navigation Properties

Navigation properties work for queries but:
- No cascade delete
- No automatic include based on foreign keys
- Must configure relationships explicitly

```csharp
// Works for querying
var ordersWithCustomer = await context.Orders
    .Include(o => o.Customer)
    .ToListAsync();

// Cascade delete doesn't work
context.Customers.Remove(customer);  // Won't delete related orders
```

## Query Limitations

### Complex Joins

ClickHouse handles simple joins but complex multi-table joins can be slow:

```csharp
// May be slow
var result = await context.Orders
    .Join(context.Customers, o => o.CustomerId, c => c.Id, (o, c) => new { o, c })
    .Join(context.Products, x => x.o.ProductId, p => p.Id, (x, p) => new { x.o, x.c, p })
    .ToListAsync();
```

**Workaround:**
- Denormalize data
- Use materialized views for pre-joined data
- Query tables separately and join in application

### Subqueries

Some subquery patterns may not translate:

```csharp
// May fail or be inefficient
var customers = await context.Customers
    .Where(c => context.Orders.Any(o => o.CustomerId == c.Id && o.Total > 1000))
    .ToListAsync();
```

**Workaround:** Use raw SQL or restructure query:

```csharp
var customerIds = await context.Orders
    .Where(o => o.Total > 1000)
    .Select(o => o.CustomerId)
    .Distinct()
    .ToListAsync();

var customers = await context.Customers
    .Where(c => customerIds.Contains(c.Id))
    .ToListAsync();
```

## Performance Considerations

### Single-Row Operations

ClickHouse is optimized for batch operations. Single-row inserts are inefficient:

```csharp
// Inefficient
foreach (var item in items)
{
    context.Items.Add(item);
    await context.SaveChangesAsync();
}

// Better
context.Items.AddRange(items);
await context.SaveChangesAsync();
```

### FINAL Modifier Overhead

Using `.Final()` forces deduplication but adds query overhead:

```csharp
// Fast but may return duplicates
var users = await context.Users.ToListAsync();

// Slower but deduplicated
var users = await context.Users.Final().ToListAsync();
```

### Query on Non-Primary Columns

Queries that don't filter on `ORDER BY` columns scan more data:

```csharp
// Fast: Timestamp is in ORDER BY
context.Events.Where(e => e.Timestamp > cutoff)

// Slow: Full scan
context.Events.Where(e => e.UserId == userId)
```

## Feature Support Matrix

| Feature | SQL Server | ClickHouse | Notes |
|---------|-----------|------------|-------|
| Transactions | ✅ | ❌ | Eventual consistency |
| UPDATE | ✅ | ⚠️ | Bulk via `ExecuteUpdateAsync`; no row-level tracking |
| DELETE | ✅ | ⚠️ | Async/eventual |
| Auto-increment | ✅ | ❌ | Use UUID |
| Foreign keys | ✅ | ❌ | Application-level |
| Unique constraints | ✅ | ❌ | ReplacingMergeTree |
| Check constraints | ✅ | ❌ | Application-level |
| Column rename | ✅ | ❌ | Add/drop columns |
| Complex joins | ✅ | ⚠️ | Simple joins OK |
| Subqueries | ✅ | ⚠️ | May need restructuring |

## See Also

- [ClickHouse Concepts](clickhouse-concepts.md) - Understanding ClickHouse architecture
- [ReplacingMergeTree](engines/replacing-mergetree.md) - Deduplication workaround
- [Update Operations](features/update-operations.md) - Bulk update via ExecuteUpdateAsync
- [Delete Operations](features/delete-operations.md) - Delete strategies
