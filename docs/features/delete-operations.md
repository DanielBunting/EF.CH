# Delete Operations

ClickHouse supports DELETE operations, but they work differently from traditional databases. EF.CH provides two strategies for deleting data.

## Delete Strategies

### Lightweight Delete (Default)

Marks rows as deleted immediately. Physical deletion occurs during background merges.

```csharp
options.UseClickHouse("Host=localhost;Database=mydb");
// Lightweight is the default
```

Generates:
```sql
DELETE FROM "Events" WHERE "Id" = @p0
```

### Mutation Delete

Asynchronous operation that rewrites data parts. Use for bulk maintenance only.

```csharp
options.UseClickHouse("Host=localhost;Database=mydb",
    o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

Generates:
```sql
ALTER TABLE "Events" DELETE WHERE "Id" = @p0
```

## Strategy Comparison

| Aspect | Lightweight | Mutation |
|--------|-------------|----------|
| Speed | Instant marking | Async rewrite |
| Visibility | Immediate | Eventually consistent |
| Resource Usage | Low | High (rewrites parts) |
| Row Count | Returns count | Returns 0 |
| Use Case | Normal operations | Bulk maintenance |

## Entity Framework Delete

### Single Entity Delete

```csharp
// Find and delete
var entity = await context.Events.FirstAsync(e => e.Id == id);
context.Events.Remove(entity);
await context.SaveChangesAsync();
```

### Multiple Entity Delete

```csharp
// Delete multiple entities
var oldEvents = await context.Events
    .Where(e => e.CreatedAt < cutoff)
    .ToListAsync();

context.Events.RemoveRange(oldEvents);
await context.SaveChangesAsync();
```

### Bulk Delete (ExecuteDeleteAsync)

Most efficient for deleting many rows:

```csharp
// Delete all matching rows without loading them
await context.Events
    .Where(e => e.CreatedAt < DateTime.UtcNow.AddDays(-90))
    .ExecuteDeleteAsync();
```

**Note**: `ExecuteDeleteAsync` may return 0 even when rows are deleted (ClickHouse HTTP interface limitation).

## Complete Example

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime CreatedAt { get; set; }
    public string EventType { get; set; } = string.Empty;
}

public class EventDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=events");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
        });
    }
}
```

### Usage

```csharp
await using var context = new EventDbContext();

// Single delete
var event = await context.Events.FirstAsync(e => e.Id == targetId);
context.Events.Remove(event);
await context.SaveChangesAsync();

// Bulk delete
await context.Events
    .Where(e => e.EventType == "temp")
    .ExecuteDeleteAsync();
```

## UPDATE Operations

Row-level tracked updates via `SaveChanges()` are not supported — but bulk updates via `ExecuteUpdateAsync` work:

```csharp
// This works! Generates ALTER TABLE ... UPDATE
await context.Events
    .Where(e => e.EventType == "temp")
    .ExecuteUpdateAsync(s => s.SetProperty(e => e.EventType, "archived"));

// This still throws ClickHouseUnsupportedOperationException
var entity = await context.Events.FirstAsync();
entity.EventType = "modified";
await context.SaveChangesAsync();
```

See [Update Operations](update-operations.md) for full documentation on `ExecuteUpdateAsync`.

### Alternative Approaches

**1. Use ReplacingMergeTree** (recommended for row-level semantics):

```csharp
entity.UseReplacingMergeTree(x => x.UpdatedAt, x => x.Id);

// "Update" by inserting new version
context.Events.Add(new Event
{
    Id = existingId,  // Same ID
    UpdatedAt = DateTime.UtcNow,  // Newer version
    EventType = "modified"
});
await context.SaveChangesAsync();

// Query with FINAL to get latest version
var current = await context.Events
    .Final()
    .FirstAsync(e => e.Id == existingId);
```

**2. Delete and Re-insert**:

```csharp
var old = await context.Events.FirstAsync(e => e.Id == targetId);
context.Events.Remove(old);

context.Events.Add(new Event
{
    Id = Guid.NewGuid(),  // New ID
    EventType = "modified",
    CreatedAt = DateTime.UtcNow
});
await context.SaveChangesAsync();
```

## Composite Key Delete

Works with composite keys:

```csharp
public class OrderItem
{
    public Guid OrderId { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
}

modelBuilder.Entity<OrderItem>(entity =>
{
    entity.HasKey(e => new { e.OrderId, e.ProductId });
    entity.UseMergeTree(x => new { x.OrderId, x.ProductId });
});

// Delete with composite key
var item = await context.OrderItems
    .FirstAsync(i => i.OrderId == orderId && i.ProductId == productId);
context.OrderItems.Remove(item);
await context.SaveChangesAsync();
```

## When to Use Each Strategy

### Use Lightweight Delete (Default)

- Normal application deletes
- Interactive user operations
- When immediate visibility matters
- Most use cases

### Use Mutation Delete

- Bulk data cleanup jobs
- Background maintenance tasks
- When you don't need immediate consistency
- Very large delete operations

```csharp
// Configure for maintenance job
services.AddDbContext<MaintenanceContext>(options =>
    options.UseClickHouse(connectionString,
        o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation)));
```

## Best Practices

### Prefer Bulk Operations

```csharp
// Good: Single bulk delete
await context.Events
    .Where(e => e.CreatedAt < cutoff)
    .ExecuteDeleteAsync();

// Avoid: Loading then deleting
var events = await context.Events
    .Where(e => e.CreatedAt < cutoff)
    .ToListAsync();
context.Events.RemoveRange(events);
await context.SaveChangesAsync();
```

### Use TTL for Automatic Cleanup

Instead of manual deletes, consider TTL:

```csharp
entity.HasTtl("CreatedAt + INTERVAL 90 DAY");
```

### Partition-Based Deletion

For time-series data, drop entire partitions:

```csharp
await context.Database.ExecuteSqlRawAsync(
    @"ALTER TABLE ""Events"" DROP PARTITION '202301'");
```

## Limitations

- **No row-level UPDATE**: `SaveChanges()` with modified entities throws; use `ExecuteUpdateAsync` for bulk updates
- **Eventual Consistency**: Deleted rows may briefly appear in queries
- **Row Count**: HTTP interface may not return accurate affected counts
- **Async Mutations**: Mutation deletes run asynchronously

## See Also

- [Update Operations](update-operations.md) - Bulk updates via ExecuteUpdateAsync
- [ReplacingMergeTree](../engines/replacing-mergetree.md) - For update semantics
- [TTL](ttl.md) - For automatic data expiration
- [Partitioning](partitioning.md) - For partition-based deletion
- [ClickHouse DELETE Docs](https://clickhouse.com/docs/en/sql-reference/statements/delete)
