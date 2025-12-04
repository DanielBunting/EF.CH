# Keyless Entities

ClickHouse tables are typically append-only and don't require primary keys for entity tracking. EF.CH supports making all entities keyless by default.

## Why Keyless?

Traditional databases use primary keys to:
1. Uniquely identify rows for UPDATE/DELETE
2. Track entity state in the change tracker

ClickHouse is different:
- **No row-level UPDATE**: Use ReplacingMergeTree or delete-and-reinsert
- **Append-only pattern**: Most analytics data is write-once
- **ORDER BY is sufficient**: Determines data layout and query efficiency

## Configuration

### Enable Keyless by Default

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder.UseClickHouse(
        "Host=localhost;Database=analytics",
        o => o.UseKeylessEntitiesByDefault());
}
```

With this setting, all entities are keyless unless you explicitly call `HasKey()`.

### Manual Keyless

Without the global setting, mark individual entities:

```csharp
modelBuilder.Entity<PageView>(entity =>
{
    entity.HasNoKey();
    entity.UseMergeTree(x => new { x.Timestamp, x.PageUrl });
});
```

## Complete Example

```csharp
public class AnalyticsDbContext : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();
    public DbSet<ApiRequest> ApiRequests => Set<ApiRequest>();
    public DbSet<User> Users => Set<User>();  // Has explicit key

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse(
            "Host=localhost;Database=analytics",
            o => o.UseKeylessEntitiesByDefault());
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // PageView - keyless (default)
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.UseMergeTree(x => new { x.Timestamp, x.PageUrl });
            entity.HasPartitionByDay(x => x.Timestamp);
        });

        // ApiRequest - also keyless (default)
        modelBuilder.Entity<ApiRequest>(entity =>
        {
            entity.UseMergeTree(x => new { x.Timestamp, x.Endpoint });
            entity.HasTtl("Timestamp + INTERVAL 90 DAY");
        });

        // User - explicit key overrides default
        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.Id);  // Override keyless
            entity.UseReplacingMergeTree(x => x.UpdatedAt, x => x.Id);
        });
    }
}
```

### Entities

```csharp
// Keyless - append-only analytics
public class PageView
{
    public DateTime Timestamp { get; set; }
    public string PageUrl { get; set; } = string.Empty;
    public string? UserId { get; set; }
    public string? Referrer { get; set; }
    public int DurationMs { get; set; }
}

// Keyless - append-only logging
public class ApiRequest
{
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public string Method { get; set; } = string.Empty;
    public int StatusCode { get; set; }
}

// Has key - supports tracking/updates via ReplacingMergeTree
public class User
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}
```

## Keyless Behavior

### What Works

```csharp
// Insert - works
context.PageViews.Add(new PageView { ... });
await context.SaveChangesAsync();

// Bulk insert - works
context.PageViews.AddRange(pageViews);
await context.SaveChangesAsync();

// Query - works
var views = await context.PageViews
    .Where(p => p.Timestamp > cutoff)
    .ToListAsync();

// Aggregation - works
var count = await context.PageViews.CountAsync();
```

### What Doesn't Work

```csharp
// Find by key - doesn't work (no key)
var view = await context.PageViews.FindAsync(id);  // Returns null

// Update tracking - doesn't work
var view = await context.PageViews.FirstAsync();
view.DurationMs = 5000;
await context.SaveChangesAsync();  // No effect - not tracked

// Remove - doesn't work via tracking
context.PageViews.Remove(view);  // Can't identify which row
```

## Override Keyless Default

Add `HasKey()` to opt out of keyless:

```csharp
modelBuilder.Entity<User>(entity =>
{
    entity.HasKey(e => e.Id);  // Override keyless default
    entity.UseReplacingMergeTree(x => x.UpdatedAt, x => x.Id);
});
```

## Use Cases

### Keyless (Default)

- Event streams
- Log data
- Metrics/telemetry
- Analytics events
- Audit trails
- Time-series data

### With Key

- User profiles (with ReplacingMergeTree)
- Configuration data
- Reference data needing updates
- State that needs change tracking

## Comparison

| Aspect | Keyless | With Key |
|--------|---------|----------|
| Insert | ✅ | ✅ |
| Query | ✅ | ✅ |
| Find() | ❌ | ✅ |
| Update tracking | ❌ | ✅ (with ReplacingMergeTree) |
| Remove tracking | ❌ | ✅ |
| Delete (bulk) | ✅ ExecuteDeleteAsync | ✅ Both methods |

## Bulk Operations Still Work

Even without keys, bulk operations work:

```csharp
// Bulk delete by predicate
await context.PageViews
    .Where(p => p.Timestamp < cutoff)
    .ExecuteDeleteAsync();

// This doesn't need entity tracking
```

## Migration Implications

Keyless entities don't generate primary key constraints:

```sql
-- With key
CREATE TABLE "Users" (
    "Id" UUID NOT NULL,
    ...
    PRIMARY KEY ("Id")  -- Not actually used by ClickHouse
)

-- Keyless
CREATE TABLE "PageViews" (
    "Timestamp" DateTime64(3) NOT NULL,
    ...
    -- No primary key
)
ENGINE = MergeTree
ORDER BY ("Timestamp", "PageUrl")
```

**Note**: ClickHouse doesn't enforce primary key constraints anyway - they're metadata only.

## Best Practices

### Default to Keyless

For analytics workloads, keyless is usually correct:

```csharp
options.UseClickHouse(connectionString,
    o => o.UseKeylessEntitiesByDefault());
```

### Add Keys Only When Needed

Only add keys for entities that:
1. Need Find() operations
2. Use ReplacingMergeTree for updates
3. Require change tracking

### Use ORDER BY for Identity

The ORDER BY columns serve as the logical identity:

```csharp
entity.UseMergeTree(x => new { x.Timestamp, x.EventId });
// These columns identify the row for queries
```

## See Also

- [ReplacingMergeTree](../engines/replacing-mergetree.md) - For update semantics with keys
- [Delete Operations](delete-operations.md) - Bulk delete without tracking
- [Limitations](../limitations.md) - What doesn't work in ClickHouse
