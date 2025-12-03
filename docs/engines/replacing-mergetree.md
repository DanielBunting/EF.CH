# ReplacingMergeTree Engine

ReplacingMergeTree automatically deduplicates rows with the same ORDER BY key, keeping only the row with the highest version. This is the closest thing ClickHouse has to "UPDATE" semantics.

## When to Use

- User profiles that need occasional updates
- Product catalogs with price changes
- Configuration or settings tables
- Any data where you need "last write wins" behavior

## How It Works

1. You insert a new row with the same key as an existing row
2. Both rows exist until a merge happens
3. During merge, ClickHouse keeps only the row with the highest version
4. Use `FINAL` to see deduplicated results immediately

```
Insert: User(id=1, name="Alice", version=1)
Insert: User(id=1, name="Alicia", version=2)  -- "update"

Before merge: Both rows exist
After merge:  Only version=2 remains
```

## Configuration

### Basic Setup

```csharp
public class User
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }  // Version column
}

public class MyDbContext : DbContext
{
    public DbSet<User> Users => Set<User>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree(
                versionColumn: x => x.UpdatedAt,  // Higher = newer
                orderByColumn: x => x.Id);        // Deduplication key
        });
    }
}
```

### With Composite Key

```csharp
// Deduplicate by TenantId + UserId combination
entity.UseReplacingMergeTree(
    versionColumn: x => x.UpdatedAt,
    orderByColumn: x => new { x.TenantId, x.UserId });
```

### Without Version Column

If you don't specify a version, ClickHouse keeps an arbitrary row:

```csharp
// Not recommended - unpredictable which row survives
entity.UseReplacingMergeTree(orderByColumn: x => x.Id);
```

## Generated DDL

```csharp
entity.UseReplacingMergeTree(x => x.UpdatedAt, x => x.Id);
entity.HasPartitionByMonth(x => x.CreatedAt);
```

Generates:

```sql
CREATE TABLE "Users" (
    "Id" UUID NOT NULL,
    "Email" String NOT NULL,
    "Name" String NOT NULL,
    "CreatedAt" DateTime64(3) NOT NULL,
    "UpdatedAt" DateTime64(3) NOT NULL
)
ENGINE = ReplacingMergeTree("UpdatedAt")
PARTITION BY toYYYYMM("CreatedAt")
ORDER BY ("Id")
```

## Usage Examples

### "Updating" a User

```csharp
// Get current user
var user = await context.Users
    .Final()  // Important: see deduplicated data
    .FirstAsync(u => u.Id == userId);

// "Update" by inserting new version
var updatedUser = new User
{
    Id = user.Id,           // Same key
    Email = user.Email,
    Name = "New Name",      // Changed field
    UpdatedAt = DateTime.UtcNow  // Higher version
};

context.Users.Add(updatedUser);
await context.SaveChangesAsync();
```

### Querying with FINAL

```csharp
// May return duplicate rows (before merge)
var users = await context.Users.ToListAsync();

// Guaranteed deduplicated (use for accurate results)
var users = await context.Users.Final().ToListAsync();

// Filter then deduplicate
var activeUsers = await context.Users
    .Where(u => u.IsActive)
    .Final()
    .ToListAsync();
```

### Bulk "Update"

```csharp
// Update multiple users at once
var updates = existingUsers.Select(u => new User
{
    Id = u.Id,
    Email = u.Email,
    Name = u.Name.ToUpper(),  // Transform
    UpdatedAt = DateTime.UtcNow
});

context.Users.AddRange(updates);
await context.SaveChangesAsync();
```

## Version Column Best Practices

### Use DateTime for Time-Based Versioning

```csharp
public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
```

- Natural ordering (newer = higher)
- Easy to understand and debug
- Works well with partitioning

### Use UInt64 for Explicit Versioning

```csharp
public ulong Version { get; set; }

// When updating:
newEntity.Version = oldEntity.Version + 1;
```

- Explicit control over version order
- No clock skew issues
- Requires tracking current version

### Avoid Using Nullable Versions

```csharp
// Don't do this - NULL versions cause unpredictable behavior
public DateTime? UpdatedAt { get; set; }
```

## FINAL Performance Considerations

The `FINAL` modifier adds overhead:

```csharp
// Fast, but may have duplicates
var count = await context.Users.CountAsync();

// Slower, but accurate
var count = await context.Users.Final().CountAsync();
```

**When to use FINAL:**
- Real-time dashboards showing current state
- Single-entity lookups
- When accuracy matters more than speed

**When to skip FINAL:**
- Historical aggregations (duplicates often don't affect results)
- Batch processing where you'll handle duplicates yourself
- When data is stable (no recent inserts)

## Forcing a Merge

Merges happen automatically, but you can force one:

```csharp
// Force immediate deduplication (expensive operation)
await context.Database.ExecuteSqlRawAsync(
    @"OPTIMIZE TABLE ""Users"" FINAL");
```

**Caution:** Don't run `OPTIMIZE ... FINAL` frequently. It rewrites data and is resource-intensive.

## Common Patterns

### Upsert Pattern

```csharp
public async Task UpsertUser(User user)
{
    user.UpdatedAt = DateTime.UtcNow;
    context.Users.Add(user);  // Always insert
    await context.SaveChangesAsync();
}
```

### Soft Delete

```csharp
public class User
{
    public Guid Id { get; set; }
    public bool IsDeleted { get; set; }
    public DateTime UpdatedAt { get; set; }
}

public async Task SoftDeleteUser(Guid userId)
{
    var user = await context.Users.Final().FirstAsync(u => u.Id == userId);

    context.Users.Add(new User
    {
        Id = user.Id,
        IsDeleted = true,
        UpdatedAt = DateTime.UtcNow
    });
    await context.SaveChangesAsync();
}
```

### Change History (Keep Both)

If you need history, use MergeTree instead and add a version column:

```csharp
// MergeTree keeps all versions
entity.UseMergeTree(x => new { x.UserId, x.Version });

// Query latest
var latest = await context.UserHistory
    .Where(h => h.UserId == userId)
    .OrderByDescending(h => h.Version)
    .FirstAsync();
```

## Limitations

- **Not Immediate**: Deduplication happens during merges, not on insert
- **FINAL Overhead**: Accurate queries are slower
- **No Partial Updates**: Must insert complete row
- **Version Required**: Without version, behavior is undefined

## When Not to Use

| Scenario | Use Instead |
|----------|-------------|
| Append-only data | [MergeTree](mergetree.md) |
| Auto-sum numeric columns | [SummingMergeTree](summing-mergetree.md) |
| Track state changes with signs | [CollapsingMergeTree](collapsing-mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [Query Modifiers - FINAL](../features/query-modifiers.md)
- [ClickHouse ReplacingMergeTree Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
