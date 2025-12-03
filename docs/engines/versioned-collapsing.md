# VersionedCollapsingMergeTree Engine

VersionedCollapsingMergeTree extends CollapsingMergeTree with a version column, allowing rows to be inserted in any order. The version ensures correct collapsing even when data arrives out of sequence.

## When to Use

- Distributed systems where events arrive out of order
- Event sourcing with potential reordering
- State tracking with unreliable message ordering
- Any CollapsingMergeTree use case where order isn't guaranteed

## How It Works

Like CollapsingMergeTree, but with a version column:

1. Insert row with Sign=+1, Version=N to add state
2. Insert row with Sign=-1, Version=N to cancel that specific version
3. Insert row with Sign=+1, Version=N+1 for new state
4. During merges, matching Sign/Version pairs collapse regardless of insert order

```
Insert order:
1. Session(userId=1, pageViews=10, sign=+1, version=2)  -- New state first
2. Session(userId=1, pageViews=5, sign=-1, version=1)   -- Cancel old
3. Session(userId=1, pageViews=5, sign=+1, version=1)   -- Old state last

After merge: Only version=2 row remains (despite insert order)
```

## Configuration

### Basic Setup

```csharp
public class UserSession
{
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public int DurationSeconds { get; set; }
    public sbyte Sign { get; set; }    // +1 or -1
    public uint Version { get; set; }  // Monotonically increasing
}

public class MyDbContext : DbContext
{
    public DbSet<UserSession> UserSessions => Set<UserSession>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserSession>(entity =>
        {
            entity.HasNoKey();
            entity.UseVersionedCollapsingMergeTree(
                signColumn: x => x.Sign,
                versionColumn: x => x.Version,
                orderByColumn: x => x.UserId);
        });
    }
}
```

### With Composite Key

```csharp
entity.UseVersionedCollapsingMergeTree(
    signColumn: x => x.Sign,
    versionColumn: x => x.Version,
    orderByColumn: x => new { x.TenantId, x.UserId });
```

## Generated DDL

```csharp
entity.UseVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => x.UserId);
```

Generates:

```sql
CREATE TABLE "UserSessions" (
    "UserId" Int64 NOT NULL,
    "PageViews" Int32 NOT NULL,
    "DurationSeconds" Int32 NOT NULL,
    "Sign" Int8 NOT NULL,
    "Version" UInt32 NOT NULL
)
ENGINE = VersionedCollapsingMergeTree("Sign", "Version")
ORDER BY ("UserId")
```

## Usage Examples

### Adding State

```csharp
context.UserSessions.Add(new UserSession
{
    UserId = 123,
    PageViews = 5,
    DurationSeconds = 120,
    Sign = 1,
    Version = 1
});
await context.SaveChangesAsync();
```

### Updating State

```csharp
// Cancel old version and add new - can be in any order
context.UserSessions.AddRange(new[]
{
    // Cancel version 1
    new UserSession
    {
        UserId = 123,
        PageViews = 5,
        DurationSeconds = 120,
        Sign = -1,
        Version = 1  // Must match original version
    },
    // Add version 2
    new UserSession
    {
        UserId = 123,
        PageViews = 10,
        DurationSeconds = 240,
        Sign = 1,
        Version = 2  // New version
    }
});

await context.SaveChangesAsync();
```

### Out-of-Order Safe Updates

The key advantage: insert order doesn't matter:

```csharp
// These can arrive in any order and still collapse correctly
var updates = new[]
{
    new UserSession { UserId = 123, PageViews = 15, Sign = 1, Version = 3 },
    new UserSession { UserId = 123, PageViews = 10, Sign = -1, Version = 2 },
    new UserSession { UserId = 123, PageViews = 10, Sign = 1, Version = 2 },
    new UserSession { UserId = 123, PageViews = 5, Sign = -1, Version = 1 },
};

// Shuffle the order - still works!
context.UserSessions.AddRange(updates.OrderBy(_ => Random.Shared.Next()));
await context.SaveChangesAsync();

// After merge: Only version=3 remains
```

### Querying Current State

```csharp
// Sum with sign to get current state
var currentSessions = await context.UserSessions
    .GroupBy(s => s.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        PageViews = g.Sum(s => s.PageViews * s.Sign),
        DurationSeconds = g.Sum(s => s.DurationSeconds * s.Sign),
        LatestVersion = g.Max(s => s.Version),
        IsActive = g.Sum(s => s.Sign) > 0
    })
    .Where(s => s.IsActive)
    .ToListAsync();
```

## Version Management

### Version Column Requirements

- Must be an unsigned integer type (`uint`, `ulong`)
- Must be monotonically increasing per ORDER BY key
- Cancel row must use the **same version** as the row being cancelled

### Version Generation Strategies

**Timestamp-based:**
```csharp
public uint Version { get; set; } = (uint)(DateTime.UtcNow.Ticks / 10000);
```

**Sequence-based:**
```csharp
public class SessionManager
{
    private readonly ConcurrentDictionary<long, uint> _versions = new();

    public uint GetNextVersion(long userId)
    {
        return _versions.AddOrUpdate(userId, 1, (_, v) => v + 1);
    }
}
```

**Database-tracked:**
```csharp
// Store current version in a separate table or cache
var currentVersion = await GetCurrentVersion(userId);
newSession.Version = currentVersion + 1;
```

## Collapsing Behavior

### How Versions Pair Up

```
Rows inserted (any order):
| UserId | PageViews | Sign | Version |
|--------|-----------|------|---------|
| 1      | 5         | +1   | 1       |  ← Pairs with row below
| 1      | 5         | -1   | 1       |  ← Pairs with row above
| 1      | 10        | +1   | 2       |  ← Survives

After merge:
| UserId | PageViews | Sign | Version |
|--------|-----------|------|---------|
| 1      | 10        | +1   | 2       |  ← Only this remains
```

### Partial Collapse

If only one side of a pair exists, it survives:

```
Rows:
| UserId | PageViews | Sign | Version |
|--------|-----------|------|---------|
| 1      | 5         | +1   | 1       |  ← No matching -1, survives
| 1      | 10        | +1   | 2       |  ← Survives

After merge: Both rows remain (no -1 version=1 to cancel first row)
```

## Comparison with CollapsingMergeTree

| Feature | CollapsingMergeTree | VersionedCollapsingMergeTree |
|---------|---------------------|------------------------------|
| Sign column | Yes | Yes |
| Version column | No | Yes |
| Insert order matters | Yes | No |
| Out-of-order safe | No | Yes |
| Storage overhead | Lower | Slightly higher (version column) |
| Use case | Ordered inserts | Distributed/async systems |

## Best Practices

### Use for Distributed Systems

```csharp
// Multiple services can send updates without coordination
// Service A sends version 2
// Service B sends version 1 (delayed)
// Both collapse correctly regardless of arrival order
```

### Track Versions Reliably

```csharp
public class VersionedSessionService
{
    public async Task UpdateSession(long userId, SessionData newData)
    {
        // Get current state to know the version
        var current = await GetCurrentSession(userId);
        var newVersion = (current?.Version ?? 0) + 1;

        var operations = new List<UserSession>();

        if (current != null)
        {
            // Cancel with exact version
            operations.Add(new UserSession
            {
                UserId = userId,
                PageViews = current.PageViews,
                DurationSeconds = current.DurationSeconds,
                Sign = -1,
                Version = current.Version  // Same version
            });
        }

        // Add new version
        operations.Add(new UserSession
        {
            UserId = userId,
            PageViews = newData.PageViews,
            DurationSeconds = newData.DurationSeconds,
            Sign = 1,
            Version = newVersion
        });

        await _context.UserSessions.AddRangeAsync(operations);
        await _context.SaveChangesAsync();
    }
}
```

### Match Cancel Versions Exactly

```csharp
// ✓ Correct: Cancel uses same version
{ UserId=1, PageViews=5, Sign=+1, Version=1 }
{ UserId=1, PageViews=5, Sign=-1, Version=1 }  // Will collapse

// ✗ Wrong: Version mismatch
{ UserId=1, PageViews=5, Sign=+1, Version=1 }
{ UserId=1, PageViews=5, Sign=-1, Version=2 }  // Won't collapse!
```

## Limitations

- **Version Tracking**: You must manage versions yourself
- **Cancel Must Match**: Both values AND version must match
- **Storage Overhead**: Additional column per row
- **Query Complexity**: Same as CollapsingMergeTree

## When Not to Use

| Scenario | Use Instead |
|----------|-------------|
| Guaranteed ordered inserts | [CollapsingMergeTree](collapsing-mergetree.md) |
| Simple deduplication | [ReplacingMergeTree](replacing-mergetree.md) |
| Append-only data | [MergeTree](mergetree.md) |
| Auto-summing | [SummingMergeTree](summing-mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [CollapsingMergeTree](collapsing-mergetree.md) - Simpler variant for ordered data
- [ClickHouse VersionedCollapsingMergeTree Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/versionedcollapsingmergetree)
