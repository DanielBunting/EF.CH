# CollapsingMergeTree Engine

CollapsingMergeTree tracks state changes using a "sign" column (+1/-1). Rows with opposite signs cancel each other during merges. This enables efficient state updates and deletions.

## When to Use

- User session tracking (active sessions)
- Real-time state management
- Event sourcing with state snapshots
- When you need to "undo" or "cancel" previous rows

## How It Works

1. Insert a row with Sign=+1 to add state
2. Insert the same row with Sign=-1 to cancel it
3. Insert a new row with Sign=+1 for updated state
4. During merges, +1 and -1 pairs cancel out

```
Insert: Session(userId=1, pageViews=5, sign=+1)   -- Add state
Insert: Session(userId=1, pageViews=5, sign=-1)  -- Cancel old
Insert: Session(userId=1, pageViews=10, sign=+1) -- New state

After merge: Only pageViews=10 row remains
```

## Configuration

### Basic Setup

```csharp
public class UserSession
{
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public int DurationSeconds { get; set; }
    public sbyte Sign { get; set; }  // +1 or -1
}

public class MyDbContext : DbContext
{
    public DbSet<UserSession> UserSessions => Set<UserSession>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserSession>(entity =>
        {
            entity.HasNoKey();
            entity.UseCollapsingMergeTree(
                signColumn: x => x.Sign,
                orderByColumn: x => x.UserId);
        });
    }
}
```

### With Composite Key

```csharp
entity.UseCollapsingMergeTree(
    signColumn: x => x.Sign,
    orderByColumn: x => new { x.TenantId, x.UserId });
```

## Generated DDL

```csharp
entity.UseCollapsingMergeTree(x => x.Sign, x => x.UserId);
entity.HasPartitionByMonth(x => x.CreatedAt);
```

Generates:

```sql
CREATE TABLE "UserSessions" (
    "UserId" Int64 NOT NULL,
    "PageViews" Int32 NOT NULL,
    "DurationSeconds" Int32 NOT NULL,
    "Sign" Int8 NOT NULL,
    "CreatedAt" DateTime64(3) NOT NULL
)
ENGINE = CollapsingMergeTree("Sign")
PARTITION BY toYYYYMM("CreatedAt")
ORDER BY ("UserId")
```

## Usage Examples

### Adding State

```csharp
// Add a new session state
context.UserSessions.Add(new UserSession
{
    UserId = 123,
    PageViews = 5,
    DurationSeconds = 120,
    Sign = 1  // +1 = add
});
await context.SaveChangesAsync();
```

### Updating State

```csharp
// To "update", cancel old state and add new
// Step 1: Cancel the old state (must match exactly)
context.UserSessions.Add(new UserSession
{
    UserId = 123,
    PageViews = 5,           // Old values
    DurationSeconds = 120,   // Must match!
    Sign = -1                // Cancel
});

// Step 2: Add the new state
context.UserSessions.Add(new UserSession
{
    UserId = 123,
    PageViews = 10,          // New values
    DurationSeconds = 240,
    Sign = 1                 // Add
});

await context.SaveChangesAsync();
```

### Deleting State

```csharp
// Cancel without adding new state
context.UserSessions.Add(new UserSession
{
    UserId = 123,
    PageViews = 10,
    DurationSeconds = 240,
    Sign = -1  // Cancel, no replacement
});
await context.SaveChangesAsync();
```

### Querying Current State

```csharp
// Before merge, sum the signs to get current state
var currentSessions = await context.UserSessions
    .GroupBy(s => s.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        PageViews = g.Sum(s => s.PageViews * s.Sign),
        DurationSeconds = g.Sum(s => s.DurationSeconds * s.Sign),
        IsActive = g.Sum(s => s.Sign) > 0
    })
    .Where(s => s.IsActive)
    .ToListAsync();
```

### Forcing Collapse

```csharp
// Force immediate collapsing (expensive)
await context.Database.ExecuteSqlRawAsync(
    @"OPTIMIZE TABLE ""UserSessions"" FINAL");

// After OPTIMIZE, cancelled rows are physically removed
```

## The Collapsing Pattern

### Correct Update Sequence

```csharp
public async Task UpdateSession(long userId, int newPageViews, int newDuration)
{
    // 1. Get current state
    var current = await GetCurrentSession(userId);

    if (current != null)
    {
        // 2. Cancel old state (MUST match exactly)
        context.UserSessions.Add(new UserSession
        {
            UserId = userId,
            PageViews = current.PageViews,
            DurationSeconds = current.DurationSeconds,
            Sign = -1
        });
    }

    // 3. Add new state
    context.UserSessions.Add(new UserSession
    {
        UserId = userId,
        PageViews = newPageViews,
        DurationSeconds = newDuration,
        Sign = 1
    });

    await context.SaveChangesAsync();
}

private async Task<UserSession?> GetCurrentSession(long userId)
{
    // Query with sign aggregation to get current state
    var result = await context.UserSessions
        .Where(s => s.UserId == userId)
        .GroupBy(s => s.UserId)
        .Select(g => new UserSession
        {
            UserId = g.Key,
            PageViews = g.Sum(s => s.PageViews * s.Sign),
            DurationSeconds = g.Sum(s => s.DurationSeconds * s.Sign),
            Sign = (sbyte)g.Sum(s => s.Sign)
        })
        .FirstOrDefaultAsync();

    return result?.Sign > 0 ? result : null;
}
```

### Important: Cancel Row Must Match

The cancel row (-1) must have **identical values** to the original row (+1):

```csharp
// Original row
{ UserId=1, PageViews=5, Sign=+1 }

// Correct cancel (matches)
{ UserId=1, PageViews=5, Sign=-1 }  // ✓ Will collapse

// Wrong cancel (doesn't match)
{ UserId=1, PageViews=10, Sign=-1 } // ✗ Won't collapse!
```

## Querying Strategies

### Sum with Sign (Most Common)

```csharp
// Multiply values by sign, then sum
var totals = await context.UserSessions
    .GroupBy(s => 1)  // All rows
    .Select(g => new
    {
        TotalPageViews = g.Sum(s => s.PageViews * s.Sign),
        TotalDuration = g.Sum(s => s.DurationSeconds * s.Sign),
        ActiveSessions = g.Sum(s => s.Sign)
    })
    .FirstAsync();
```

### Using FINAL

```csharp
// After OPTIMIZE or for small tables
var sessions = await context.UserSessions
    .Final()
    .Where(s => s.Sign == 1)  // Only positive rows remain
    .ToListAsync();
```

### Filter Cancelled Records

```csharp
// Only get non-cancelled records (pre-merge)
var activeSessions = await context.UserSessions
    .GroupBy(s => s.UserId)
    .Where(g => g.Sum(s => s.Sign) > 0)
    .Select(g => new
    {
        UserId = g.Key,
        // ... aggregated values
    })
    .ToListAsync();
```

## Best Practices

### Store Original Values

Keep track of original values to cancel correctly:

```csharp
public class SessionTracker
{
    private readonly Dictionary<long, UserSession> _currentStates = new();

    public void Update(long userId, int pageViews, int duration)
    {
        if (_currentStates.TryGetValue(userId, out var old))
        {
            // Cancel with exact old values
            _cancelRows.Add(new UserSession { ...old, Sign = -1 });
        }

        var newState = new UserSession
        {
            UserId = userId,
            PageViews = pageViews,
            DurationSeconds = duration,
            Sign = 1
        };

        _currentStates[userId] = newState;
        _addRows.Add(newState);
    }
}
```

### Use ORDER BY for Efficient Queries

```csharp
// ORDER BY should match your query patterns
entity.UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId, x.EventTime });

// This query is efficient:
context.UserSessions.Where(s => s.UserId == 123)
```

### Consider VersionedCollapsingMergeTree

If inserts can arrive out of order, use [VersionedCollapsingMergeTree](versioned-collapsing.md) instead.

## Limitations

- **Must Cancel Exactly**: Cancel row must match original exactly
- **No Partial Updates**: Can't update individual fields
- **Order Matters**: Out-of-order inserts may not collapse correctly (use Versioned variant)
- **Query Complexity**: Queries must account for sign column

## When Not to Use

| Scenario | Use Instead |
|----------|-------------|
| Out-of-order inserts | [VersionedCollapsingMergeTree](versioned-collapsing.md) |
| Simple deduplication | [ReplacingMergeTree](replacing-mergetree.md) |
| Only summing values | [SummingMergeTree](summing-mergetree.md) |
| Append-only data | [MergeTree](mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [VersionedCollapsingMergeTree](versioned-collapsing.md) - For out-of-order data
- [ClickHouse CollapsingMergeTree Docs](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/collapsingmergetree)
