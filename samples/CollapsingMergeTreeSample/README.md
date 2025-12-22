# CollapsingMergeTree Sample

Demonstrates state tracking using CollapsingMergeTree engine with +1/-1 sign columns.

## What This Shows

- Configuring CollapsingMergeTree with sign column
- Adding state with Sign=+1
- Cancelling state with Sign=-1
- Updating by cancelling old + adding new
- Querying current state using `SUM(value * Sign)`

## The Pattern

CollapsingMergeTree uses a "sign" column to track state changes:

| Action | Pattern |
|--------|---------|
| Add | Insert with Sign=+1 |
| Cancel/Delete | Insert matching row with Sign=-1 |
| Update | Cancel old (-1) + Add new (+1) |

During merges, +1 and -1 pairs cancel out.

## How It Works

```csharp
entity.UseCollapsingMergeTree(
    signColumn: x => x.Sign,
    orderByColumn: x => x.UserId);
```

**Important:** The cancel row (-1) must have identical values to the row being cancelled.

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
CollapsingMergeTree Sample
==========================

Creating database and tables...
--- Tracking User Sessions ---

User 1 starts session (pageViews=1, duration=10)
User 2 starts session (pageViews=1, duration=5)

After initial sessions:
  User 1: pageViews=1, duration=10s [ACTIVE]
  User 2: pageViews=1, duration=5s [ACTIVE]
  --- Active sessions: 2, Total pageViews: 2

--- User 1 session update ---
Cancel old: (pageViews=1, duration=10, sign=-1)
Add new:    (pageViews=5, duration=60, sign=+1)

After User 1 update:
  User 1: pageViews=5, duration=60s [ACTIVE]
  User 2: pageViews=1, duration=5s [ACTIVE]
  --- Active sessions: 2, Total pageViews: 6

--- User 2 session ends ---
Cancel: (pageViews=1, duration=5, sign=-1)

After User 2 ends:
  User 1: pageViews=5, duration=60s [ACTIVE]
  User 2: pageViews=0, duration=0s [ended]
  --- Active sessions: 1, Total pageViews: 5

--- Physical Storage ---
Physical rows before OPTIMIZE: 7
Physical rows after OPTIMIZE:  1

--- Remaining Physical Rows ---
  UserId=1, PageViews=10, Duration=120s, Sign=1

Done!
```

## Key Code

### Entity with Sign Column

```csharp
public class UserSession
{
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public int DurationSeconds { get; set; }
    public sbyte Sign { get; set; }  // +1 or -1
}
```

### Configuration

```csharp
entity.UseCollapsingMergeTree(
    signColumn: x => x.Sign,
    orderByColumn: x => x.UserId);
```

### Adding State

```csharp
context.Sessions.Add(new UserSession
{
    UserId = 1,
    PageViews = 1,
    DurationSeconds = 10,
    Sign = 1  // +1 = add
});
```

### Updating State

```csharp
// Must cancel with exact matching values
context.Sessions.Add(new UserSession
{
    UserId = 1,
    PageViews = 1,         // Old value
    DurationSeconds = 10,  // Old value
    Sign = -1              // Cancel
});

// Then add new state
context.Sessions.Add(new UserSession
{
    UserId = 1,
    PageViews = 5,         // New value
    DurationSeconds = 60,  // New value
    Sign = 1               // Add
});
```

### Querying Current State

```csharp
var sessions = await context.Sessions
    .GroupBy(s => s.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        PageViews = g.Sum(s => s.PageViews * s.Sign),
        Duration = g.Sum(s => s.DurationSeconds * s.Sign),
        IsActive = g.Sum(s => s.Sign) > 0
    })
    .ToListAsync();
```

## When to Use

- Real-time state tracking (active sessions, live counters)
- Event sourcing with state snapshots
- When you need to "undo" previous values

## When NOT to Use

- Out-of-order data arrival (use VersionedCollapsingMergeTree)
- Simple deduplication (use ReplacingMergeTree)
- Only summing (use SummingMergeTree)

## Learn More

- [CollapsingMergeTree Documentation](../../docs/engines/collapsing-mergetree.md)
- [VersionedCollapsingMergeTree](../../docs/engines/versioned-collapsing.md) - For out-of-order data
