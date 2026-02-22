# CollapsingMergeTree Engine

CollapsingMergeTree tracks state changes through a sign column. Rows with `Sign = 1` represent the current state, and rows with `Sign = -1` cancel a previous state. During background merges, pairs of rows with opposite signs and the same ORDER BY key are removed, leaving only the latest state.

## Basic Configuration

```csharp
modelBuilder.Entity<UserSession>(entity =>
{
    entity.UseCollapsingMergeTree(
        x => x.Sign,                        // sign column (sbyte)
        x => new { x.UserId });             // ORDER BY key
});
```

```sql
CREATE TABLE "UserSessions" (
    "UserId" UInt64,
    "PageViews" UInt32,
    "Duration" UInt32,
    "Sign" Int8
)
ENGINE = CollapsingMergeTree("Sign")
ORDER BY ("UserId")
```

## Sign Column

The sign column must be `sbyte` (mapped to ClickHouse `Int8`) with exactly two possible values:

- `+1` -- state row (the current values for this key)
- `-1` -- cancel row (marks a previous state as obsolete)

```csharp
public class UserSession
{
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public int Duration { get; set; }
    public sbyte Sign { get; set; }
}
```

## State Update Pattern

To update state, insert the old row with `Sign = -1` and the new row with `Sign = +1`:

```csharp
// Initial state
await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 5, Duration = 100, Sign = 1 }
});

// Update: cancel old state, insert new state
await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 5, Duration = 100, Sign = -1 },  // cancel
    new UserSession { UserId = 1, PageViews = 10, Duration = 200, Sign = 1 }   // new state
});
```

After a background merge (or `OPTIMIZE TABLE ... FINAL`), the `+1/-1` pair collapses, leaving only the new state row:

```sql
-- Before merge: 3 rows
-- After merge:  1 row (PageViews = 10, Duration = 200, Sign = 1)
```

## String Overload

```csharp
entity.UseCollapsingMergeTree("Sign", "UserId");
```

```sql
ENGINE = CollapsingMergeTree("Sign")
ORDER BY ("UserId")
```

## Multiple ORDER BY Columns

```csharp
entity.UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId, x.EventTime });
```

```sql
ENGINE = CollapsingMergeTree("Sign")
ORDER BY ("UserId", "EventTime")
```

## Insert Order Requirements

CollapsingMergeTree requires that within a single insert batch, the cancel row (`-1`) must come **before** the new state row (`+1`) for the same key. If rows arrive in the wrong order across different inserts, collapsing may not work correctly until a merge runs.

```csharp
// Correct: cancel first, then new state
await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 5, Sign = -1 },   // cancel old
    new UserSession { UserId = 1, PageViews = 10, Sign = 1 }    // new state
});
```

> **Note:** If you cannot guarantee insert order, use [VersionedCollapsingMergeTree](versioned-collapsing.md) instead. It handles out-of-order inserts correctly by using a version column.

## Querying

Before merges complete, both state and cancel rows may be visible. Use aggregation to get correct results:

```csharp
var sessions = await context.UserSessions
    .GroupBy(s => s.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        PageViews = g.Sum(s => s.PageViews * s.Sign),
        Duration = g.Sum(s => s.Duration * s.Sign)
    })
    .ToListAsync();
```

```sql
SELECT "UserId",
       sumOrNull("PageViews" * "Sign"),
       sumOrNull("Duration" * "Sign")
FROM "UserSessions"
GROUP BY "UserId"
```

## Complete Example

```csharp
public class SessionState
{
    public long UserId { get; set; }
    public DateTime SessionStart { get; set; }
    public int PageViews { get; set; }
    public int Duration { get; set; }
    public sbyte Sign { get; set; }
}

modelBuilder.Entity<SessionState>(entity =>
{
    entity.ToTable("session_states");
    entity.HasNoKey();

    entity.UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId })
        .HasPartitionByMonth(x => x.SessionStart);
});
```

```sql
CREATE TABLE "session_states" (
    "UserId" UInt64,
    "SessionStart" DateTime64(3),
    "PageViews" Int32,
    "Duration" Int32,
    "Sign" Int8
)
ENGINE = CollapsingMergeTree("Sign")
PARTITION BY toYYYYMM("SessionStart")
ORDER BY ("UserId")
```

## See Also

- [VersionedCollapsingMergeTree](versioned-collapsing.md) -- handles out-of-order inserts
- [ReplacingMergeTree](replacing-mergetree.md) -- simpler deduplication without sign columns
- [MergeTree](mergetree.md) -- base engine
