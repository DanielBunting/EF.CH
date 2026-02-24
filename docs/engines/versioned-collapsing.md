# VersionedCollapsingMergeTree Engine

VersionedCollapsingMergeTree extends CollapsingMergeTree to handle out-of-order inserts. It uses both a sign column and a version column, allowing correct collapsing even when cancel and state rows arrive in different batches or in the wrong order.

## Basic Configuration

```csharp
modelBuilder.Entity<UserSession>(entity =>
{
    entity.UseVersionedCollapsingMergeTree(
        x => x.Sign,                        // sign column (sbyte)
        x => x.Version,                     // version column
        x => new { x.UserId });             // ORDER BY key
});
```

```sql
CREATE TABLE "UserSessions" (
    "UserId" UInt64,
    "PageViews" UInt32,
    "Sign" Int8,
    "Version" UInt32
)
ENGINE = VersionedCollapsingMergeTree("Sign", "Version")
ORDER BY ("UserId")
```

## How It Improves on CollapsingMergeTree

With CollapsingMergeTree, the cancel row (`-1`) must be inserted **before** the new state row (`+1`) within the same batch. VersionedCollapsingMergeTree removes this restriction by using the version number to match cancel and state rows during merges:

- Rows with the same ORDER BY key and the same `Version` but opposite `Sign` values collapse during merges
- Insert order does not matter -- the version column handles matching

```csharp
// These can arrive in any order, even across separate inserts:
await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 10, Sign = 1, Version = 2 }   // new state
});

await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 5, Sign = -1, Version = 1 }   // cancel old
});

// After merge: only the Version=2 row survives
```

## Version Column

The version column must be an incrementing value. Supported types include `uint` (UInt32), `ulong` (UInt64), `byte` (UInt8), and `ushort` (UInt16):

```csharp
public class UserSession
{
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public sbyte Sign { get; set; }     // Int8: +1 or -1
    public uint Version { get; set; }   // UInt32: incrementing version
}
```

## String Overload

```csharp
entity.UseVersionedCollapsingMergeTree("Sign", "Version", "UserId");
```

```sql
ENGINE = VersionedCollapsingMergeTree("Sign", "Version")
ORDER BY ("UserId")
```

## Multiple ORDER BY Columns

```csharp
entity.UseVersionedCollapsingMergeTree(
    x => x.Sign,
    x => x.Version,
    x => new { x.UserId, x.EventTime });
```

```sql
ENGINE = VersionedCollapsingMergeTree("Sign", "Version")
ORDER BY ("UserId", "EventTime")
```

## State Update Pattern

```csharp
// Version 1: initial state
await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 5, Sign = 1, Version = 1 }
});

// Version 2: cancel v1, insert v2 (order does not matter)
await context.BulkInsertAsync(new[]
{
    new UserSession { UserId = 1, PageViews = 5, Sign = -1, Version = 1 },  // cancel v1
    new UserSession { UserId = 1, PageViews = 10, Sign = 1, Version = 2 }   // new state v2
});
```

After `OPTIMIZE TABLE ... FINAL`, the result is a single row with `PageViews = 10, Version = 2, Sign = 1`.

## Querying

As with CollapsingMergeTree, use aggregation with the sign column for correct results before merges complete:

```csharp
var sessions = await context.UserSessions
    .GroupBy(s => s.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        PageViews = g.Sum(s => s.PageViews * s.Sign)
    })
    .ToListAsync();
```

## Complete Example

```csharp
public class AccountBalance
{
    public long AccountId { get; set; }
    public decimal Balance { get; set; }
    public string Currency { get; set; } = string.Empty;
    public sbyte Sign { get; set; }
    public uint Version { get; set; }
}

modelBuilder.Entity<AccountBalance>(entity =>
{
    entity.ToTable("account_balances");
    entity.HasNoKey();

    entity.UseVersionedCollapsingMergeTree(
        x => x.Sign,
        x => x.Version,
        x => new { x.AccountId })
        .HasPartitionByMonth(x => x.Version);
});
```

```sql
CREATE TABLE "account_balances" (
    "AccountId" Int64,
    "Balance" Decimal(18, 4),
    "Currency" String,
    "Sign" Int8,
    "Version" UInt32
)
ENGINE = VersionedCollapsingMergeTree("Sign", "Version")
ORDER BY ("AccountId")
```

> **Note:** Choose VersionedCollapsingMergeTree over CollapsingMergeTree when data arrives from multiple sources or insert order cannot be guaranteed. The version column adds minimal storage overhead while providing correctness guarantees.

## See Also

- [CollapsingMergeTree](collapsing-mergetree.md) -- simpler variant requiring ordered inserts
- [ReplacingMergeTree](replacing-mergetree.md) -- deduplication without sign columns
- [MergeTree](mergetree.md) -- base engine
