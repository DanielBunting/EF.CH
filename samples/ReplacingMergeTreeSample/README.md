# ReplacingMergeTree Sample

Demonstrates "update" semantics using ReplacingMergeTree engine.

## What This Shows

- Configuring ReplacingMergeTree with version column
- "Updating" data by inserting new versions
- Using `.Final()` to see deduplicated results
- Difference between pre-merge and post-merge queries
- Using `OPTIMIZE TABLE FINAL` to force deduplication

## The Problem

ClickHouse doesn't support `UPDATE` statements. Attempting to update an entity throws `NotSupportedException`.

## The Solution

ReplacingMergeTree keeps only the row with the highest version value for each key:

```csharp
entity.UseReplacingMergeTree(
    versionColumn: x => x.UpdatedAt,  // Higher = newer
    orderByColumn: x => x.Id);        // Deduplication key
```

## How It Works

1. Insert initial row with `UpdatedAt = DateTime.UtcNow`
2. To "update", insert new row with same `Id` but later `UpdatedAt`
3. ClickHouse eventually merges, keeping only the newest version
4. Use `.Final()` for immediate deduplicated results

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
ReplacingMergeTree Sample
=========================

Creating database and tables...
Creating user with ID: a1b2c3d4-...

After initial insert:
  Name: Alice, Bio: Original bio

"Updating" user by inserting new version...

After update (with FINAL for accurate results):
  Name: Alice Smith
  Bio: Updated bio - I love ClickHouse!
  Updated: 2024-01-15 14:30:15

Performing multiple updates...
  Inserted version 2
  Inserted version 3
  Inserted version 4

Query WITHOUT FINAL (may see unmerged rows):
  Found 5 row(s)

Query WITH FINAL (deduplicated):
  Name: Alice Smith (v4)
  Bio: Bio version 4

Forcing OPTIMIZE FINAL (physically deduplicates)...

After OPTIMIZE:
  Found 1 row(s)
  Name: Alice Smith (v4)

Done!
```

## Key Code

### Entity with Version Column

```csharp
public class User
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public DateTime UpdatedAt { get; set; }  // Version column
}
```

### Configuration

```csharp
entity.UseReplacingMergeTree(
    versionColumn: x => x.UpdatedAt,
    orderByColumn: x => x.Id);
```

### "Updating" Data

```csharp
// Insert new version with same Id
context.Users.Add(new User
{
    Id = existingUser.Id,      // Same key
    Name = "New Name",         // Updated value
    UpdatedAt = DateTime.UtcNow // Higher version
});
await context.SaveChangesAsync();
```

### Querying with FINAL

```csharp
// Guaranteed deduplicated results
var user = await context.Users
    .Final()
    .FirstAsync(u => u.Id == userId);
```

## When to Use

- User profiles that need updates
- Product catalogs with price changes
- Configuration/settings tables
- Any "last write wins" scenario

## When NOT to Use

- Append-only event logs (use MergeTree)
- High-frequency updates (consider design changes)
- When you need immediate consistency (FINAL adds overhead)

## Learn More

- [ReplacingMergeTree Documentation](../../docs/engines/replacing-mergetree.md)
- [Engines Overview](../../docs/engines/overview.md)
