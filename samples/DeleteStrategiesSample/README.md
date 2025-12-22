# DeleteStrategiesSample

Demonstrates ClickHouse delete operations and strategies.

## What This Shows

- Single entity delete via change tracker
- Bulk delete via `ExecuteDeleteAsync`
- Lightweight delete strategy (default)
- Mutation delete strategy (for bulk maintenance)

## Delete Strategies

| Strategy | SQL Generated | Use Case |
|----------|---------------|----------|
| Lightweight | `DELETE FROM table WHERE ...` | Normal operations |
| Mutation | `ALTER TABLE table DELETE WHERE ...` | Bulk maintenance |

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Delete Strategies Sample
========================

Creating database and tables...
Inserting events...

Inserted 5 events.

--- Initial Events ---
Events (5):
  Event A [important] - 2025-01-05
  Event B [temporary] - 2025-01-10
  Event C [temporary] - 2025-01-12
  Event D [important] - 2025-01-14
  Event E [temporary] - 2025-01-15

--- Delete single entity via change tracker ---
Deleting: Event A
Events (4):
  Event B [temporary] - 2025-01-10
  Event C [temporary] - 2025-01-12
  Event D [important] - 2025-01-14
  Event E [temporary] - 2025-01-15

--- Bulk delete temporary events via ExecuteDeleteAsync ---
Deleting all events with Category = 'temporary'...
Events (1):
  Event D [important] - 2025-01-14

--- Delete Strategy Comparison ---
Lightweight (default): DELETE FROM table WHERE ...
  - Instant marking, filtered immediately
  - Physical deletion during background merges
  - Best for normal operations

Mutation: ALTER TABLE table DELETE WHERE ...
  - Async operation, rewrites data parts
  - Does not return affected row count
  - Best for bulk maintenance only

...

Done!
```

## Key Code

### Single Entity Delete

```csharp
var entity = await context.Events.FirstAsync(e => e.Id == targetId);
context.Events.Remove(entity);
await context.SaveChangesAsync();
```

### Bulk Delete (Most Efficient)

```csharp
// Delete without loading entities
await context.Events
    .Where(e => e.Category == "temporary")
    .ExecuteDeleteAsync();
```

### Configure Delete Strategy

```csharp
// Default: Lightweight
options.UseClickHouse(connectionString);

// Explicit: Lightweight
options.UseClickHouse(connectionString,
    o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Lightweight));

// Mutation (for maintenance jobs)
options.UseClickHouse(connectionString,
    o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

## Strategy Comparison

### Lightweight Delete (Default)

- **How**: Marks rows as deleted immediately
- **Visibility**: Rows filtered out from queries immediately
- **Physical deletion**: During background merges
- **Row count**: Returns affected row count
- **Use for**: Normal application operations

### Mutation Delete

- **How**: Issues `ALTER TABLE DELETE` mutation
- **Visibility**: Eventually consistent
- **Physical deletion**: Async operation rewrites parts
- **Row count**: Returns 0 (async operation)
- **Use for**: Bulk maintenance, cleanup jobs

## UPDATE Not Supported

ClickHouse doesn't support row-level UPDATE. Attempting to update throws:

```csharp
var entity = await context.Events.FirstAsync();
entity.Name = "Modified";
await context.SaveChangesAsync();  // Throws NotSupportedException
```

### Workarounds

1. **ReplacingMergeTree**: Insert new version with same key
2. **Delete + Insert**: Remove old row, add new one

See [ReplacingMergeTree Sample](../ReplacingMergeTreeSample/) for update patterns.

## Learn More

- [Delete Operations Documentation](../../docs/features/delete-operations.md)
- [ReplacingMergeTree Documentation](../../docs/engines/replacing-mergetree.md)
