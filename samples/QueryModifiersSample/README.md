# QueryModifiersSample

Demonstrates ClickHouse-specific query modifiers: `Final()`, `Sample()`, and `WithSettings()`.

## What This Shows

- `Final()` for ReplacingMergeTree deduplication
- `Sample()` for probabilistic sampling on large datasets
- `WithSettings()` for query-level execution settings
- `WithSetting()` shorthand for single settings

## Query Modifiers

| Modifier | Purpose | SQL Generated |
|----------|---------|---------------|
| `Final()` | Force deduplication | `SELECT ... FROM table FINAL` |
| `Sample(0.1)` | Sample 10% of rows | `SELECT ... FROM table SAMPLE 0.1` |
| `WithSettings()` | Custom settings | `SELECT ... SETTINGS key=value` |

## Prerequisites

- .NET 10.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Expected Output

```
Query Modifiers Sample
======================

Creating database and tables...
Inserting user profiles (with version updates)...

Inserted users with version history.

Inserting events for sampling demo...

Inserted 1000 events.

--- Without Final() - may show duplicate versions ---
  alice@example.com: Alice (updated 10:30:00)
  alice@example.com: Alice Smith (updated 12:30:00)
  bob@example.com: Bob (updated 11:30:00)
  charlie@example.com: Charlie (updated 09:30:00)
  charlie.updated@example.com: Charlie Brown (updated 12:00:00)
Total rows: 5

--- With Final() - shows only latest versions ---
  alice@example.com: Alice Smith (updated 12:30:00)
  bob@example.com: Bob (updated 11:30:00)
  charlie.updated@example.com: Charlie Brown (updated 12:00:00)
Total rows: 3

--- Sample(0.1) - approximately 10% of events ---
Sampled 98 events (expected ~100)
Purchase events in sample: 32
Extrapolated total purchases: ~320

--- WithSettings() - controlling query execution ---
Found 330 purchase events with custom settings.

--- WithSetting() - single setting shorthand ---
Retrieved 100 events with row limit setting.

--- Combining Final() with other operations ---
Found 3 deduplicated active users.

Done!
```

## Key Code

### Final() for Deduplication

```csharp
// Without Final - may see duplicate versions
var users = await context.Users.ToListAsync();

// With Final - forces deduplication on-the-fly
var users = await context.Users
    .Final()
    .ToListAsync();
```

### Sample() for Large Datasets

```csharp
// Sample 10% of rows for approximate results
var sample = await context.Events
    .Sample(0.1)
    .ToListAsync();

// Extrapolate: sample.Count * 10 ≈ total count
```

**Requirement**: Table must have `SAMPLE BY` clause:
```csharp
entity.HasSampleBy("intHash32(Id)");
```

### WithSettings() for Execution Control

```csharp
// Multiple settings
var events = await context.Events
    .WithSettings(new Dictionary<string, object>
    {
        ["max_threads"] = 4,
        ["max_execution_time"] = 30,
        ["max_rows_to_read"] = 1000000
    })
    .ToListAsync();

// Single setting shorthand
var events = await context.Events
    .WithSetting("max_execution_time", 60)
    .ToListAsync();
```

## Common Settings

| Setting | Description |
|---------|-------------|
| `max_threads` | Maximum parallel threads |
| `max_execution_time` | Query timeout (seconds) |
| `max_rows_to_read` | Fail if more rows would be read |
| `max_bytes_to_read` | Fail if more bytes would be read |
| `optimize_read_in_order` | Read in ORDER BY key order |

## ReplacingMergeTree Configuration

For `Final()` to work, use ReplacingMergeTree:

```csharp
entity.UseReplacingMergeTree(
    versionColumnExpression: x => x.UpdatedAt,
    orderByExpression: x => x.Id);
```

## Learn More

- [Query Modifiers Documentation](../../docs/features/query-modifiers.md)
- [ReplacingMergeTree Documentation](../../docs/engines/replacing-mergetree.md)
