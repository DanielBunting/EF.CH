# Query Modifiers

EF.CH provides LINQ extension methods for ClickHouse-specific query modifiers: `Final()`, `Sample()`, and `WithSettings()`.

## Final()

Forces deduplication for ReplacingMergeTree tables. Without `FINAL`, queries may return duplicate rows that haven't been merged yet.

### Usage

```csharp
// Get deduplicated results
var users = await context.Users
    .Final()
    .Where(u => u.IsActive)
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Users" FINAL WHERE "IsActive" = true
```

### When to Use

- ReplacingMergeTree tables with pending merges
- When you need the latest version of each row
- Real-time queries requiring consistency

### Performance Note

`FINAL` has performance overhead as it merges rows on-the-fly. For large tables:

```csharp
// Better: Filter first, then apply FINAL
var recentUsers = await context.Users
    .Where(u => u.UpdatedAt > cutoff)  // Reduce dataset first
    .Final()
    .ToListAsync();
```

## Sample()

Probabilistic sampling for approximate results on large datasets. Returns a random fraction of rows.

### Basic Sampling

```csharp
// Sample 10% of rows
var sample = await context.Events
    .Sample(0.1)
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events" SAMPLE 0.1
```

### Reproducible Sampling

Use an offset for consistent results across queries:

```csharp
// Same sample every time
var sample = await context.Events
    .Sample(0.1, offset: 12345)
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events" SAMPLE 0.1 OFFSET 12345
```

### Requirements

Sampling requires a `SAMPLE BY` clause in the table definition:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasSampleBy("intHash32(Id)");  // Required for Sample()
});
```

### When to Use

- Exploratory analytics on very large tables
- Approximate counts and aggregations
- Quick previews of data distribution

## WithSettings()

Applies ClickHouse query settings to control execution behavior.

### Single Setting

```csharp
// Set query timeout
var events = await context.Events
    .WithSetting("max_execution_time", 30)
    .ToListAsync();
```

### Multiple Settings

```csharp
// Multiple settings
var events = await context.Events
    .WithSettings(new Dictionary<string, object>
    {
        ["max_threads"] = 4,
        ["max_rows_to_read"] = 1000000,
        ["optimize_read_in_order"] = 1
    })
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events" SETTINGS max_threads = 4, max_rows_to_read = 1000000
```

### Common Settings

| Setting | Description |
|---------|-------------|
| `max_threads` | Maximum parallel threads |
| `max_execution_time` | Query timeout in seconds |
| `max_rows_to_read` | Fail if more rows would be read |
| `max_bytes_to_read` | Fail if more bytes would be read |
| `optimize_read_in_order` | Optimize reading in ORDER BY key order |
| `max_block_size` | Maximum rows per block |

### Resource Limits

Protect against runaway queries:

```csharp
var events = await context.Events
    .WithSettings(new Dictionary<string, object>
    {
        ["max_execution_time"] = 60,      // 60 second timeout
        ["max_rows_to_read"] = 10000000,  // Max 10M rows
        ["max_bytes_to_read"] = 1073741824  // Max 1GB
    })
    .ToListAsync();
```

## Combining Modifiers

Modifiers can be chained:

```csharp
var result = await context.Users
    .Final()                              // Deduplicate first
    .Where(u => u.Country == "US")        // Filter
    .WithSetting("max_threads", 8)        // Control resources
    .OrderByDescending(u => u.CreatedAt)
    .Take(100)
    .ToListAsync();
```

## Complete Example

```csharp
public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public decimal Value { get; set; }
}

modelBuilder.Entity<AnalyticsEvent>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
    entity.HasPartitionByMonth(x => x.Timestamp);
    entity.HasSampleBy("intHash32(Id)");  // Enable sampling
});
```

### Usage

```csharp
// Quick approximate count using 1% sample
var approxCount = await context.AnalyticsEvents
    .Sample(0.01)
    .Where(e => e.EventType == "purchase")
    .CountAsync() * 100;  // Extrapolate

// Exact results with timeout protection
var recentEvents = await context.AnalyticsEvents
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-7))
    .WithSetting("max_execution_time", 30)
    .ToListAsync();
```

## Modifier Placement

Modifiers should generally come early in the query:

```csharp
// Good: Modifiers before filters
context.Users
    .Final()
    .Sample(0.1)
    .Where(u => u.IsActive)
    .Select(u => new { u.Id, u.Name });

// Avoid: Modifiers after complex operations
context.Users
    .Where(u => u.IsActive)
    .GroupBy(u => u.Country)
    .Final();  // May not have desired effect
```

## Limitations

- **Sample requires SAMPLE BY**: Table must be configured with `HasSampleBy()`
- **Final overhead**: Merges rows in memory, impacting performance
- **Settings scope**: Applied per-query, not connection-wide

## See Also

- [ReplacingMergeTree](../engines/replacing-mergetree.md) - Uses `Final()` for deduplication
- [ClickHouse FINAL Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/from#final-modifier)
- [ClickHouse SAMPLE Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/sample)
- [ClickHouse SETTINGS Docs](https://clickhouse.com/docs/en/sql-reference/statements/select#settings-in-select-query)
