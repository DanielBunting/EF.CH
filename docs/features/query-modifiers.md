# Query Modifiers

EF.CH provides LINQ extension methods for ClickHouse-specific query modifiers: `Final()`, `Sample()`, `PreWhere()`, `LimitBy()`, and `WithSettings()`.

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

## PreWhere()

Applies optimized pre-filtering that reads only the filter columns before reading remaining columns. This reduces I/O for large tables with selective filters.

### Usage

```csharp
// Filter on indexed column - reads Date first, then remaining columns for matching rows
var events = await context.Events
    .PreWhere(e => e.Date > DateTime.UtcNow.AddDays(-7))
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events"
PREWHERE "Date" > @p0
```

### Combined Conditions

```csharp
// Multiple conditions in single PreWhere
var events = await context.Events
    .PreWhere(e => e.Date > cutoffDate && e.Type == "click")
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events"
PREWHERE ("Date" > @p0) AND ("Type" = 'click')
```

### When to Use PREWHERE

**Ideal for:**
- Filter on indexed/sorted columns (ORDER BY key columns)
- Highly selective filters that eliminate most rows (>90% filtered out)
- Large tables where I/O reduction matters
- Queries reading many columns but filtering on few

**Avoid when:**
- Filters match most rows anyway (low selectivity)
- Small tables where overhead isn't worth it
- Complex expressions that can't be pushed down

### Performance Example

For a 1TB table with 100 columns, filtering on an indexed date column:

```csharp
// PREWHERE: Reads only Date column (~10GB), then full columns for matching rows
var efficient = await context.LargeTable
    .PreWhere(e => e.Date > cutoffDate)  // Eliminates 95% of rows
    .ToListAsync();

// WHERE: Reads all 100 columns (~1TB), then filters
var lesEfficient = await context.LargeTable
    .Where(e => e.Date > cutoffDate)
    .ToListAsync();
```

### Limitations

- Only one `PreWhere()` call per query - combine conditions in a single call
- Best performance on primary key columns (ORDER BY columns)
- ClickHouse may auto-optimize simple WHERE to PREWHERE in some cases

## LimitBy()

Returns the top N rows per group based on a key, using ClickHouse's `LIMIT BY` clause. This is more efficient than window functions for "top N per category" queries.

### Basic Usage

```csharp
// Top 5 events per category
var topEvents = await context.Events
    .OrderByDescending(e => e.Score)
    .LimitBy(5, e => e.Category)
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events" ORDER BY "Score" DESC LIMIT 5 BY "Category"
```

### Compound Keys

Use anonymous types for multi-column grouping:

```csharp
// Top 3 per category AND region
var results = await context.Events
    .OrderByDescending(e => e.Score)
    .LimitBy(3, e => new { e.Category, e.Region })
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events" ORDER BY "Score" DESC LIMIT 3 BY "Category", "Region"
```

### With Offset

Skip rows within each group before taking the limit:

```csharp
// Skip first 2, take next 5 per user (rows 3-7 per user)
var results = await context.Events
    .OrderByDescending(e => e.CreatedAt)
    .LimitBy(2, 5, e => e.UserId)
    .ToListAsync();
```

Generates:
```sql
SELECT ... FROM "Events" ORDER BY "CreatedAt" DESC LIMIT 2, 5 BY "UserId"
```

### When to Use

**Ideal for:**
- "Top N per category" queries (top products per category, recent posts per user)
- Pagination within groups
- Sampling a fixed number from each partition

**Compared to Window Functions:**

```csharp
// Using LimitBy - simpler and often faster
var top3 = await context.Products
    .OrderByDescending(p => p.Sales)
    .LimitBy(3, p => p.Category)
    .ToListAsync();

// Equivalent using window functions - more complex
var top3Window = await context.Products
    .Select(p => new
    {
        Product = p,
        Rank = EF.Functions.RowNumber(
            EF.Functions.Over()
                .PartitionBy(p.Category)
                .OrderByDescending(p.Sales))
    })
    .Where(x => x.Rank <= 3)
    .Select(x => x.Product)
    .ToListAsync();
```

### Requirements

- **Requires ORDER BY**: Always use `OrderBy()` or `OrderByDescending()` before `LimitBy()` to control which rows are kept
- **Limit must be positive**: The limit parameter must be greater than 0
- **Offset must be non-negative**: When using offset, it must be >= 0

### Example: Recent Activity Per User

```csharp
public class UserActivity
{
    public Guid Id { get; set; }
    public Guid UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public string Action { get; set; } = string.Empty;
}

// Get last 10 activities per user
var recentActivity = await context.UserActivities
    .OrderByDescending(a => a.Timestamp)
    .LimitBy(10, a => a.UserId)
    .ToListAsync();

// Paginate: skip first 10, take next 10 per user
var page2Activity = await context.UserActivities
    .OrderByDescending(a => a.Timestamp)
    .LimitBy(10, 10, a => a.UserId)
    .ToListAsync();
```

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
    .PreWhere(u => u.CreatedAt > cutoff)  // Pre-filter on indexed column
    .Where(u => u.Country == "US")        // Additional filter
    .WithSetting("max_threads", 8)        // Control resources
    .OrderByDescending(u => u.CreatedAt)
    .LimitBy(5, u => u.Department)        // Top 5 per department
    .Take(100)                            // Overall limit
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

Modifiers should generally come early in the query, except `LimitBy()` which should come after `OrderBy()`:

```csharp
// Good: Modifiers before filters, LimitBy after OrderBy
context.Users
    .Final()
    .Sample(0.1)
    .PreWhere(u => u.CreatedAt > cutoff)
    .Where(u => u.IsActive)
    .OrderByDescending(u => u.Score)
    .LimitBy(5, u => u.Department)
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
- **PreWhere single call**: Only one `PreWhere()` per query - combine conditions in one call
- **LimitBy requires OrderBy**: Always call `OrderBy()` before `LimitBy()` to control row selection
- **Settings scope**: Applied per-query, not connection-wide

## See Also

- [Set Operations](set-operations.md) - UNION, INTERSECT, EXCEPT with convenience extensions
- [CTEs](cte.md) - Common Table Expressions via `AsCte()`
- [Time Series Gap Filling](interpolate.md) - `Interpolate()` for WITH FILL and INTERPOLATE clauses
- [ReplacingMergeTree](../engines/replacing-mergetree.md) - Uses `Final()` for deduplication
- [ClickHouse FINAL Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/from#final-modifier)
- [ClickHouse SAMPLE Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/sample)
- [ClickHouse PREWHERE Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/prewhere)
- [ClickHouse LIMIT BY Docs](https://clickhouse.com/docs/en/sql-reference/statements/select/limit-by)
- [ClickHouse SETTINGS Docs](https://clickhouse.com/docs/en/sql-reference/statements/select#settings-in-select-query)
