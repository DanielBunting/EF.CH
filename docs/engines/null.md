# Null Engine

The Null engine discards all inserted data and returns empty results for SELECT queries. It functions like `/dev/null` - data goes in but is never stored.

## When to Use

- **Materialized view source**: Raw events trigger MVs but don't need to be stored
- **Testing/benchmarking**: Measure INSERT throughput without disk I/O
- **Development**: Quickly iterate on MV definitions without data accumulation

## Configuration

### Basic Setup

```csharp
public class RawEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class MyDbContext : DbContext
{
    public DbSet<RawEvent> RawEvents => Set<RawEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RawEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseNullEngine();  // No ORDER BY required
        });
    }
}
```

### Generic Overload

```csharp
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseNullEngine();  // Works with both generic and non-generic builders
});
```

## Generated DDL

The configuration above generates:

```sql
CREATE TABLE "RawEvents" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "Category" String NOT NULL,
    "Amount" Decimal(18, 4) NOT NULL
)
ENGINE = Null
```

**Note**: Unlike MergeTree engines, the Null engine:
- Does **not** require ORDER BY
- Does **not** use parentheses (`ENGINE = Null`, not `ENGINE = Null()`)
- Ignores PARTITION BY, TTL, and other table-level options

## Common Pattern: Null + Materialized Views

The primary use case is as a source table for materialized views:

```csharp
// Raw events - discarded after triggering MVs
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseNullEngine();
});

// Hourly aggregates - stored in SummingMergeTree with TTL
modelBuilder.Entity<HourlySummary>(entity =>
{
    entity.HasNoKey();
    entity.UseSummingMergeTree(x => new { x.Hour, x.Category });
    entity.HasPartitionByMonth(x => x.Hour);
    entity.HasTtl(x => x.Hour, ClickHouseInterval.Years(1));
    entity.AsMaterializedView<HourlySummary, RawEvent>(
        events => events
            .GroupBy(e => new { Hour = e.Timestamp.ToStartOfHour(), e.Category })
            .Select(g => new HourlySummary
            {
                Hour = g.Key.Hour,
                Category = g.Key.Category,
                EventCount = g.Count(),
                TotalAmount = g.Sum(e => e.Amount)
            }));
});
```

### How This Pattern Works

1. **INSERT into Null table**: Data is written to `RawEvents`
2. **MV triggers**: The materialized view processes the inserted data
3. **Aggregates stored**: Summarized data is written to `HourlySummary`
4. **Raw data discarded**: `RawEvents` doesn't store anything

This gives you:
- Zero storage for raw events
- Instant aggregation on write
- Queryable summaries with TTL for retention

## Usage Examples

### Inserting Data

```csharp
// Data triggers materialized views but isn't stored
context.RawEvents.Add(new RawEvent
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    Category = "sales",
    Amount = 99.99m
});
await context.SaveChangesAsync();

// Batch insert (more efficient)
var events = Enumerable.Range(0, 10000)
    .Select(i => new RawEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow,
        Category = i % 2 == 0 ? "sales" : "returns",
        Amount = Random.Shared.Next(1, 1000)
    });

context.RawEvents.AddRange(events);
await context.SaveChangesAsync();
```

### Querying (Always Empty)

```csharp
// This always returns empty - data is not stored
var events = await context.RawEvents.ToListAsync();  // []

// Query the materialized view instead
var summaries = await context.HourlySummaries
    .Where(s => s.Hour > DateTime.UtcNow.AddDays(-7))
    .ToListAsync();
```

## Limitations

- **No data storage**: SELECT always returns empty results
- **Cannot be queried directly**: The table exists only to trigger MVs
- **No ORDER BY/PARTITION BY**: These options are ignored
- **No TTL**: Since no data is stored, TTL has no effect

## When Not to Use Null Engine

| Need | Use Instead |
|------|-------------|
| Query raw data | [MergeTree](mergetree.md) |
| Store and query aggregates | [SummingMergeTree](summing-mergetree.md) |
| Deduplicate on insert | [ReplacingMergeTree](replacing-mergetree.md) |

## See Also

- [Engines Overview](overview.md)
- [Materialized Views](../features/materialized-views.md)
- [TTL](../features/ttl.md)
- [ClickHouse Null Engine Docs](https://clickhouse.com/docs/en/engines/table-engines/special/null)
