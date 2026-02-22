# MergeTree Engine

The default and most versatile ClickHouse table engine. MergeTree stores data sorted by the ORDER BY key, supports partitioning, sampling, TTL, and custom engine settings.

## ORDER BY

Every MergeTree table requires an ORDER BY clause that defines the sort key and determines how data is stored on disk.

### Expression lambda

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Date, x.Id });
});
```

```sql
CREATE TABLE "Events" (
    ...
)
ENGINE = MergeTree()
ORDER BY ("Date", "Id")
```

### Single column

```csharp
entity.UseMergeTree(x => x.Id);
```

```sql
ENGINE = MergeTree()
ORDER BY ("Id")
```

### String overload

```csharp
entity.UseMergeTree("EventTime", "Id");
```

```sql
ENGINE = MergeTree()
ORDER BY ("EventTime", "Id")
```

## PARTITION BY

Partitioning controls how data is physically organized on disk. Choose partition granularity based on query patterns and data volume.

### Monthly partitioning

```csharp
entity.UseMergeTree(x => new { x.Date, x.Id })
    .HasPartitionByMonth(x => x.CreatedAt);
```

```sql
ENGINE = MergeTree()
PARTITION BY toYYYYMM("CreatedAt")
ORDER BY ("Date", "Id")
```

### Daily partitioning

```csharp
entity.HasPartitionByDay(x => x.EventDate);
```

```sql
PARTITION BY toYYYYMMDD("EventDate")
```

### Yearly partitioning

```csharp
entity.HasPartitionByYear(x => x.OrderDate);
```

```sql
PARTITION BY toYear("OrderDate")
```

### Custom partition expression

```csharp
entity.HasPartitionBy(x => x.Region);
```

```sql
PARTITION BY "Region"
```

### Raw string partition expression

```csharp
entity.HasPartitionBy("toYYYYMM(EventTime)");
```

```sql
PARTITION BY toYYYYMM(EventTime)
```

## PRIMARY KEY

By default, the PRIMARY KEY matches the ORDER BY key. You can override this to use a prefix of the ORDER BY columns, which affects the primary index granularity without changing the sort order.

```csharp
entity.UseMergeTree(x => new { x.Date, x.UserId, x.Id });
// PRIMARY KEY defaults to ORDER BY if not explicitly set
```

> **Note:** ClickHouse PRIMARY KEY is not a uniqueness constraint. It defines which columns appear in the sparse primary index for faster lookups. If not specified, it defaults to the ORDER BY columns.

## SAMPLE BY

Enables probabilistic sampling for approximate queries. The SAMPLE BY column must be included in the ORDER BY key.

```csharp
entity.UseMergeTree(x => new { x.UserId, x.Date })
    .HasSampleBy(x => x.UserId);
```

```sql
ENGINE = MergeTree()
ORDER BY ("UserId", "Date")
SAMPLE BY "UserId"
```

At query time, use the `.Sample()` extension:

```csharp
var sample = await context.Events
    .Sample(0.1)        // 10% of data
    .Where(x => x.Active)
    .ToListAsync();
```

```sql
SELECT ... FROM "Events" SAMPLE 0.1 WHERE ...
```

## TTL

Time-to-live expressions automatically expire old rows during background merges.

### TimeSpan

Best for days, hours, minutes, and seconds. The TimeSpan is converted to the largest whole unit that fits.

```csharp
entity.UseMergeTree(x => x.CreatedAt)
    .HasTtl(x => x.CreatedAt, TimeSpan.FromDays(30));
```

```sql
ENGINE = MergeTree()
ORDER BY ("CreatedAt")
TTL "CreatedAt" + INTERVAL 30 DAY
```

```csharp
entity.HasTtl(x => x.CreatedAt, TimeSpan.FromHours(24));
```

```sql
TTL "CreatedAt" + INTERVAL 24 HOUR
```

### ClickHouseInterval

Use for calendar-based intervals (months, quarters, years) that cannot be accurately represented by TimeSpan.

```csharp
entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Months(3));
```

```sql
TTL "CreatedAt" + INTERVAL 3 MONTH
```

```csharp
entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Years(1));
```

```sql
TTL "CreatedAt" + INTERVAL 1 YEAR
```

### Raw SQL

```csharp
entity.HasTtl("CreatedAt + INTERVAL 90 DAY");
```

```sql
TTL CreatedAt + INTERVAL 90 DAY
```

> **Note:** TTL expiration happens during background merges, not immediately. Use `OPTIMIZE TABLE ... FINAL` to force cleanup. Expired rows may still be visible until the merge occurs.

## Engine SETTINGS

Pass ClickHouse engine-level settings as a dictionary.

```csharp
entity.UseMergeTree(x => new { x.Date, x.Id })
    .HasEngineSettings(new Dictionary<string, string>
    {
        ["max_parts_to_merge_at_once"] = "16",
        ["index_granularity"] = "4096"
    });
```

```sql
ENGINE = MergeTree()
ORDER BY ("Date", "Id")
SETTINGS max_parts_to_merge_at_once = 16, index_granularity = 4096
```

## Complete Example

A fully configured MergeTree table with all options:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.ToTable("events");
    entity.HasKey(e => e.Id);

    entity.UseMergeTree(x => new { x.Date, x.Id })
        .HasPartitionByMonth(x => x.CreatedAt)
        .HasSampleBy(x => x.UserId)
        .HasTtl(x => x.CreatedAt, TimeSpan.FromDays(30))
        .HasEngineSettings(new Dictionary<string, string>
        {
            ["max_parts_to_merge_at_once"] = "16"
        });
});
```

```sql
CREATE TABLE "events" (
    "Id" UUID,
    "Date" Date,
    "CreatedAt" DateTime64(3),
    "UserId" UInt64,
    ...
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM("CreatedAt")
ORDER BY ("Date", "Id")
TTL "CreatedAt" + INTERVAL 30 DAY
SETTINGS max_parts_to_merge_at_once = 16
```

## See Also

- [ReplacingMergeTree](replacing-mergetree.md) -- deduplication by version column
- [SummingMergeTree](summing-mergetree.md) -- automatic numeric column summation
- [AggregatingMergeTree](aggregating-mergetree.md) -- intermediate aggregate state storage
