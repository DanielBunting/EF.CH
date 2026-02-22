# TTL (Time to Live)

TTL configures automatic data expiration at the table level. When a row's TTL expression evaluates to a timestamp in the past, ClickHouse removes the row during background merge operations. This provides automatic data lifecycle management without scheduled jobs or manual cleanup.

---

## Configuration Methods

### TimeSpan Interval

Best for intervals measured in days, hours, minutes, or seconds. The TimeSpan is converted to the largest whole unit that fits.

```csharp
modelBuilder.Entity<LogEntry>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.ServiceName });

    // Expire after 30 days
    entity.HasTtl(x => x.Timestamp, TimeSpan.FromDays(30));
});
```

**Generated SQL:**

```sql
CREATE TABLE LogEntries (
    ...
) ENGINE = MergeTree()
ORDER BY (Timestamp, ServiceName)
TTL "Timestamp" + INTERVAL 30 DAY
```

Additional examples:

```csharp
// Expire after 24 hours
entity.HasTtl(x => x.CreatedAt, TimeSpan.FromHours(24));

// Expire after 7 days
entity.HasTtl(x => x.EventDate, TimeSpan.FromDays(7));
```

`TimeSpan` cannot accurately represent calendar months or years (which vary in length). For those intervals, use `ClickHouseInterval`.

### ClickHouseInterval

Supports all ClickHouse interval units including calendar-based units like months, quarters, and years.

```csharp
// Expire after 3 months
entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Months(3));

// Expire after 1 year
entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Years(1));

// Expire after 2 quarters
entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Quarters(2));

// Expire after 4 weeks
entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Weeks(4));
```

**Generated SQL:**

```sql
TTL "CreatedAt" + INTERVAL 3 MONTH
TTL "CreatedAt" + INTERVAL 1 YEAR
TTL "CreatedAt" + INTERVAL 2 QUARTER
TTL "CreatedAt" + INTERVAL 4 WEEK
```

Available `ClickHouseInterval` factory methods:

| Method | SQL Unit | Example |
|---|---|---|
| `ClickHouseInterval.Seconds(n)` | `SECOND` | Short-lived cache entries |
| `ClickHouseInterval.Minutes(n)` | `MINUTE` | Session data |
| `ClickHouseInterval.Hours(n)` | `HOUR` | Temporary processing data |
| `ClickHouseInterval.Days(n)` | `DAY` | Recent event logs |
| `ClickHouseInterval.Weeks(n)` | `WEEK` | Weekly retention windows |
| `ClickHouseInterval.Months(n)` | `MONTH` | Standard retention policies |
| `ClickHouseInterval.Quarters(n)` | `QUARTER` | Quarterly compliance windows |
| `ClickHouseInterval.Years(n)` | `YEAR` | Long-term archives |

### Raw SQL Expression

For complex TTL logic that cannot be expressed with the typed API.

```csharp
entity.HasTtl("CreatedAt + INTERVAL 90 DAY");

entity.HasTtl("toDateTime(EventDate) + INTERVAL 6 MONTH");
```

**Generated SQL:**

```sql
TTL CreatedAt + INTERVAL 90 DAY
TTL toDateTime(EventDate) + INTERVAL 6 MONTH
```

---

## How TTL Works

1. **Background processing**: ClickHouse evaluates TTL during background merge operations, not at query time. Expired rows are not immediately deleted -- they are removed when ClickHouse merges the data parts that contain them.

2. **Eventual deletion**: There is a delay between when a row expires and when it is physically removed. The `merge_with_ttl_timeout` server setting controls the minimum interval between TTL merges (default: 14400 seconds / 4 hours).

3. **No query filtering**: Expired but not-yet-removed rows are still visible in query results. If exact cutoff behavior is needed, combine TTL with a WHERE filter in your queries.

4. **Partition alignment**: TTL works well with partitioning. When all rows in a partition are expired, ClickHouse can drop the entire partition efficiently rather than rewriting individual parts.

---

## TTL with Partitioning

Combining TTL with partitioning is the recommended pattern for time-series data. TTL handles automatic cleanup, and partitioning enables efficient partition-level drops for immediate space reclamation.

```csharp
modelBuilder.Entity<MetricPoint>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.MetricName });

    entity.HasPartitionByMonth(x => x.Timestamp);
    entity.HasTtl(x => x.Timestamp, ClickHouseInterval.Months(6));
});
```

For immediate removal of old data, dropping a partition is instantaneous regardless of data volume:

```csharp
await context.Database.ExecuteSqlRawAsync(
    "ALTER TABLE MetricPoints DROP PARTITION 202501");
```

---

## Choosing a Retention Strategy

| Retention Period | Recommended Method |
|---|---|
| Hours to days | `TimeSpan.FromHours(n)` or `TimeSpan.FromDays(n)` |
| Weeks | `ClickHouseInterval.Weeks(n)` or `TimeSpan.FromDays(n * 7)` |
| Months | `ClickHouseInterval.Months(n)` |
| Quarters | `ClickHouseInterval.Quarters(n)` |
| Years | `ClickHouseInterval.Years(n)` |
| Complex expressions | Raw SQL string |

Use `ClickHouseInterval` over `TimeSpan` for months and longer periods, since `TimeSpan` cannot represent variable-length calendar units.

---

## Complete Example

```csharp
modelBuilder.Entity<AuditLog>(entity =>
{
    entity.HasKey(x => x.Id);

    entity.UseMergeTree(x => new { x.Timestamp, x.Action });

    // Monthly partitions for management
    entity.HasPartitionByMonth(x => x.Timestamp);

    // Auto-expire after 1 year
    entity.HasTtl(x => x.Timestamp, ClickHouseInterval.Years(1));

    // Timestamp codec for compression
    entity.Property(x => x.Timestamp)
        .HasTimestampCodec();

    // LowCardinality for action type
    entity.Property(x => x.Action)
        .HasLowCardinality();
});
```

---

## See Also

- [Partitioning](partitioning.md) -- PARTITION BY strategies for data organization
- [Column Features](column-features.md) -- compression codecs, computed columns
- [Skip Indices](skip-indices.md) -- data skipping indices for query acceleration
