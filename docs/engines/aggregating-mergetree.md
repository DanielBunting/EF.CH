# AggregatingMergeTree Engine

AggregatingMergeTree stores intermediate aggregate function states and merges them during background compaction. Unlike SummingMergeTree which only sums, this engine supports any ClickHouse aggregate function (count, uniq, quantile, etc.) through `-State` and `-Merge` combinators.

## Basic Configuration

```csharp
modelBuilder.Entity<DailyAggregates>(entity =>
{
    entity.UseAggregatingMergeTree(x => x.Timestamp);
});
```

```sql
CREATE TABLE "DailyAggregates" (
    "Timestamp" DateTime64(3),
    ...
)
ENGINE = AggregatingMergeTree()
ORDER BY ("Timestamp")
```

## Multi-Column ORDER BY

```csharp
entity.UseAggregatingMergeTree(x => new { x.Date, x.Category });
```

```sql
ENGINE = AggregatingMergeTree()
ORDER BY ("Date", "Category")
```

## String Overload

```csharp
entity.UseAggregatingMergeTree("Date", "Category");
```

```sql
ENGINE = AggregatingMergeTree()
ORDER BY ("Date", "Category")
```

## AggregateFunction Columns

Aggregate state columns store binary intermediate state. In C#, these are mapped as `byte[]` and configured with `HasAggregateFunction`:

```csharp
public class DailyAggregates
{
    public DateOnly Date { get; set; }
    public string Category { get; set; } = string.Empty;
    public byte[] TotalAmount { get; set; } = [];     // AggregateFunction(sum, Int64)
    public byte[] UniqueUsers { get; set; } = [];     // AggregateFunction(uniq, UUID)
    public byte[] EventCount { get; set; } = [];      // AggregateFunction(count)
}

modelBuilder.Entity<DailyAggregates>(entity =>
{
    entity.UseAggregatingMergeTree(x => new { x.Date, x.Category });

    entity.Property(x => x.TotalAmount)
        .HasAggregateFunction("sum", typeof(long));

    entity.Property(x => x.UniqueUsers)
        .HasAggregateFunction("uniq", typeof(Guid));

    entity.Property(x => x.EventCount)
        .HasAggregateFunctionRaw("count", "");
});
```

```sql
CREATE TABLE "DailyAggregates" (
    "Date" Date,
    "Category" String,
    "TotalAmount" AggregateFunction(sum, Int64),
    "UniqueUsers" AggregateFunction(uniq, UUID),
    "EventCount" AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
ORDER BY ("Date", "Category")
```

## Materialized View Pattern

The most common use of AggregatingMergeTree is as the target of a materialized view. Raw data flows into a source table, and the materialized view computes aggregate states into the AggregatingMergeTree table.

```csharp
// Source table -- raw events
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.UseMergeTree(x => new { x.Date, x.Id });
});

// Target table -- aggregate states
modelBuilder.Entity<DailyAggregates>(entity =>
{
    entity.UseAggregatingMergeTree(x => new { x.Date, x.Category });

    entity.AsMaterializedView<DailyAggregates, RawEvent>(
        query: events => events
            .GroupBy(e => new { e.Date, e.Category })
            .Select(g => new DailyAggregates
            {
                Date = g.Key.Date,
                Category = g.Key.Category,
                TotalAmount = g.SumState(e => e.Amount),
                UniqueUsers = g.UniqState(e => e.UserId),
                EventCount = g.CountState()
            }),
        populate: false
    );
});
```

## Reading Aggregate States with -Merge

To read back the aggregated results, use `-Merge` combinators which finalize the intermediate states:

```csharp
var results = await context.DailyAggregates
    .GroupBy(a => new { a.Date, a.Category })
    .Select(g => new
    {
        g.Key.Date,
        g.Key.Category,
        TotalAmount = g.SumMerge(a => a.TotalAmount),
        UniqueUsers = g.UniqMerge(a => a.UniqueUsers),
        EventCount = g.CountMerge(a => a.EventCount)
    })
    .ToListAsync();
```

```sql
SELECT "Date", "Category",
       sumMerge("TotalAmount"),
       uniqMerge("UniqueUsers"),
       countMerge("EventCount")
FROM "DailyAggregates"
GROUP BY "Date", "Category"
```

> **Note:** Always use `-Merge` combinators when querying AggregatingMergeTree tables. The raw `byte[]` columns contain opaque binary state that is not human-readable.

## Complete Example

```csharp
modelBuilder.Entity<TrafficAggregates>(entity =>
{
    entity.ToTable("traffic_aggregates");
    entity.HasNoKey();

    entity.UseAggregatingMergeTree(x => new { x.Hour, x.Domain })
        .HasPartitionByMonth(x => x.Hour);

    entity.Property(x => x.Hits).HasAggregateFunction("sum", typeof(long));
    entity.Property(x => x.UniqueVisitors).HasAggregateFunction("uniqExact", typeof(Guid));
    entity.Property(x => x.MedianLatency).HasAggregateFunctionRaw("quantile(0.5)", "Float64");
});
```

```sql
CREATE TABLE "traffic_aggregates" (
    "Hour" DateTime64(3),
    "Domain" String,
    "Hits" AggregateFunction(sum, Int64),
    "UniqueVisitors" AggregateFunction(uniqExact, UUID),
    "MedianLatency" AggregateFunction(quantile(0.5), Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM("Hour")
ORDER BY ("Hour", "Domain")
```

## See Also

- [SummingMergeTree](summing-mergetree.md) -- simpler alternative when you only need sums
- [MergeTree](mergetree.md) -- base engine for raw data storage
- [Materialized Views](../features/materialized-views.md) -- the primary way to populate AggregatingMergeTree tables
