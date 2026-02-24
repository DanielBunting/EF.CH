# Null Engine

The Null engine accepts inserts but discards all data immediately. SELECT queries against a Null table always return empty results. It functions like `/dev/null` for ClickHouse tables.

## Basic Configuration

```csharp
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.UseNullEngine();
});
```

```sql
CREATE TABLE "RawEvents" (
    "Id" UUID,
    "Timestamp" DateTime64(3),
    "Data" String
)
ENGINE = Null
```

> **Note:** The Null engine does not require ORDER BY, PARTITION BY, or any other MergeTree clauses. No parentheses are appended to the engine name.

## Materialized View Source Pattern

The primary use of the Null engine is as a source table for materialized views. Data is inserted into the Null table, triggers the materialized view logic, and then is discarded. Only the transformed/aggregated result is stored in the view's target table.

```csharp
// Source: raw events go here and are discarded
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.ToTable("events_raw");
    entity.HasNoKey();
    entity.UseNullEngine();
});

// Target: materialized view stores aggregated data
modelBuilder.Entity<HourlySummary>(entity =>
{
    entity.ToTable("hourly_summary");
    entity.HasNoKey();

    entity.UseSummingMergeTree(x => new { x.Hour, x.Category });

    entity.AsMaterializedView<HourlySummary, RawEvent>(
        query: events => events
            .GroupBy(e => new { Hour = e.Timestamp.Date, e.Category })
            .Select(g => new HourlySummary
            {
                Hour = g.Key.Hour,
                Category = g.Key.Category,
                EventCount = g.Count(),
                TotalAmount = g.Sum(e => e.Amount)
            }),
        populate: false
    );
});
```

```sql
CREATE TABLE "events_raw" (
    "Id" UUID,
    "Timestamp" DateTime64(3),
    "Category" String,
    "Amount" Int64
)
ENGINE = Null

CREATE MATERIALIZED VIEW "hourly_summary" ...
ENGINE = SummingMergeTree()
ORDER BY ("Hour", "Category")
AS SELECT ...
FROM "events_raw"
GROUP BY ...
```

This pattern saves storage because the raw events are never written to disk, while the aggregated data is stored efficiently in the SummingMergeTree table.

## Multiple Materialized Views

A single Null table can feed multiple materialized views, each computing a different aggregation:

```csharp
// Raw events -- discarded after triggering views
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.UseNullEngine();
});

// View 1: hourly counts
modelBuilder.Entity<HourlyCounts>(entity =>
{
    entity.UseSummingMergeTree(x => x.Hour);
    entity.AsMaterializedView<HourlyCounts, RawEvent>(...);
});

// View 2: unique users per day
modelBuilder.Entity<DailyUniques>(entity =>
{
    entity.UseAggregatingMergeTree(x => x.Date);
    entity.AsMaterializedView<DailyUniques, RawEvent>(...);
});
```

## When to Use

The Null engine is the right choice when:

- You only need transformed or aggregated data, not the raw input
- You want to minimize storage costs by discarding raw events
- You have one or more materialized views that process incoming data

It is not suitable when:

- You need to query the raw data directly
- You need to reprocess historical data (use MergeTree with a materialized view instead)

## See Also

- [MergeTree](mergetree.md) -- use when you need to keep raw data alongside materialized views
- [SummingMergeTree](summing-mergetree.md) -- common target engine for materialized views from Null tables
- [AggregatingMergeTree](aggregating-mergetree.md) -- for complex aggregate state storage from Null tables
