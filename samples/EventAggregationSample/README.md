# Event Aggregation Sample

Demonstrates the **Null Engine + Materialized Views + TTL** pattern for high-volume event processing.

## The Problem

You have millions of raw events per day but:
- Storing all raw data is too expensive
- You only need aggregated data for dashboards and reports
- Different aggregation levels need different retention periods

## The Solution

Use a three-tier architecture:

1. **Null Engine table**: Receives raw events, immediately discards them
2. **Hourly aggregates**: Materialized view aggregates to hourly buckets, 1-year retention
3. **Daily aggregates**: Materialized view aggregates to daily buckets, 5-year retention

```
Raw Events ──► Null Table ──┬──► Hourly MV (1 year TTL)
               (discarded)  │
                            └──► Daily MV (5 years TTL)
```

## What This Sample Shows

| Feature | How It's Used |
|---------|---------------|
| **Null Engine** | `entity.UseNullEngine()` - data goes in, triggers MVs, nothing stored |
| **Materialized Views** | Auto-aggregate on INSERT |
| **ClickHouseInterval** | `ClickHouseInterval.Years(1)` for TTL |
| **SummingMergeTree** | Auto-sum aggregates on merge |
| **Partitioning** | Monthly (hourly) and yearly (daily) |

## Key Code

### Null Engine Configuration

```csharp
modelBuilder.Entity<RawEvent>(entity =>
{
    entity.UseNullEngine();  // No ORDER BY needed
});
```

### TTL with ClickHouseInterval

```csharp
// 1-year retention for hourly data
entity.HasTtl(x => x.Hour, ClickHouseInterval.Years(1));

// 5-year retention for daily data
entity.HasTtl(x => x.Date, ClickHouseInterval.Years(5));
```

### Materialized View

```csharp
entity.UseSummingMergeTree(x => new { x.Hour, x.Category });
entity.AsMaterializedViewRaw(
    sourceTable: "raw_events",
    selectSql: @"
        SELECT
            toStartOfHour(""Timestamp"") AS ""Hour"",
            ""Category"",
            count() AS ""EventCount"",
            sum(""Amount"") AS ""TotalAmount""
        FROM ""raw_events""
        GROUP BY ""Hour"", ""Category""
    ",
    populate: false);
```

## Prerequisites

- .NET 8.0+
- ClickHouse running on localhost:8123

```bash
# Start ClickHouse with Docker
docker run -d --name clickhouse-sample -p 8123:8123 clickhouse/clickhouse-server
```

## Running the Sample

```bash
cd samples/EventAggregationSample
dotnet run
```

## Expected Output

```
Event Aggregation Sample
========================

Creating database and tables...
Inserting 10,000 raw events...

Inserted 10000 events into Null table.
Raw events were discarded - Null engine stores nothing.

Raw events table count: 0 (always 0 with Null engine)

--- Hourly Summaries (last 24 hours) ---
  2024-01-15 14:00 | sales       :   145 events, $  72,341.50
  2024-01-15 14:00 | returns     :   132 events, $  65,890.25
  ...

--- Daily Summaries (all) ---
  2024-01-15       | sales       : 1,234 events, $ 617,500.00
  ...

--- Storage Analysis ---
Raw events inserted: 10,000
Raw events stored:   0 (Null engine)
Hourly aggregates:   168 rows
Daily aggregates:    28 rows
Storage reduction:   ~51x
```

## When to Use This Pattern

**Good for:**
- High-volume metrics/telemetry
- Event streams where only aggregates matter
- IoT sensor data
- Click streams and analytics

**Not good for:**
- Data that needs to be queried at row level
- Audit logs requiring full fidelity
- Data with complex, unpredictable query patterns

## Generated DDL

```sql
-- Raw events (Null engine - stores nothing)
CREATE TABLE "raw_events" (...)
ENGINE = Null

-- Hourly aggregates with 1-year TTL
CREATE TABLE "hourly_summary_mv" (...)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM("Hour")
ORDER BY ("Hour", "Category")
TTL "Hour" + INTERVAL 1 YEAR

-- Daily aggregates with 5-year TTL
CREATE TABLE "daily_summary_mv" (...)
ENGINE = SummingMergeTree
PARTITION BY toYear("Date")
ORDER BY ("Date", "Category")
TTL "Date" + INTERVAL 5 YEAR
```

## See Also

- [Null Engine Documentation](../../docs/engines/null.md)
- [TTL Documentation](../../docs/features/ttl.md)
- [Materialized Views](../../docs/features/materialized-views.md)
