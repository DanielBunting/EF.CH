# EventAnalyticsSample

Demonstrates an end-to-end analytics pipeline that composes multiple ClickHouse features together: Null engine, materialized views, multiple MergeTree variants, projections, bulk insert, window functions, aggregate combinators, and data export.

## What This Shows

- **Null engine** as a materialized view source (data discarded after processing)
- **SummingMergeTree** for automatic page view aggregation
- **AggregatingMergeTree** with `-State` / `-Merge` combinators for revenue analytics
- **ReplacingMergeTree** with `.Final()` for latest user session state
- **Materialized views** that transform raw events into pre-aggregated summaries
- **Projections** for fast lookups by alternate sort order
- **BulkInsert** of 50,000 events with batch size tuning
- **Window functions** (DenseRank, running Sum) for trending page analysis
- **Export** to CSV via `ToCsvAsync`
- **Advanced aggregates**: TopK, Quantile, Median, GroupUniqArray

## Data Flow

```
RawEvent (Null engine)
    |
    |--- MV ---> PageViewSummary (SummingMergeTree)
    |                 |--- Projection: proj_by_page
    |
    |--- MV ---> RevenueSummary (AggregatingMergeTree)
    |                 |--- sumState(Revenue)
    |                 |--- uniqExactState(UserId)
    |
UserSession (ReplacingMergeTree)
         |--- .Final() for deduplicated reads
```

## API Reference

| Feature | API Used |
|---------|----------|
| Null engine | `entity.UseNullEngine()` |
| SummingMergeTree | `entity.UseSummingMergeTree(x => new { x.Date, x.Page })` |
| AggregatingMergeTree | `entity.UseAggregatingMergeTree(x => new { x.Date, x.EventType })` |
| ReplacingMergeTree | `entity.UseReplacingMergeTree(x => x.Version, x => new { x.UserId })` |
| Materialized view (raw) | `entity.AsMaterializedViewRaw(sourceTable, selectSql, populate)` |
| Projection | `entity.HasProjection("name").OrderBy(x => x.Col)` |
| AggregateFunction column | `property.HasAggregateFunction("sum", typeof(decimal))` |
| SimpleAggregateFunction | `property.HasSimpleAggregateFunction("sum")` |
| LowCardinality | `property.HasLowCardinality()` |
| Bulk insert | `context.BulkInsertAsync(entities, o => o.WithBatchSize(10_000))` |
| Window functions | `Window.DenseRank(w => w.PartitionBy(...).OrderBy(...))` |
| FINAL | `context.UserSessions.Final()` |
| -Merge combinator | `g.SumMerge<RevenueSummary, decimal>(r => r.RevenueState)` |
| CSV export | `query.ToCsvAsync(context)` |
| TopK | `g.TopK(5, p => p.Page)` |
| Quantile | `g.Quantile(0.90, p => (double)p.ViewCount)` |
| Median | `g.Median(p => (double)p.ViewCount)` |
| GroupUniqArray | `g.GroupUniqArray(p => p.Page)` |

## Prerequisites

- .NET 8.0+
- Docker (for running ClickHouse)

## Running

Start a local ClickHouse instance:

```bash
docker run -d --name clickhouse-analytics \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

Run the sample:

```bash
dotnet run
```

Stop ClickHouse when done:

```bash
docker stop clickhouse-analytics && docker rm clickhouse-analytics
```

## Expected Output

```
Event Analytics Pipeline Sample
===============================

Creating database, tables, and materialized views...
Setup complete.

--- Generating Events ---
Inserted 50,000 events into Null engine table.
Throughput: 250,000 rows/sec
Raw events stored: 0 (Null engine discards data)

Generating user session records...
Inserted 500 initial + 200 updated sessions.

--- Page View Analytics ---
Page view totals:
  /home            Views:    1,250  Visitors:  1,250
  /products        Views:    1,230  Visitors:  1,230
  ...

Trending pages (ranked by daily views with window functions):
  2026-02-22 | #1 /home            Views:     42  Running:    1,250
  ...

--- Revenue Analytics (-Merge Combinators) ---
Revenue by event type (finalized from aggregate states):
  purchase     Revenue: $  2,500,000.00  Unique Users:    500
  ...

--- User Session Analytics (FINAL) ---
Physical rows (before merge): 700
Deduplicated users (with FINAL): 500
Top 10 users by event count (deduplicated):
  user_0042:   680 events  (v2, 2025-12-01 to 2026-02-22)
  ...

--- Export Results (CSV) ---
Page view summary (CSV format, top 20 rows):
  "Date","Page","ViewCount","UniqueVisitors"
  ...

--- Advanced Aggregate Combinators ---
TopK - Most popular pages (top 5):
  /home
  /products
  /checkout
  /blog
  /about

Quantile - View count distribution:
  Median:      12
  90th pctile: 35
  99th pctile: 48

GroupArray - Pages seen per date (last 5 days):
  2026-02-22: 1,250 views across [/home, /products, /checkout, /about, /blog]
  ...

Done!
```

## Key Code

### Multi-Engine Pipeline Setup

```csharp
// Null engine: data flows through, discarded after MV processing
entity.UseNullEngine();

// SummingMergeTree: auto-sums numeric columns during merges
entity.UseSummingMergeTree(x => new { x.Date, x.Page });

// AggregatingMergeTree: stores intermediate aggregate states
entity.UseAggregatingMergeTree(x => new { x.Date, x.EventType });

// ReplacingMergeTree: keeps latest version per key
entity.UseReplacingMergeTree(x => x.Version, x => new { x.UserId });
```

### AggregateFunction Columns

```csharp
// Store intermediate state (binary, opaque)
entity.Property(e => e.RevenueState)
    .HasAggregateFunction("sum", typeof(decimal));

// Read with -Merge combinator to finalize
var revenue = g.SumMerge<RevenueSummary, decimal>(r => r.RevenueState);
var users = g.UniqExactMerge(r => r.UserCountState);
```

### Window Functions for Trending Analysis

```csharp
var trending = await context.PageViewSummaries
    .Select(p => new
    {
        p.Date,
        p.Page,
        DailyRank = Window.DenseRank(w => w
            .PartitionBy(p.Date)
            .OrderByDescending(p.ViewCount)),
        RunningTotal = Window.Sum(p.ViewCount, w => w
            .PartitionBy(p.Page)
            .OrderBy(p.Date))
    })
    .ToListAsync();
```

### FINAL for Deduplicated Reads

```csharp
var latestSessions = await context.UserSessions
    .Final()
    .OrderByDescending(u => u.EventCount)
    .ToListAsync();
```

## Learn More

- [Materialized Views](../../docs/features/materialized-views.md)
- [Aggregate Combinators](../../docs/features/aggregate-combinators.md)
- [Window Functions](../../docs/features/window-functions.md)
- [Bulk Insert](../../docs/features/bulk-insert.md)
