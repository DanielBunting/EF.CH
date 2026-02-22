# Materialized View Sample

Demonstrates materialized views in ClickHouse via EF.CH. Materialized views automatically transform and aggregate data as it is inserted into source tables.

## Features Covered

1. **LINQ-based Materialized View** - Type-safe view definition using `AsMaterializedView<TTarget, TSource>()` with GroupBy/Select pipeline. The LINQ expression is translated to ClickHouse SQL at configuration time.

2. **Raw SQL Materialized View** - Full control over the SELECT transformation using `AsMaterializedViewRaw()`. Useful for ClickHouse-specific functions that cannot be expressed in LINQ.

3. **Null Engine Source** - The source table uses the Null engine (`UseNullEngine()`), which discards raw data after the materialized view processes it. Only the aggregated result in the MV target is retained. This pattern is ideal when you only need derived data.

4. **Populate Option** - The `POPULATE` keyword backfills the materialized view from existing source data at creation time. Without POPULATE, only data inserted after the view is created flows through.

## Pattern: Orders to DailySummary

The samples follow a common pattern:
- **Source table**: Raw event/order data (MergeTree or Null engine)
- **MV target table**: Aggregated summaries (SummingMergeTree)
- **Materialized view**: SQL transformation connecting source to target

Data flow: `INSERT INTO source` -> MV automatically runs `SELECT ... GROUP BY ...` -> result written to target.

## Prerequisites

- Docker (for ClickHouse)
- .NET 8.0 SDK

## Running

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest

# Run the sample
dotnet run --project samples/MaterializedViewSample/

# Or with a custom connection string
dotnet run --project samples/MaterializedViewSample/ -- "Host=localhost;Port=8123;Database=default"
```

## Key Concepts

- Materialized views in ClickHouse are triggers on INSERT, not stored query results
- The MV target table has its own engine (typically SummingMergeTree or AggregatingMergeTree)
- `OPTIMIZE TABLE FINAL` forces merges so aggregated values are visible immediately
- `await Task.Delay(500)` after OPTIMIZE allows ClickHouse time to complete background merges
- Null engine source tables keep zero data on disk -- only the MV target retains the aggregated data
