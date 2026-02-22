# Engines Sample

Demonstrates 5 ClickHouse table engine types supported by EF.CH.

## Engines Covered

1. **MergeTree** - Basic columnar storage with ORDER BY and PARTITION BY. The most common engine for analytics workloads.

2. **ReplacingMergeTree** - Deduplicates rows by ORDER BY key, keeping the row with the highest version column. Use `.Final()` to see deduplicated results at query time.

3. **SummingMergeTree** - Automatically sums numeric columns for rows that share the same ORDER BY key during background merges. Ideal for counters and pre-aggregated metrics.

4. **AggregatingMergeTree** - Stores intermediate aggregate function states (binary blobs) that can be finalized at query time with `-Merge` functions. Typically used as the target table for materialized views.

5. **CollapsingMergeTree** - Tracks state changes using a sign column (+1 for inserts, -1 for cancellations). During merges, pairs of rows with opposite signs and matching keys are removed.

## Prerequisites

- Docker (for ClickHouse)
- .NET 8.0 SDK

## Running

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest

# Run the sample
dotnet run --project samples/EnginesSample/

# Or with a custom connection string
dotnet run --project samples/EnginesSample/ -- "Host=localhost;Port=8123;Database=default"
```

## Key Concepts

- Tables are created with raw SQL via `ExecuteSqlRawAsync` and cleaned up after each demo
- `await Task.Delay(500)` is used after `OPTIMIZE TABLE` to allow ClickHouse time to complete background merges
- `.Final()` forces on-the-fly deduplication for ReplacingMergeTree queries
- `OptimizeTableFinalAsync<T>()` triggers a forced merge to see aggregated/collapsed results immediately
