# Query Features Sample

Demonstrates ClickHouse-specific query extensions available through EF.CH LINQ integration.

## Features Covered

1. **FINAL** - On-the-fly deduplication for ReplacingMergeTree tables. Generates `FROM table FINAL`.

2. **SAMPLE** - Probabilistic sampling for approximate results on large datasets. Generates `SAMPLE 0.5`.

3. **PREWHERE** - Column-level pre-filtering that reads only filter columns first, then loads remaining columns for matching rows. Generates `PREWHERE condition`.

4. **LimitBy** - Top-N rows per group without window functions. Generates `LIMIT N BY column`. Useful for "top 3 per category" queries.

5. **CTE** - Common Table Expressions via `.AsCte("name")`. Generates `WITH "name" AS (...) SELECT ... FROM "name"`.

6. **Set Operations** - `.UnionAll()` and `.UnionDistinct()` for combining queries. ClickHouse requires explicit UNION ALL or UNION DISTINCT (bare UNION is not supported).

7. **GROUP BY Modifiers** - `.WithRollup()`, `.WithCube()`, and `.WithTotals()` add subtotal rows to GROUP BY results.

8. **Text Search** - Token-based search with `.ContainsToken()`, multi-term search with `.ContainsAny()`, and `EF.Functions.HasToken()` for direct DbFunctions usage.

## Prerequisites

- Docker (for ClickHouse)
- .NET 8.0 SDK

## Running

```bash
# Start ClickHouse
docker run -d --name clickhouse -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server:latest

# Run the sample
dotnet run --project samples/QueryFeaturesSample/

# Or with a custom connection string
dotnet run --project samples/QueryFeaturesSample/ -- "Host=localhost;Port=8123;Database=default"
```

## Key Concepts

- All query features are chainable LINQ extensions that generate ClickHouse-specific SQL
- PREWHERE is an optimization hint: it reads fewer columns for the initial filter pass
- SAMPLE requires a SAMPLE BY clause in the table definition
- Set operations in ClickHouse always need explicit ALL or DISTINCT qualifiers
- Text search functions like `hasToken()` use alphanumeric token boundaries, not substring matching
