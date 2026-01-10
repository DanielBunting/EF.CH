# EF.CH Documentation

Welcome to the EF.CH documentation - an Entity Framework Core provider for ClickHouse.

## Quick Links

| Topic | Description |
|-------|-------------|
| [Getting Started](getting-started.md) | Installation, first project, and configuration |
| [ClickHouse Concepts](clickhouse-concepts.md) | Key differences from SQL Server/PostgreSQL |
| [Limitations](limitations.md) | What's not supported |
| [Troubleshooting](troubleshooting.md) | FAQ and common issues |
| [Attributes Reference](attributes-reference.md) | Complete attribute documentation |

---

## Choose Your Path

### By Task

| I want to... | Start here |
|--------------|------------|
| Set up a new project | [Getting Started](getting-started.md) |
| Choose a table engine | [Engine Overview](engines/overview.md) |
| Handle mutable data | [ReplacingMergeTree](engines/replacing-mergetree.md) |
| Pre-aggregate data | [Materialized Views](features/schema/materialized-views.md) |
| Optimize query performance | [Query Modifiers](features/query/query-modifiers.md) |
| Reduce storage size | [Compression Codecs](features/storage/compression-codecs.md) |
| Query external databases | [External Entities](features/schema/external-entities.md) |
| Fill gaps in time series | [Interpolate](features/query/interpolate.md) |
| Use fast key-value lookups | [Dictionaries](features/schema/dictionaries.md) |

---

## Table Engines

ClickHouse tables require an ENGINE specification. Choose based on your use case:

| Engine | Use Case | Documentation |
|--------|----------|---------------|
| MergeTree | General purpose, append-only | [MergeTree](engines/mergetree.md) |
| ReplacingMergeTree | Deduplication, "last write wins" | [ReplacingMergeTree](engines/replacing-mergetree.md) |
| SummingMergeTree | Auto-sum numeric columns | [SummingMergeTree](engines/summing-mergetree.md) |
| AggregatingMergeTree | Pre-aggregated state columns | [AggregatingMergeTree](engines/aggregating-mergetree.md) |
| CollapsingMergeTree | Row cancellation with sign | [CollapsingMergeTree](engines/collapsing-mergetree.md) |
| VersionedCollapsingMergeTree | Out-of-order row cancellation | [VersionedCollapsing](engines/versioned-collapsing.md) |
| Null | Discard data (MV source) | [Null Engine](engines/null.md) |

See [Engine Overview](engines/overview.md) for a decision guide.

---

## Type Mappings

| Topic | Description |
|-------|-------------|
| [Overview](types/overview.md) | Complete .NET to ClickHouse type reference |
| [DateTime](types/datetime.md) | DateTime64, DateOnly, TimeOnly, timezone handling |
| [JSON](types/json.md) | Native JSON type with path extraction (ClickHouse 24.8+) |
| [Arrays](types/arrays.md) | Array operations and LINQ translation |
| [Maps](types/maps.md) | Map(K, V) dictionary type |
| [Enums](types/enums.md) | Enum8/Enum16 mapping |
| [Nested](types/nested.md) | Nested columnar structures |
| [IP Addresses](types/ip-addresses.md) | IPv4/IPv6 support |

---

## Feature Reference

### Query Features

LINQ extensions for ClickHouse-specific query capabilities.

| Feature | Description |
|---------|-------------|
| [Query Modifiers](features/query/query-modifiers.md) | FINAL, SAMPLE, PREWHERE, SETTINGS |
| [Window Functions](features/query/window-functions.md) | RowNumber, Rank, Lag, Lead, running totals |
| [Aggregate Combinators](features/query/aggregate-combinators.md) | -State, -Merge, -If for AggregatingMergeTree |
| [Gap Filling](features/query/interpolate.md) | WITH FILL and INTERPOLATE for time series |

### Storage Optimization

Physical data layout and compression settings.

| Feature | Description |
|---------|-------------|
| [Compression Codecs](features/storage/compression-codecs.md) | Per-column compression (DoubleDelta, Gorilla, ZSTD) |
| [Skip Indices](features/storage/skip-indices.md) | Minmax, bloom filter, token/ngram, set indices |
| [Projections](features/storage/projections.md) | Pre-sorted and pre-aggregated table-level optimizations |
| [Partitioning](features/storage/partitioning.md) | Monthly, daily, and custom partitioning |
| [TTL](features/storage/ttl.md) | Automatic data expiration |
| [Optimize](features/storage/optimize.md) | OPTIMIZE TABLE operations |

### Schema Features

DDL and advanced table structures.

| Feature | Description |
|---------|-------------|
| [Computed Columns](features/schema/computed-columns.md) | MATERIALIZED, ALIAS, DEFAULT expressions |
| [Materialized Views](features/schema/materialized-views.md) | INSERT triggers for real-time aggregation |
| [Dictionaries](features/schema/dictionaries.md) | In-memory key-value stores |
| [External Entities](features/schema/external-entities.md) | Query PostgreSQL, MySQL, Redis, ODBC |
| [Split Migrations](features/schema/split-migrations.md) | Atomic step-based migrations |

### Operations

DML and runtime behaviors.

| Feature | Description |
|---------|-------------|
| [Delete Operations](features/operations/delete-operations.md) | Lightweight vs mutation delete strategies |
| [Keyless Entities](features/operations/keyless-entities.md) | Append-only data without primary keys |
| [Low Cardinality](features/operations/low-cardinality.md) | Dictionary-encoded columns |
| [Default for Null](features/operations/default-for-null.md) | Avoid Nullable type overhead |

---

## Migrations & Scaffolding

| Topic | Description |
|-------|-------------|
| [Migrations](migrations.md) | EF Core migrations with ClickHouse |
| [Migration Phase Ordering](migration-phase-ordering.md) | Dependency ordering for safe DDL execution |
| [Scaffolding](scaffolding.md) | Reverse engineering with C# enum generation |

---

## Samples

Sample projects demonstrating each feature are available in the [samples/](../samples/) directory. See the [README](../README.md#samples) for a complete list.
