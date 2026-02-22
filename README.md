# EF.CH

**Entity Framework Core provider for ClickHouse**

[![NuGet Version](https://img.shields.io/nuget/v/EF.CH)](https://www.nuget.org/packages/EF.CH)
[![NuGet Downloads](https://img.shields.io/nuget/dt/EF.CH)](https://www.nuget.org/packages/EF.CH)
[![Build](https://github.com/DanielBunting/EF.CH/actions/workflows/dotnet.yml/badge.svg)](https://github.com/DanielBunting/EF.CH/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

EF.CH brings ClickHouse into the Entity Framework Core ecosystem. Define your models in C#, write LINQ queries, and run migrations -- all against ClickHouse's columnar analytics engine.

---

## Quick Start

```bash
dotnet add package EF.CH
```

```csharp
using Microsoft.EntityFrameworkCore;
using EF.CH.Extensions;

public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class AppDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse("Host=localhost;Port=8123;Database=default");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(e => new { e.Timestamp, e.EventType })
                .HasPartitionByMonth(e => e.Timestamp);
        });
    }
}

// Insert
await using var db = new AppDbContext();
await db.Database.EnsureCreatedAsync();

db.Events.Add(new Event
{
    Id = Guid.NewGuid(),
    Timestamp = DateTime.UtcNow,
    EventType = "purchase",
    Amount = 99.95m
});
await db.SaveChangesAsync();

// Query
var summary = await db.Events
    .GroupBy(e => e.EventType)
    .Select(g => new { Type = g.Key, Total = g.Sum(e => e.Amount) })
    .ToListAsync();
```

---

## Feature Highlights

| Category | Highlights | Docs |
|---|---|---|
| **Engines** | 14 table engines: MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree, VersionedCollapsingMergeTree, Replicated variants, Distributed, Null | [docs/engines/](docs/engines/) |
| **Type System** | Arrays, Maps, Tuples, Nested, JSON, IPv4/IPv6, Enums, DateTime64 with timezones, LowCardinality, 40+ type mappings | [docs/types/](docs/types/) |
| **Query Power** | FINAL, SAMPLE, PREWHERE, LimitBy, CTEs, Window Functions, Set Operations, Interpolate, 22+ LINQ extensions | [docs/querying/](docs/querying/) |
| **Analytics** | 66+ aggregate functions, materialized views, projections, aggregate combinators (-If, -Array, -State, -Merge) | [docs/functions/](docs/functions/), [docs/advanced/](docs/advanced/) |
| **Data Operations** | Bulk insert, INSERT...SELECT, export to CSV/JSON/Parquet, temp tables | [docs/data-operations/](docs/data-operations/) |
| **Enterprise** | Multi-DC clustering, connection routing, replicated engines, EF Core migrations with ClickHouse DDL | [docs/clustering/](docs/clustering/), [docs/migrations/](docs/migrations/) |

---

## ClickHouse for EF Core Developers

If you are coming from SQL Server or PostgreSQL, ClickHouse works differently in several fundamental ways. EF.CH bridges these gaps where possible and surfaces the differences clearly where it cannot.

| SQL Server / PostgreSQL | ClickHouse | EF.CH API |
|---|---|---|
| Table just works | Must specify ENGINE | `.UseMergeTree(x => ...)` |
| IDENTITY / SERIAL | No auto-increment | Guid / application-generated IDs |
| `UPDATE SET ... WHERE` | `ALTER TABLE UPDATE` (async) | `.ExecuteUpdateAsync()` |
| Transaction scope | No transactions | Design for idempotency |
| Foreign keys | None | Application-level joins |
| `COUNT(DISTINCT x)` | `uniq(x)` (approx) or `uniqExact(x)` | `.Uniq()` / `.UniqExact()` |
| Clustered index | ORDER BY in MergeTree | Engine ORDER BY expression |
| Row-level updates | Part-level merges | Background async processing |

See [ClickHouse for EF Developers](docs/clickhouse-for-ef-developers.md) for the full guide.

---

## Requirements

| Dependency | Minimum Version |
|---|---|
| .NET | 8.0+ |
| ClickHouse | 22.0+ |
| EF Core | 8.0+ |

**Runtime dependencies:** [ClickHouse.Driver](https://www.nuget.org/packages/ClickHouse.Driver) 0.9.0, [Microsoft.EntityFrameworkCore.Relational](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore.Relational) 8.0.13.

---

## Documentation

| Section | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation, first DbContext, connection strings |
| [ClickHouse for EF Developers](docs/clickhouse-for-ef-developers.md) | Conceptual differences from SQL Server / PostgreSQL |
| [Engines](docs/engines/) | MergeTree family, Replicated, Distributed, Null |
| [Types](docs/types/) | Arrays, Maps, Nested, JSON, IPv4/IPv6, Enums, DateTime |
| [Modeling](docs/modeling/) | Computed columns, codecs, LowCardinality, skip indices, TTL |
| [Querying](docs/querying/) | FINAL, SAMPLE, PREWHERE, LimitBy, CTEs, Window Functions |
| [Functions](docs/functions/) | 66+ aggregates, string, date, hash, IP, URL, encoding functions |
| [Data Operations](docs/data-operations/) | Bulk insert, INSERT...SELECT, export, temp tables |
| [Advanced](docs/advanced/) | Materialized views, projections, parameterized views, dictionaries |
| [Clustering](docs/clustering/) | Multi-node setup, connection routing, Distributed engine |
| [Migrations](docs/migrations/) | DDL generation, migration splitting, custom operations |
| [Scaffolding](docs/scaffolding.md) | Reverse-engineering existing ClickHouse databases |
| [Limitations](docs/limitations.md) | Known gaps and workarounds |

---

## Samples

All samples are standalone projects in the [`samples/`](samples/) directory. Build individually:

```bash
dotnet build samples/QuickStartSample/
```

### Getting Started

| Sample | Description |
|---|---|
| [QuickStartSample](samples/QuickStartSample/) | Minimal setup: DbContext, insert, query |
| [TypesSample](samples/TypesSample/) | ClickHouse type mappings: arrays, maps, enums, IPv4/IPv6 |
| [MigrationSample](samples/MigrationSample/) | EF Core migrations with ClickHouse DDL |

### Core Features

| Sample | Description |
|---|---|
| [EnginesSample](samples/EnginesSample/) | MergeTree family, ReplacingMergeTree, SummingMergeTree |
| [QueryFeaturesSample](samples/QueryFeaturesSample/) | FINAL, SAMPLE, PREWHERE, LimitBy, CTEs |
| [BulkOperationsSample](samples/BulkOperationsSample/) | High-throughput bulk insert and INSERT...SELECT |
| [MaterializedViewSample](samples/MaterializedViewSample/) | Materialized views with source and target tables |
| [DeleteUpdateSample](samples/DeleteUpdateSample/) | Lightweight deletes, ALTER TABLE UPDATE strategies |

### Advanced

| Sample | Description |
|---|---|
| [DictionarySample](samples/DictionarySample/) | ClickHouse dictionaries with EF Core |
| [ExternalEntitiesSample](samples/ExternalEntitiesSample/) | External tables and data sources in queries |
| [ClusterSample](samples/ClusterSample/) | Multi-node cluster with Distributed tables |
| [QueryProfilingSample](samples/QueryProfilingSample/) | Query profiling and performance analysis |
| [ParameterizedViewSample](samples/ParameterizedViewSample/) | Parameterized views with typed arguments |

### Real-World Patterns

| Sample | Description |
|---|---|
| [EventAnalyticsSample](samples/EventAnalyticsSample/) | End-to-end event analytics pipeline |
| [TempTableWorkflowSample](samples/TempTableWorkflowSample/) | Temp tables for complex multi-step queries |

---

## License

MIT. See [LICENSE](LICENSE) for details.

## Acknowledgments

Built by [Daniel Bunting](https://github.com/DanielBunting). Powered by [ClickHouse.Driver](https://github.com/pach1co/clickhouse-driver-csharp) and [Microsoft.EntityFrameworkCore](https://github.com/dotnet/efcore).
