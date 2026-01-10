# EF.CH - Entity Framework Core Provider for ClickHouse

An Entity Framework Core provider for [ClickHouse](https://clickhouse.com/), built on the [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) ADO.NET driver.

## Features

- **LINQ to ClickHouse SQL** - Full query translation with ClickHouse-specific optimizations
- **MergeTree Engine Family** - MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree
- **Rich Type System** - Arrays, Maps, Tuples, Nested types, Enums, IPv4/IPv6, DateTime64, native JSON
- **Materialized Views & Projections** - Pre-aggregated data for fast analytics
- **EF Core Migrations** - DDL generation with ClickHouse-specific clauses
- **Dictionaries & External Entities** - In-memory lookups and federated queries to PostgreSQL, MySQL, Redis
- **Query Modifiers** - FINAL, SAMPLE, PREWHERE, SETTINGS, window functions, gap filling

## Quick Start

```csharp
// 1. Install: dotnet add package EF.CH

// 2. Create your entity
public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public decimal Total { get; set; }
}

// 3. Create your DbContext
public class MyDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse("Host=localhost;Database=mydb");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });  // Required: ORDER BY
            entity.HasPartitionByMonth(x => x.OrderDate);         // Optional
        });
    }
}

// 4. Use it
await using var context = new MyDbContext();
await context.Database.EnsureCreatedAsync();

context.Orders.Add(new Order { Id = Guid.NewGuid(), OrderDate = DateTime.UtcNow, CustomerId = "c-123", Total = 99.99m });
await context.SaveChangesAsync();

var recentOrders = await context.Orders.Where(o => o.OrderDate > DateTime.UtcNow.AddDays(-7)).ToListAsync();
```

## Installation

```bash
dotnet add package EF.CH
```

**Requirements:** .NET 8.0+, ClickHouse 22.0+, EF Core 8.0+

## Key Differences from SQL Server/PostgreSQL

| Feature | SQL Server/PostgreSQL | ClickHouse |
|---------|----------------------|------------|
| Transactions | Full ACID | Eventual consistency |
| UPDATE | Efficient row updates | Not supported - use ReplacingMergeTree |
| DELETE | Immediate | Lightweight (marks) or mutation (async) |
| Auto-increment | `IDENTITY`, `SERIAL` | Not available - use UUID |
| Foreign Keys | Enforced constraints | Application-level only |
| Use Case | OLTP | OLAP/Analytics |

## Table Engines

| Engine | Use Case | Configuration |
|--------|----------|---------------|
| **MergeTree** | General purpose, append-only | `entity.UseMergeTree(x => x.Id)` |
| **ReplacingMergeTree** | Deduplication by key | `entity.UseReplacingMergeTree(x => x.Version, x => x.Id)` |
| **SummingMergeTree** | Auto-sum numeric columns | `entity.UseSummingMergeTree(x => x.Key)` |
| **AggregatingMergeTree** | Pre-aggregated state | `entity.UseAggregatingMergeTree(x => x.Key)` |
| **CollapsingMergeTree** | Row cancellation with sign | `entity.UseCollapsingMergeTree(x => x.Sign, x => x.Key)` |

See [docs/engines/](docs/engines/) for detailed documentation.

## Type Mappings

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `int`, `long`, `short` | `Int32`, `Int64`, `Int16` |
| `float`, `double`, `decimal` | `Float32`, `Float64`, `Decimal(18,4)` |
| `string`, `bool`, `Guid` | `String`, `Bool`, `UUID` |
| `DateTime`, `DateTimeOffset` | `DateTime64(3)` with optional timezone |
| `T[]`, `List<T>`, `Dictionary<K,V>` | `Array(T)`, `Map(K,V)` |
| `enum` | `Enum8` or `Enum16` (auto-selected) |
| `JsonElement` | `JSON` (ClickHouse 24.8+) |

See [docs/types/](docs/types/) for the complete reference.

## Documentation

| Topic | Description |
|-------|-------------|
| [Documentation Index](docs/index.md) | Central navigation hub |
| [Getting Started](docs/getting-started.md) | Installation and first project |
| [ClickHouse Concepts](docs/clickhouse-concepts.md) | Key differences from RDBMS |
| [Table Engines](docs/engines/overview.md) | MergeTree family guide |
| [Type Mappings](docs/types/overview.md) | Complete type reference |
| [Query Features](docs/features/query/) | Window functions, aggregates, gap filling |
| [Storage Optimization](docs/features/storage/) | Compression, indices, projections |
| [Schema Features](docs/features/schema/) | MVs, dictionaries, external entities |
| [Attributes Reference](docs/attributes-reference.md) | All configuration attributes |
| [Migrations](docs/migrations.md) | EF Core migrations with ClickHouse |
| [Limitations](docs/limitations.md) | What's not supported |
| [Troubleshooting](docs/troubleshooting.md) | FAQ and common issues |

## Samples

| Sample | Description |
|--------|-------------|
| [QuickStartSample](samples/QuickStartSample/) | Minimal working example |
| [ReplacingMergeTreeSample](samples/ReplacingMergeTreeSample/) | Deduplication patterns |
| [MaterializedViewSample](samples/MaterializedViewSample/) | Real-time aggregation |
| [QueryModifiersSample](samples/QueryModifiersSample/) | Final(), Sample(), PreWhere() |
| [DictionarySample](samples/DictionarySample/) | In-memory dictionary lookups |
| [ExternalPostgresSample](samples/ExternalPostgresSample/) | Federated queries |
| [JsonTypeSample](samples/JsonTypeSample/) | Native JSON with subcolumn queries |

See [samples/](samples/) for the complete list.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) - The ADO.NET driver this provider builds on
- [EntityFrameworkCore.ClickHouse](https://github.com/denis-ivanov/EntityFrameworkCore.ClickHouse) - Reference implementation
