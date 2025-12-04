# EF.CH - Entity Framework Core Provider for ClickHouse

An Entity Framework Core provider for [ClickHouse](https://clickhouse.com/), built on the [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client) ADO.NET driver.

## Features

- **LINQ to ClickHouse SQL** - Full query translation with ClickHouse-specific optimizations
- **MergeTree Engine Family** - MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree
- **Rich Type System** - Arrays, Maps, Tuples, Nested types, Enums, IPv4/IPv6, DateTime64
- **Materialized Views** - LINQ-based and raw SQL definitions
- **EF Core Migrations** - DDL generation with ClickHouse-specific clauses
- **DELETE Support** - Lightweight and mutation-based strategies
- **Scaffolding** - Reverse engineering with C# enum generation

## Quick Start

```csharp
// 1. Install the package
// dotnet add package EF.CH

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
            entity.HasPartitionByMonth(x => x.OrderDate);         // Optional: partitioning
        });
    }
}

// 4. Use it
await using var context = new MyDbContext();
await context.Database.EnsureCreatedAsync();

context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    OrderDate = DateTime.UtcNow,
    CustomerId = "customer-123",
    Total = 99.99m
});
await context.SaveChangesAsync();

var recentOrders = await context.Orders
    .Where(o => o.OrderDate > DateTime.UtcNow.AddDays(-7))
    .ToListAsync();
```

## Installation

```bash
dotnet add package EF.CH
```

**Requirements:**
- .NET 10.0+
- ClickHouse 22.0+
- EF Core 10.0

## ClickHouse Concepts for EF Core Developers

If you're coming from SQL Server or PostgreSQL, ClickHouse works differently. Understanding these differences is essential:

### Every Table Needs an ENGINE

Unlike SQL Server where tables "just work", ClickHouse requires you to specify a table engine. The MergeTree family is most common:

```csharp
// This is REQUIRED - there's no default engine
entity.UseMergeTree(x => new { x.OrderDate, x.Id });
```

### No ACID Transactions

ClickHouse uses eventual consistency. `SaveChanges()` batches INSERTs but there's no rollback on failure. Design your application accordingly.

### No Row-Level UPDATE

ClickHouse doesn't support efficient `UPDATE` statements. Attempting to update an entity throws `NotSupportedException`. Instead:
- Use `ReplacingMergeTree` with a version column for "last write wins" semantics
- Use delete-and-reinsert patterns for infrequent updates

### Batch-Oriented, Not Row-at-a-Time

ClickHouse is optimized for bulk inserts (thousands of rows). Single-row inserts work but aren't efficient. Batch your writes when possible.

### No Auto-Increment

There's no `IDENTITY` or auto-increment. Use `Guid` or application-generated IDs:

```csharp
public Guid Id { get; set; } = Guid.NewGuid();
```

### No Foreign Key Enforcement

ClickHouse doesn't enforce referential integrity. Foreign keys are your application's responsibility.

## Table Engines

Choose the right engine for your use case:

| Engine | Use Case | Configuration |
|--------|----------|---------------|
| **MergeTree** | General purpose, append-only | `entity.UseMergeTree(x => x.Id)` |
| **ReplacingMergeTree** | Deduplication by key with version | `entity.UseReplacingMergeTree(x => x.Version, x => x.Id)` |
| **SummingMergeTree** | Auto-sum numeric columns | `entity.UseSummingMergeTree(x => new { x.Date, x.ProductId })` |
| **AggregatingMergeTree** | Pre-aggregated state | `entity.UseAggregatingMergeTree(x => x.Key)` |
| **CollapsingMergeTree** | Row cancellation with sign | `entity.UseCollapsingMergeTree(x => x.Sign, x => x.Key)` |
| **VersionedCollapsingMergeTree** | Out-of-order row cancellation | `entity.UseVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => x.Key)` |

See [docs/engines/](docs/engines/) for detailed documentation on each engine.

## Type Mappings

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `int`, `long`, `short`, `sbyte` | `Int32`, `Int64`, `Int16`, `Int8` |
| `uint`, `ulong`, `ushort`, `byte` | `UInt32`, `UInt64`, `UInt16`, `UInt8` |
| `float`, `double` | `Float32`, `Float64` |
| `decimal` | `Decimal(18, 4)` |
| `string` | `String` |
| `bool` | `Bool` |
| `Guid` | `UUID` |
| `DateTime` | `DateTime64(3)` |
| `DateTimeOffset` | `DateTime64(3)` with timezone |
| `DateOnly` | `Date` |
| `TimeOnly` | `Time` |
| `T[]`, `List<T>` | `Array(T)` |
| `Dictionary<K,V>` | `Map(K, V)` |
| `enum` | `Enum8` or `Enum16` (auto-selected) |

See [docs/types/](docs/types/) for the complete type mapping reference.

## Key Differences from SQL Server/PostgreSQL

| Feature | SQL Server/PostgreSQL | ClickHouse |
|---------|----------------------|------------|
| Transactions | Full ACID | Eventual consistency |
| UPDATE | Efficient row updates | Not supported - use ReplacingMergeTree |
| DELETE | Immediate | Lightweight (marks) or mutation (async rewrite) |
| Auto-increment | `IDENTITY`, `SERIAL` | Not available - use UUID |
| Foreign Keys | Enforced constraints | Application-level only |
| Indexes | B-tree, hash, etc. | Primary key (ORDER BY) only |
| Insert Pattern | Row-at-a-time OK | Batch thousands of rows |
| Use Case | OLTP | OLAP/Analytics |

## Table Options

```csharp
// Partitioning - improves query performance and data management
entity.HasPartitionByMonth(x => x.CreatedAt);  // PARTITION BY toYYYYMM()
entity.HasPartitionByDay(x => x.EventDate);    // PARTITION BY toYYYYMMDD()

// TTL - automatic data expiration
entity.HasTtl("CreatedAt + INTERVAL 90 DAY");

// Sampling - for approximate queries on large datasets
entity.HasSampleBy("intHash32(UserId)");
```

## DELETE Operations

```csharp
// Via change tracker (lightweight delete by default)
var entity = await context.Orders.FindAsync(id);
context.Orders.Remove(entity);
await context.SaveChangesAsync();

// Bulk delete
await context.Orders
    .Where(o => o.OrderDate < cutoffDate)
    .ExecuteDeleteAsync();

// Configure mutation-based delete
options.UseClickHouse("...", o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));
```

## Materialized Views

```csharp
// LINQ-based (type-safe)
modelBuilder.Entity<HourlySummary>(entity =>
{
    entity.UseSummingMergeTree(x => new { x.Hour, x.ProductId });
    entity.AsMaterializedView<HourlySummary, Order>(
        query: orders => orders
            .GroupBy(o => new { Hour = o.OrderDate.Date, o.ProductId })
            .Select(g => new HourlySummary
            {
                Hour = g.Key.Hour,
                ProductId = g.Key.ProductId,
                OrderCount = g.Count(),
                TotalRevenue = g.Sum(o => o.Total)
            }),
        populate: false);
});
```

## Documentation

| Topic | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Installation and first project |
| [ClickHouse Concepts](docs/clickhouse-concepts.md) | Key differences from RDBMS |
| [Table Engines](docs/engines/) | MergeTree family guide |
| [Type Mappings](docs/types/) | Complete type reference |
| [Features](docs/features/) | Materialized views, partitioning, TTL, etc. |
| [Migrations](docs/migrations.md) | EF Core migrations with ClickHouse |
| [Scaffolding](docs/scaffolding.md) | Reverse engineering |
| [Limitations](docs/limitations.md) | What doesn't work |

## Samples

| Sample | Description |
|--------|-------------|
| [QuickStartSample](samples/QuickStartSample/) | Minimal working example |
| [MigrationSample](samples/MigrationSample/) | EF Core migrations |
| [KeylessSample](samples/KeylessSample/) | Keyless entities for append-only data |
| [ReplacingMergeTreeSample](samples/ReplacingMergeTreeSample/) | Deduplication patterns |
| [SummingMergeTreeSample](samples/SummingMergeTreeSample/) | Auto-aggregation with SummingMergeTree |
| [CollapsingMergeTreeSample](samples/CollapsingMergeTreeSample/) | Row cancellation with sign column |
| [MaterializedViewSample](samples/MaterializedViewSample/) | Real-time aggregation |
| [ArrayTypeSample](samples/ArrayTypeSample/) | Working with arrays |
| [MapTypeSample](samples/MapTypeSample/) | Working with Map(K, V) dictionaries |
| [EnumTypeSample](samples/EnumTypeSample/) | ClickHouse enum type mapping |
| [PartitioningSample](samples/PartitioningSample/) | Table partitioning strategies |
| [QueryModifiersSample](samples/QueryModifiersSample/) | Final(), Sample(), WithSettings() |
| [DeleteStrategiesSample](samples/DeleteStrategiesSample/) | Lightweight vs mutation deletes |
| [OptimizeTableSample](samples/OptimizeTableSample/) | Programmatic OPTIMIZE TABLE |

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- [ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client) - The ADO.NET driver this provider builds on
- [EntityFrameworkCore.ClickHouse](https://github.com/denis-ivanov/EntityFrameworkCore.ClickHouse) - Reference implementation
