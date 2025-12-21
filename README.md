# EF.CH - Entity Framework Core Provider for ClickHouse

An Entity Framework Core provider for [ClickHouse](https://clickhouse.com/), built on the [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) ADO.NET driver.

## Features

- **LINQ to ClickHouse SQL** - Full query translation with ClickHouse-specific optimizations
- **MergeTree Engine Family** - MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, CollapsingMergeTree
- **Rich Type System** - Arrays, Maps, Tuples, Nested types, Enums, IPv4/IPv6, DateTime64
- **Materialized Views** - LINQ-based and raw SQL definitions
- **Projections** - Pre-sorted and pre-aggregated table-level optimizations
- **EF Core Migrations** - DDL generation with ClickHouse-specific clauses
- **DELETE Support** - Lightweight and mutation-based strategies
- **Dictionaries** - In-memory key-value stores with dictGet translation
- **External Entities** - Query PostgreSQL, MySQL, Redis, and ODBC sources via table functions
- **Scaffolding** - Reverse engineering with C# enum generation
- **Compression Codecs** - Per-column compression via fluent API and attributes
- **Window Functions** - Row numbering, ranking, lag/lead, running totals with fluent API
- **Data Skipping Indices** - Minmax, bloom filter, token/ngram bloom filters, and set indices
- **Time Series Gap Filling** - WITH FILL and INTERPOLATE for continuous time series data
- **Query Modifiers** - FINAL, SAMPLE, PREWHERE, and SETTINGS for query-level hints

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
| Indexes | B-tree, hash, etc. | Primary key (ORDER BY) + skip indices |
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

## Query Modifiers

ClickHouse-specific query hints via LINQ extension methods:

```csharp
using EF.CH.Extensions;

// FINAL - force deduplication for ReplacingMergeTree
var users = await context.Users
    .Final()
    .ToListAsync();

// SAMPLE - probabilistic sampling (~10% of rows)
var sample = await context.Events
    .Sample(0.1)
    .ToListAsync();

// PREWHERE - optimized pre-filtering (reads filter columns first)
var filtered = await context.Events
    .PreWhere(e => e.Date > cutoffDate)
    .ToListAsync();

// SETTINGS - query-level execution hints
var events = await context.Events
    .WithSetting("max_threads", 4)
    .ToListAsync();
```

**When to use PREWHERE:**
- Filter on indexed/sorted columns (ORDER BY key columns)
- Highly selective filters that eliminate most rows
- Large tables where I/O reduction matters

See [docs/features/query-modifiers.md](docs/features/query-modifiers.md) for full documentation.

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

## Window Functions

```csharp
using EF.CH.Extensions;

var analytics = context.Orders.Select(o => new
{
    o.Id,
    // Lambda style (recommended) - no .Value needed
    RowNum = Window.RowNumber(w => w
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)),

    PrevAmount = Window.Lag(o.Amount, 1, w => w
        .OrderBy(o.OrderDate)),

    RunningTotal = Window.Sum(o.Amount, w => w
        .PartitionBy(o.Region)
        .OrderBy(o.OrderDate)
        .Rows().UnboundedPreceding().CurrentRow())
});
```

**Available Functions:** `RowNumber`, `Rank`, `DenseRank`, `PercentRank`, `NTile`, `Lag`, `Lead`, `FirstValue`, `LastValue`, `NthValue`, `Sum`, `Avg`, `Count`, `Min`, `Max`

See [docs/features/window-functions.md](docs/features/window-functions.md) for full documentation including fluent API style.

## Time Series Gap Filling

Fill gaps in time series data with ClickHouse's `WITH FILL` and `INTERPOLATE` clauses:

```csharp
using EF.CH.Extensions;

// Basic gap filling - insert missing hourly rows
var hourlyData = context.Readings
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1));

// With FROM/TO bounds for complete date range
var fullRange = context.Readings
    .OrderBy(x => x.Date)
    .Interpolate(x => x.Date, TimeSpan.FromDays(1), startDate, endDate);

// Forward-fill values from previous row
var filledData = context.Readings
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1),
                 x => x.Value, InterpolateMode.Prev);

// Multiple columns with builder
var multiColumn = context.Readings
    .OrderBy(x => x.Hour)
    .Interpolate(x => x.Hour, TimeSpan.FromHours(1), i => i
        .Fill(x => x.Temperature, InterpolateMode.Prev)
        .Fill(x => x.Count, 0));
```

**Step Types:**

| Type | Use Case | Example |
|------|----------|---------|
| `TimeSpan` | Hours, minutes, seconds, days | `TimeSpan.FromHours(1)` |
| `ClickHouseInterval` | Months, quarters, years | `ClickHouseInterval.Months(1)` |
| `int` | Numeric sequences | `10` |

See [docs/features/interpolate.md](docs/features/interpolate.md) for full documentation.

## Data Skipping Indices

Skip indices allow ClickHouse to skip reading granules that don't match query predicates, dramatically improving query performance for selective filters.

```csharp
modelBuilder.Entity<LogEvent>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Minmax for datetime range queries
    entity.HasIndex(x => x.Timestamp)
        .UseMinmax()
        .HasGranularity(4);

    // Bloom filter for array membership (has(Tags, 'error'))
    entity.HasIndex(x => x.Tags)
        .UseBloomFilter(falsePositive: 0.025)
        .HasGranularity(3);

    // Token bloom filter for log search (LIKE '%exception%')
    entity.HasIndex(x => x.Message)
        .UseTokenBF(size: 10240, hashes: 3, seed: 0)
        .HasGranularity(4);

    // Set index for low-cardinality columns
    entity.HasIndex(x => x.Status)
        .UseSet(maxRows: 100)
        .HasGranularity(2);
});
```

**Or use attributes:**

```csharp
public class LogEvent
{
    [MinMaxIndex(Granularity = 4)]
    public DateTime Timestamp { get; set; }

    [BloomFilterIndex(FalsePositive = 0.025, Granularity = 3)]
    public string[] Tags { get; set; } = [];

    [TokenBFIndex(Granularity = 4)]
    public string Message { get; set; } = string.Empty;

    [SetIndex(MaxRows = 100, Granularity = 2)]
    public string Status { get; set; } = string.Empty;
}
```

**Index Types:**

| Type | Use Case |
|------|----------|
| `UseMinmax()` | Numeric/datetime range queries |
| `UseBloomFilter(fpp)` | Exact matching, array membership |
| `UseTokenBF(...)` | Tokenized text search (logs, URLs) |
| `UseNgramBF(...)` | Fuzzy/substring text matching |
| `UseSet(maxRows)` | Low-cardinality exact matching |

See [docs/features/skip-indices.md](docs/features/skip-indices.md) for full documentation.

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

## Projections

Projections are table-level optimizations stored alongside the main table data. Unlike materialized views, projections are **not separately queryable** - the query optimizer automatically uses them when beneficial.

```csharp
// Sort-order projection - auto-named: orders__prj_ord__customer_id__order_date
entity.HasProjection()
    .OrderBy(x => x.CustomerId)
    .ThenBy(x => x.OrderDate)
    .Build();

// Aggregation projection - explicit name, anonymous type
entity.HasProjection("daily_stats")
    .GroupBy(x => x.OrderDate.Date)
    .Select(g => new {
        Date = g.Key,
        TotalAmount = g.Sum(o => o.Amount),
        OrderCount = g.Count()
    })
    .Build();

// ClickHouse-specific aggregates (uniq, argMax, quantile, etc.)
entity.HasProjection("advanced_stats")
    .GroupBy(x => x.Date)
    .Select(g => new {
        Date = g.Key,
        UniqueUsers = ClickHouseAggregates.Uniq(g, o => o.UserId),
        TopProduct = ClickHouseAggregates.ArgMax(g, o => o.ProductId, o => o.Revenue)
    })
    .Build();
```

See [docs/features/projections.md](docs/features/projections.md) for full documentation including all ClickHouse aggregate functions.

## Dictionaries

ClickHouse dictionaries are in-memory key-value stores for fast lookups:

```csharp
// Define dictionary entity with marker interface
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

// Configure in OnModelCreating
entity.AsDictionary<CountryLookup, Country>(cfg => cfg
    .HasKey(x => x.Id)
    .FromTable()
    .UseHashedLayout()
    .HasLifetime(300));

// Use in LINQ queries - translates to dictGet()
var orders = db.Orders
    .Select(o => new {
        o.Id,
        CountryName = db.CountryDict.Get(o.CountryId, c => c.Name)
    });
```

**Layouts:** `Flat`, `Hashed`, `ComplexKeyHashed`, `Cache`, `Direct`

## External Entities

Query remote databases directly through ClickHouse table functions:

```csharp
// Define external entity (keyless - uses table function, not a ClickHouse table)
public class ExternalCustomer
{
    public int id { get; set; }
    public string name { get; set; } = string.Empty;
    public string email { get; set; } = string.Empty;
}

// Configure in OnModelCreating
modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
    .FromTable("customers", schema: "public")
    .Connection(c => c
        .HostPort(env: "PG_HOST")
        .Database(env: "PG_DATABASE")
        .Credentials("PG_USER", "PG_PASSWORD")));

// Query like any other entity - generates postgresql() table function
var customers = await context.ExternalCustomers
    .Where(c => c.name.StartsWith("A"))
    .ToListAsync();

// JOIN with native ClickHouse tables
var orderSummary = await context.Orders
    .Join(context.ExternalCustomers, o => o.CustomerId, c => c.id,
        (o, c) => new { c.name, o.Amount })
    .ToListAsync();
```

**Supported Providers:**

| Provider | Extension Method | Use Case |
|----------|------------------|----------|
| PostgreSQL | `ExternalPostgresEntity<T>()` | Direct credentials |
| MySQL | `ExternalMySqlEntity<T>()` | REPLACE INTO, ON DUPLICATE KEY |
| ODBC | `ExternalOdbcEntity<T>()` | SQL Server, Oracle via DSN |
| Redis | `ExternalRedisEntity<T>()` | Key-value with auto-generated schema |

See [docs/features/external-entities.md](docs/features/external-entities.md) for detailed configuration.

## Documentation

| Topic | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Installation and first project |
| [ClickHouse Concepts](docs/clickhouse-concepts.md) | Key differences from RDBMS |
| [Table Engines](docs/engines/) | MergeTree family guide |
| [Type Mappings](docs/types/) | Complete type reference |
| [Features](docs/features/) | Materialized views, partitioning, TTL, etc. |
| [Projections](docs/features/projections.md) | Table-level sort and aggregation optimizations |
| [Compression Codecs](docs/features/compression-codecs.md) | Per-column compression configuration |
| [Window Functions](docs/features/window-functions.md) | Ranking, lead/lag, running totals |
| [Data Skipping Indices](docs/features/skip-indices.md) | Bloom filter, minmax, set, and token indices |
| [Time Series Gap Filling](docs/features/interpolate.md) | WITH FILL and INTERPOLATE for continuous data |
| [Query Modifiers](docs/features/query-modifiers.md) | FINAL, SAMPLE, PREWHERE, SETTINGS query hints |
| [External Entities](docs/features/external-entities.md) | Query remote PostgreSQL, MySQL, Redis, ODBC |
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
| [QueryModifiersSample](samples/QueryModifiersSample/) | Final(), Sample(), PreWhere(), WithSettings() |
| [DeleteStrategiesSample](samples/DeleteStrategiesSample/) | Lightweight vs mutation deletes |
| [OptimizeTableSample](samples/OptimizeTableSample/) | Programmatic OPTIMIZE TABLE |
| [DictionarySample](samples/DictionarySample/) | In-memory dictionary lookups |
| [DictionaryJoinSample](samples/DictionaryJoinSample/) | Dictionaries as JOIN replacement |
| [ExternalPostgresSample](samples/ExternalPostgresSample/) | Query PostgreSQL from ClickHouse |
| [ExternalRedisSample](samples/ExternalRedisSample/) | Redis key-value integration |

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) - The ADO.NET driver this provider builds on
- [EntityFrameworkCore.ClickHouse](https://github.com/denis-ivanov/EntityFrameworkCore.ClickHouse) - Reference implementation
