# Dictionaries

ClickHouse dictionaries are in-memory key-value stores that provide fast lookups for reference data. They're ideal for dimension tables like countries, currencies, product categories, or any frequently-accessed lookup data.

## Why Use Dictionaries Instead of JOINs?

Dictionaries provide significant performance benefits over JOINs:

| Benefit | Description |
|---------|-------------|
| **In-memory lookups** | Data is cached in memory, no disk I/O |
| **No JOIN overhead** | No hash table construction at query time |
| **Automatic refresh** | Data stays fresh with configurable LIFETIME |
| **Distributed-friendly** | No cross-shard data shuffling |

Use dictionaries when:
- You have dimension/reference tables with < 10M rows
- Data is frequently joined to fact tables
- Lookup performance is critical

## Quick Start

```csharp
// 1. Define dictionary entity with marker interface
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
}

// 2. Configure in OnModelCreating
modelBuilder.Entity<CountryLookup>(entity =>
{
    entity.AsDictionary<CountryLookup, Country>(cfg => cfg
        .HasKey(x => x.Id)
        .FromTable()
        .UseHashedLayout()
        .HasLifetime(300));
});

// 3. Add dictionary accessor to DbContext
public class MyDbContext : DbContext
{
    private ClickHouseDictionary<CountryLookup, ulong>? _countryDict;

    public ClickHouseDictionary<CountryLookup, ulong> CountryDict
        => _countryDict ??= new ClickHouseDictionary<CountryLookup, ulong>(this);
}

// 4. Use in LINQ queries
var orders = db.Orders
    .Select(o => new {
        o.Id,
        CountryName = db.CountryDict.Get(o.CountryId, c => c.Name)
    });
// Generates: SELECT Id, dictGet('country_lookup', 'Name', CountryId) FROM orders
```

## Configuration

### Marker Interface

Dictionary entities must implement `IClickHouseDictionary`:

```csharp
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
```

### AsDictionary API

Configure dictionaries in `OnModelCreating`:

```csharp
entity.AsDictionary<TDictionary, TSource>(cfg => cfg
    .HasKey(x => x.Id)           // Required: primary key
    .FromTable()                  // Source is a ClickHouse table
    .UseHashedLayout()            // Memory layout
    .HasLifetime(300)             // Refresh interval in seconds
    .HasDefault(x => x.Name, "Unknown"));  // Default for missing keys
```

### Keys

**Single Key:**
```csharp
cfg.HasKey(x => x.Id)
```

**Composite Key:**
```csharp
cfg.HasCompositeKey(x => new { x.Region, x.Category })
```

## Layout Options

| Layout | Best For | Key Type |
|--------|----------|----------|
| `Flat` | Sequential UInt64 keys starting near 0 | UInt64 |
| `Hashed` | General purpose (recommended default) | UInt64 |
| `ComplexKeyHashed` | Composite keys or string keys | Any |
| `Cache` | Very large dictionaries (loads on demand) | Any |
| `Direct` | No caching, queries source directly | Any |

```csharp
// Hashed (default, recommended)
cfg.UseHashedLayout()

// Flat with options
cfg.UseFlatLayout(opts => opts.MaxArraySize = 100000)

// Cache with size
cfg.UseCacheLayout(opts => opts.SizeInCells = 50000)

// Complex key for composite keys
cfg.UseLayout(DictionaryLayout.ComplexKeyHashed)
```

## Lifetime

Control how often the dictionary refreshes from the source:

```csharp
// Fixed lifetime (refreshes every 300 seconds)
cfg.HasLifetime(300)

// Range lifetime (random refresh between min and max)
cfg.HasLifetime(minSeconds: 60, maxSeconds: 600)

// No auto-refresh (static data)
cfg.HasNoAutoRefresh()
```

## Default Values

Provide defaults for missing keys:

```csharp
cfg.HasDefault(x => x.Name, "Unknown")
   .HasDefault(x => x.IsoCode, "XX")
```

## DbContext Setup

Add dictionary accessor properties to your DbContext:

```csharp
public class MyDbContext : DbContext
{
    // Backing field for lazy initialization
    private ClickHouseDictionary<CountryLookup, ulong>? _countryDict;

    // Dictionary property - metadata resolved from OnModelCreating
    public ClickHouseDictionary<CountryLookup, ulong> CountryDict
        => _countryDict ??= new ClickHouseDictionary<CountryLookup, ulong>(this);

    // For composite keys, use tuple
    private ClickHouseDictionary<RegionPricing, (string, string)>? _pricingDict;

    public ClickHouseDictionary<RegionPricing, (string, string)> PricingDict
        => _pricingDict ??= new ClickHouseDictionary<RegionPricing, (string, string)>(this);
}
```

## LINQ Usage

### Get (dictGet)

```csharp
var orders = db.Orders
    .Select(o => new {
        o.Id,
        CountryName = db.CountryDict.Get(o.CountryId, c => c.Name)
    });
// → dictGet('country_lookup', 'Name', CountryId)
```

### GetOrDefault (dictGetOrDefault)

```csharp
var orders = db.Orders
    .Select(o => new {
        o.Id,
        CountryName = db.CountryDict.GetOrDefault(o.CountryId, c => c.Name, "Unknown")
    });
// → dictGetOrDefault('country_lookup', 'Name', CountryId, 'Unknown')
```

### ContainsKey (dictHas)

```csharp
// In WHERE clause
var validOrders = db.Orders
    .Where(o => db.CountryDict.ContainsKey(o.CountryId));
// → WHERE dictHas('country_lookup', CountryId)

// In projection
var orders = db.Orders
    .Select(o => new {
        o.Id,
        HasCountry = db.CountryDict.ContainsKey(o.CountryId)
    });
```

### Multiple Lookups

```csharp
var enrichedOrders = db.Orders
    .Select(o => new {
        o.Id,
        o.Amount,
        CountryName = db.CountryDict.Get(o.CountryId, c => c.Name),
        CountryCode = db.CountryDict.Get(o.CountryId, c => c.IsoCode),
        PriceMultiplier = db.PricingDict.Get((o.Region, o.Category), p => p.Multiplier)
    });
```

### JOINs with AsQueryable

For queries requiring actual JOINs with dictionaries (multiple attributes, filtering on dictionary columns, complex queries), use `AsQueryable()`:

```csharp
// Table → Dictionary: Enrich orders with all country data
var enrichedOrders = db.Orders
    .Join(
        db.CountryDict.AsQueryable(),
        o => o.CountryId,
        c => c.Id,
        (o, c) => new { o.Id, o.Amount, c.Name, c.IsoCode });

// Dictionary → Table: Find all products in a category by name
var products = db.CategoryDict.AsQueryable()
    .Where(c => c.Name == "Electronics")
    .Join(
        db.Products,
        c => c.CategoryId,
        p => p.CategoryId,
        (c, p) => new { p.ProductId, p.Name, c.Description });
```

Generated SQL uses `dictionary('name')` table function:
```sql
SELECT "o"."Id", "o"."Amount", "c"."Name", "c"."IsoCode"
FROM "orders" AS "o"
INNER JOIN dictionary('country_lookup') AS "c" ON "o"."CountryId" = "c"."Id"
```

**When to use each approach:**

| Pattern | Use Case |
|---------|----------|
| `.Get()` | Single attribute lookup in projection |
| `.GetOrDefault()` | Single attribute with fallback for missing keys |
| `.ContainsKey()` | Filter rows by key existence |
| `.AsQueryable()` | Multiple attributes, filtering on dictionary columns, complex queries |

## Direct Access (Async)

For access outside LINQ queries:

```csharp
// Get value
var name = await db.CountryDict.GetAsync(countryId, c => c.Name);

// Get with default
var name = await db.CountryDict.GetOrDefaultAsync(countryId, c => c.Name, "Unknown");

// Check existence
var exists = await db.CountryDict.ContainsKeyAsync(countryId);

// Force refresh
await db.CountryDict.RefreshAsync();

// Get status
var status = await db.CountryDict.GetStatusAsync();
```

## Generated DDL

EF Core migrations generate `CREATE DICTIONARY` statements:

```sql
CREATE DICTIONARY "country_lookup"
(
    "Id" UInt64,
    "Name" String DEFAULT 'Unknown',
    "IsoCode" String DEFAULT 'XX'
)
PRIMARY KEY "Id"
SOURCE(CLICKHOUSE(TABLE 'country'))
LAYOUT(HASHED())
LIFETIME(300)
```

## Best Practices

1. **Use Hashed layout for most cases** - Good balance of memory and performance

2. **Keep dictionaries small** - Only include columns you need

3. **Set appropriate LIFETIME** - Shorter for frequently changing data

4. **Define DEFAULT values** - Avoid NULL handling in application code

5. **Prefer GetOrDefault over ContainsKey + Get** - Single lookup instead of two

6. **Monitor memory usage** - Check `system.dictionaries` table

## See Also

- [DictionarySample](../../samples/DictionarySample/) - Basic dictionary usage
- [DictionaryJoinSample](../../samples/DictionaryJoinSample/) - Dictionary as JOIN replacement
- [ClickHouse Dictionary Documentation](https://clickhouse.com/docs/en/sql-reference/dictionaries)
