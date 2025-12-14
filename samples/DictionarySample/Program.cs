using EF.CH.Dictionaries;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Dictionary Sample
// ============================================================
// Demonstrates ClickHouse dictionaries - in-memory key-value
// stores that provide fast lookups for reference data.
// ============================================================

Console.WriteLine("ClickHouse Dictionary Sample");
Console.WriteLine("============================\n");

await using var context = new DictionarySampleContext();

Console.WriteLine("Creating database, tables, and dictionaries...");

// Note: In production, you would use EnsureCreatedAsync which generates
// CREATE DICTIONARY statements. For this sample, we'll demonstrate
// the LINQ query patterns that translate to dictGet functions.

try
{
    await context.Database.EnsureCreatedAsync();
}
catch (Exception ex)
{
    Console.WriteLine($"Note: Database creation may require ClickHouse server. Error: {ex.Message}");
}

// ============================================================
// Dictionary Configuration Examples
// ============================================================

Console.WriteLine("\n--- Dictionary Configuration Examples ---\n");

Console.WriteLine("1. Basic Dictionary (Hashed Layout):");
Console.WriteLine("""
   entity.AsDictionary<CountryLookup, Country>(cfg => cfg
       .HasKey(x => x.Id)
       .FromTable()
       .UseHashedLayout()
       .HasLifetime(300));
   """);

Console.WriteLine("\n2. Dictionary with Composite Key:");
Console.WriteLine("""
   entity.AsDictionary<RegionPricing, PricingRule>(cfg => cfg
       .HasCompositeKey(x => new { x.Region, x.Category })
       .FromTable()
       .UseLayout(DictionaryLayout.ComplexKeyHashed)
       .HasLifetime(minSeconds: 60, maxSeconds: 600));
   """);

Console.WriteLine("\n3. Dictionary with Default Values:");
Console.WriteLine("""
   entity.AsDictionary<CountryLookup, Country>(cfg => cfg
       .HasKey(x => x.Id)
       .FromTable()
       .UseHashedLayout()
       .HasDefault(x => x.Name, "Unknown")
       .HasDefault(x => x.IsoCode, "XX"));
   """);

Console.WriteLine("\n4. Cache Layout (for large dictionaries):");
Console.WriteLine("""
   entity.AsDictionary<ProductLookup, Product>(cfg => cfg
       .HasKey(x => x.Id)
       .FromTable()
       .UseCacheLayout(opts => opts.SizeInCells = 50000));
   """);

Console.WriteLine("\n5. Flat Layout (for sequential IDs):");
Console.WriteLine("""
   entity.AsDictionary<StatusLookup, Status>(cfg => cfg
       .HasKey(x => x.Id)
       .FromTable()
       .UseFlatLayout(opts => opts.MaxArraySize = 1000));
   """);

// ============================================================
// LINQ Query Examples (translate to dictGet functions)
// ============================================================

Console.WriteLine("\n--- LINQ Query Examples ---\n");

Console.WriteLine("1. Basic dictGet in projection:");
Console.WriteLine("""
   // LINQ:
   var orders = db.Orders
       .Select(o => new {
           o.Id,
           CountryName = db.CountryDict.Get(o.CountryId, c => c.Name)
       });

   // Translates to SQL:
   // SELECT o.Id, dictGet('country_lookup', 'Name', o.CountryId) AS CountryName
   // FROM orders o
   """);

Console.WriteLine("\n2. dictGetOrDefault with fallback:");
Console.WriteLine("""
   // LINQ:
   var orders = db.Orders
       .Select(o => new {
           o.Id,
           CountryName = db.CountryDict.GetOrDefault(o.CountryId, c => c.Name, "Unknown")
       });

   // Translates to SQL:
   // SELECT o.Id, dictGetOrDefault('country_lookup', 'Name', o.CountryId, 'Unknown')
   // FROM orders o
   """);

Console.WriteLine("\n3. dictHas in WHERE clause:");
Console.WriteLine("""
   // LINQ:
   var validOrders = db.Orders
       .Where(o => db.CountryDict.ContainsKey(o.CountryId));

   // Translates to SQL:
   // SELECT * FROM orders o
   // WHERE dictHas('country_lookup', o.CountryId)
   """);

Console.WriteLine("\n4. Multiple dictionary lookups:");
Console.WriteLine("""
   // LINQ:
   var enrichedOrders = db.Orders
       .Select(o => new {
           o.Id,
           o.Amount,
           CountryName = db.CountryDict.Get(o.CountryId, c => c.Name),
           CountryCode = db.CountryDict.Get(o.CountryId, c => c.IsoCode),
           PriceMultiplier = db.RegionPricingDict.Get(
               (o.Region, o.Category),
               p => p.Multiplier)
       });
   """);

Console.WriteLine("\n5. Dictionary lookup with aggregation:");
Console.WriteLine("""
   // LINQ:
   var salesByCountry = db.Orders
       .GroupBy(o => db.CountryDict.Get(o.CountryId, c => c.Name))
       .Select(g => new {
           Country = g.Key,
           TotalSales = g.Sum(o => o.Amount)
       });

   // Translates to SQL:
   // SELECT dictGet('country_lookup', 'Name', o.CountryId) AS Country,
   //        sum(o.Amount) AS TotalSales
   // FROM orders o
   // GROUP BY Country
   """);

Console.WriteLine("\n6. Conditional logic with dictionary:");
Console.WriteLine("""
   // LINQ:
   var orders = db.Orders
       .Select(o => new {
           o.Id,
           CountryName = db.CountryDict.ContainsKey(o.CountryId)
               ? db.CountryDict.Get(o.CountryId, c => c.Name)
               : "Invalid Country"
       });
   """);

// ============================================================
// DDL Output Examples
// ============================================================

Console.WriteLine("\n--- Generated DDL Examples ---\n");

Console.WriteLine("Basic Hashed Dictionary:");
Console.WriteLine("""
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
   """);

Console.WriteLine("\nComposite Key Dictionary:");
Console.WriteLine("""
   CREATE DICTIONARY "region_pricing"
   (
       "Region" String,
       "Category" String,
       "Multiplier" Decimal(18, 4)
   )
   PRIMARY KEY ("Region", "Category")
   SOURCE(CLICKHOUSE(TABLE 'pricing_rule'))
   LAYOUT(COMPLEX_KEY_HASHED())
   LIFETIME(MIN 60 MAX 600)
   """);

Console.WriteLine("\nCache Layout Dictionary:");
Console.WriteLine("""
   CREATE DICTIONARY "product_lookup"
   (
       "Id" UInt64,
       "Name" String,
       "Price" Decimal(18, 4)
   )
   PRIMARY KEY "Id"
   SOURCE(CLICKHOUSE(TABLE 'product'))
   LAYOUT(CACHE(SIZE_IN_CELLS 50000))
   LIFETIME(300)
   """);

// ============================================================
// Best Practices
// ============================================================

Console.WriteLine("\n--- Best Practices ---\n");

Console.WriteLine("""
   1. Use Hashed layout for most use cases
      - Good balance of memory and performance
      - Supports any UInt64 key type

   2. Use ComplexKeyHashed for composite keys
      - Required when key is more than one column
      - Supports string keys

   3. Use Flat layout for sequential integer keys
      - Most memory efficient for dense ID ranges
      - Keys must be UInt64 starting near 0

   4. Use Cache layout for very large dictionaries
      - Only loads frequently accessed entries
      - Good for dictionaries with millions of rows

   5. Set appropriate LIFETIME values
      - Shorter for frequently changing data
      - Use HasNoAutoRefresh() for static data

   6. Define DEFAULT values for missing keys
      - Prevents NULL values in queries
      - Makes queries more predictable
   """);

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Source table for countries.
/// </summary>
public class Country
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public bool IsActive { get; set; } = true;
}

/// <summary>
/// Dictionary entity for country lookups.
/// Implements IClickHouseDictionary as a marker interface.
/// </summary>
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
}

/// <summary>
/// Source table for pricing rules.
/// </summary>
public class PricingRule
{
    public Guid Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Multiplier { get; set; }
    public DateTime EffectiveDate { get; set; }
}

/// <summary>
/// Dictionary entity for region pricing with composite key.
/// </summary>
public class RegionPricing : IClickHouseDictionary
{
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Multiplier { get; set; }
}

/// <summary>
/// Orders table that references dictionaries.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public ulong CountryId { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class DictionarySampleContext : DbContext
{
    // Source tables
    public DbSet<Country> Countries => Set<Country>();
    public DbSet<PricingRule> PricingRules => Set<PricingRule>();
    public DbSet<Order> Orders => Set<Order>();

    // Dictionary entities (used for configuration, dictGet translation)
    public DbSet<CountryLookup> CountryLookups => Set<CountryLookup>();
    public DbSet<RegionPricing> RegionPricings => Set<RegionPricing>();

    // Dictionary accessors for LINQ queries
    // Backing fields for lazy initialization
    private ClickHouseDictionary<CountryLookup, ulong>? _countryDict;
    private ClickHouseDictionary<RegionPricing, (string, string)>? _regionPricingDict;

    // Dictionary properties with runtime metadata discovery
    public ClickHouseDictionary<CountryLookup, ulong> CountryDict
        => _countryDict ??= new ClickHouseDictionary<CountryLookup, ulong>(this);

    public ClickHouseDictionary<RegionPricing, (string, string)> RegionPricingDict
        => _regionPricingDict ??= new ClickHouseDictionary<RegionPricing, (string, string)>(this);

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=dictionary_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Country source table
        modelBuilder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        // Country dictionary
        modelBuilder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseHashedLayout()
                .HasLifetime(300)
                .HasDefault(x => x.Name, "Unknown")
                .HasDefault(x => x.IsoCode, "XX"));
        });

        // Pricing rules source table
        modelBuilder.Entity<PricingRule>(entity =>
        {
            entity.ToTable("pricing_rule");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Region, x.Category, x.Id });
        });

        // Region pricing dictionary (composite key)
        modelBuilder.Entity<RegionPricing>(entity =>
        {
            entity.AsDictionary<RegionPricing, PricingRule>(cfg => cfg
                .HasCompositeKey(x => new { x.Region, x.Category })
                .FromTable()
                .UseLayout(DictionaryLayout.ComplexKeyHashed)
                .HasLifetime(minSeconds: 60, maxSeconds: 600)
                .HasDefault(x => x.Multiplier, 1.0m));
        });

        // Orders table
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
