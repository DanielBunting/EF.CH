// -----------------------------------------------------------------
// DictionarySample - ClickHouse Dictionary Features with EF.CH
// -----------------------------------------------------------------
// Demonstrates:
//   1. Table-backed dictionaries (AsDictionary with FromTable)
//   2. Dictionary query API (Get, GetOrDefault, ContainsKey)
//   3. External PostgreSQL-sourced dictionaries
//   4. Layout comparison (Hashed vs Flat)
// -----------------------------------------------------------------

using EF.CH.Dictionaries;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// Start ClickHouse container
var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.");

try
{
    var connectionString = container.GetConnectionString();

    try
    {
        await DemoTableBackedDictionary(connectionString);
        await DemoDictionaryQueryApi(connectionString);
    }
    catch (Exception ex) when (ex.Message.Contains("Authentication") || ex.Message.Contains("AUTHENTICATION"))
    {
        Console.WriteLine($"\n  Dictionary query failed: {ex.Message.Split('\n')[0]}");
        Console.WriteLine("  Note: ClickHouse 26+ requires explicit credentials for dictionary source access.");
        Console.WriteLine("  Table-backed dictionaries work correctly with standard ClickHouse deployments");
        Console.WriteLine("  where authentication is configured. See the test suite for verified examples.\n");
    }

    DemoExternalPostgresDictionary();
    DemoLayoutComparison();
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// -----------------------------------------------------------------
// Demo 1: Table-Backed Dictionary
// -----------------------------------------------------------------
static async Task DemoTableBackedDictionary(string connectionString)
{
    Console.WriteLine("\n=== 1. Table-Backed Dictionary ===\n");

    await using var context = new DictionaryDemoContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    // Seed the source table with country data
    await context.BulkInsertAsync(new List<Country>
    {
        new() { Id = 1, Name = "United States", IsoCode = "US", Region = "North America", IsActive = true },
        new() { Id = 2, Name = "Germany", IsoCode = "DE", Region = "Europe", IsActive = true },
        new() { Id = 3, Name = "Japan", IsoCode = "JP", Region = "Asia", IsActive = true },
        new() { Id = 4, Name = "Brazil", IsoCode = "BR", Region = "South America", IsActive = true },
        new() { Id = 5, Name = "Inactive Country", IsoCode = "XX", Region = "None", IsActive = false },
    });

    // Create the dictionary from the source table.
    // Table-backed dictionaries are created via migrations, but we can also
    // use EnsureAllDictionariesAsync to create them directly.
    await context.EnsureAllDictionariesAsync();

    Console.WriteLine("Table-backed dictionary created from 'countries' table.");
    Console.WriteLine("Only active countries are included (IsActive filter).");

    // Seed products that reference countries by ID
    await context.BulkInsertAsync(new List<Product>
    {
        new() { Id = 1, Name = "Widget A", Price = 29.99m, CountryId = 1 },
        new() { Id = 2, Name = "Gadget B", Price = 49.99m, CountryId = 2 },
        new() { Id = 3, Name = "Device C", Price = 99.99m, CountryId = 3 },
        new() { Id = 4, Name = "Tool D", Price = 19.99m, CountryId = 4 },
    });

    // Use the dictionary in a LINQ query via dictGet
    var dict = new ClickHouseDictionary<CountryLookup, ulong>(context);

    // Direct async access
    var countryName = await dict.GetAsync<string>(1, c => c.Name);
    Console.WriteLine($"Country ID 1: {countryName}");

    var isoCode = await dict.GetAsync<string>(2, c => c.IsoCode);
    Console.WriteLine($"Country ID 2 ISO: {isoCode}");

    // Check if a key exists
    var exists = await dict.ContainsKeyAsync(5);
    Console.WriteLine($"Country ID 5 exists: {exists} (filtered out because IsActive=false)");

    // Dictionary status
    var status = await dict.GetStatusAsync();
    if (status != null)
    {
        Console.WriteLine($"Dictionary status: {status.Status}");
        Console.WriteLine($"Element count: {status.ElementCount}");
    }
}

// -----------------------------------------------------------------
// Demo 2: Dictionary Query API
// -----------------------------------------------------------------
static async Task DemoDictionaryQueryApi(string connectionString)
{
    Console.WriteLine("\n=== 2. Dictionary Query API ===\n");

    await using var context = new DictionaryDemoContext(connectionString);

    var dict = new ClickHouseDictionary<CountryLookup, ulong>(context);

    // Get: retrieves an attribute value by key
    Console.WriteLine("--- dict.Get<T>(key, x => x.Attr) ---");
    var name = await dict.GetAsync<string>(1, c => c.Name);
    Console.WriteLine($"  Get(1, Name): {name}");

    // GetOrDefault: returns a default if the key is not found
    Console.WriteLine("\n--- dict.GetOrDefault<T>(key, x => x.Attr, default) ---");
    var unknownName = await dict.GetOrDefaultAsync<string>(
        999, c => c.Name, "Unknown Country");
    Console.WriteLine($"  GetOrDefault(999, Name, 'Unknown Country'): {unknownName}");

    var knownName = await dict.GetOrDefaultAsync<string>(
        2, c => c.Name, "Unknown Country");
    Console.WriteLine($"  GetOrDefault(2, Name, 'Unknown Country'): {knownName}");

    // ContainsKey: checks if a key exists in the dictionary
    Console.WriteLine("\n--- dict.ContainsKey(key) ---");
    var hasKey1 = await dict.ContainsKeyAsync(1);
    var hasKey999 = await dict.ContainsKeyAsync(999);
    Console.WriteLine($"  ContainsKey(1): {hasKey1}");
    Console.WriteLine($"  ContainsKey(999): {hasKey999}");

    // Refresh: force reload from source
    Console.WriteLine("\n--- dict.RefreshAsync() ---");
    await dict.RefreshAsync();
    Console.WriteLine("  Dictionary refreshed from source table.");
}

// -----------------------------------------------------------------
// Demo 3: External PostgreSQL Source (Configuration Only)
// -----------------------------------------------------------------
static void DemoExternalPostgresDictionary()
{
    Console.WriteLine("\n=== 3. External PostgreSQL Dictionary (Configuration) ===\n");

    Console.WriteLine("External dictionaries source data from PostgreSQL, MySQL, HTTP, or Redis.");
    Console.WriteLine("They are NOT created during migrations (to avoid storing credentials).");
    Console.WriteLine("Instead, call context.EnsureDictionariesAsync() at startup.\n");

    Console.WriteLine("Configuration example (in OnModelCreating):");
    Console.WriteLine("""
      modelBuilder.Entity<CountryLookup>(entity =>
      {
          entity.AsDictionary<CountryLookup>(cfg => cfg
              .HasKey(x => x.Id)
              .FromPostgreSql(pg => pg
                  .FromTable("countries", schema: "public")
                  .Connection(c => c
                      .HostPort(env: "PG_HOSTPORT")
                      .Database(env: "PG_DATABASE")
                      .Credentials("PG_USER", "PG_PASSWORD"))
                  .Where("is_active = true")
                  .InvalidateQuery("SELECT max(updated_at) FROM countries"))
              .UseHashedLayout()
              .HasLifetime(minSeconds: 60, maxSeconds: 300)
              .HasDefault(x => x.Name, "Unknown"));
      });
    """);

    Console.WriteLine("At startup:");
    Console.WriteLine("  await context.EnsureDictionariesAsync();");
    Console.WriteLine("\nNote: Requires PostgreSQL running with the referenced table.");
}

// -----------------------------------------------------------------
// Demo 4: Layout Comparison
// -----------------------------------------------------------------
static void DemoLayoutComparison()
{
    Console.WriteLine("\n=== 4. Dictionary Layout Comparison ===\n");

    Console.WriteLine("ClickHouse supports several dictionary layouts:\n");

    Console.WriteLine("UseHashedLayout()");
    Console.WriteLine("  - Best for: Medium-sized lookups with integer keys");
    Console.WriteLine("  - Storage: Hash table in memory");
    Console.WriteLine("  - Key types: UInt64 (single key)");
    Console.WriteLine("  - Memory: Proportional to number of entries\n");

    Console.WriteLine("UseFlatLayout()");
    Console.WriteLine("  - Best for: Small dictionaries (< 500k entries) with dense integer keys");
    Console.WriteLine("  - Storage: Flat array indexed by key value");
    Console.WriteLine("  - Key types: UInt64 only, values 0..N");
    Console.WriteLine("  - Memory: Proportional to max key value (sparse = wasteful)\n");

    Console.WriteLine("UseDirectLayout()");
    Console.WriteLine("  - Best for: Dictionaries that must always query the source");
    Console.WriteLine("  - Storage: No local storage, queries source on every access");
    Console.WriteLine("  - Useful when data changes frequently\n");

    Console.WriteLine("UseCacheLayout()");
    Console.WriteLine("  - Best for: Large dictionaries where only a subset is accessed");
    Console.WriteLine("  - Storage: LRU cache of recently accessed entries");
    Console.WriteLine("  - Configurable cache size\n");

    Console.WriteLine("UseComplexKeyHashedLayout()");
    Console.WriteLine("  - Best for: Composite (multi-column) keys");
    Console.WriteLine("  - Storage: Hash table with composite key support");
}

// -----------------------------------------------------------------
// Entities
// -----------------------------------------------------------------

// Source table entity
public class Country
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public bool IsActive { get; set; }
}

// Dictionary entity (subset of Country, used for fast lookups)
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
}

// Product entity that references countries
public class Product
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public ulong CountryId { get; set; }
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class DictionaryDemoContext(string connectionString) : DbContext
{
    public DbSet<Country> Countries => Set<Country>();
    public DbSet<Product> Products => Set<Product>();
    public DbSet<CountryLookup> CountryLookups => Set<CountryLookup>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table: countries
        modelBuilder.Entity<Country>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("countries");
            entity.UseMergeTree(x => x.Id);
        });

        // Products table
        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("products");
            entity.UseMergeTree(x => x.Id);
        });

        // Dictionary: table-backed, sourced from countries table
        // Demonstrates:
        //   - HasKey: the dictionary lookup key
        //   - FromTable: projects from Country to CountryLookup, with a filter
        //   - UseHashedLayout: hash table storage
        //   - HasLifetime: auto-refresh interval (60-300 seconds)
        //   - HasDefault: fallback value for missing keys
        modelBuilder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable(
                    projection: c => new CountryLookup
                    {
                        Id = c.Id,
                        Name = c.Name,
                        IsoCode = c.IsoCode
                    },
                    filter: q => q.Where(c => c.IsActive))
                .UseHashedLayout()
                .HasLifetime(minSeconds: 60, maxSeconds: 300)
                .HasDefault(x => x.Name, "Unknown"));
        });
    }
}
