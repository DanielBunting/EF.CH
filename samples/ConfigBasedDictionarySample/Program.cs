using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Config-Based External Dictionary Sample
// ============================================================
// Demonstrates ClickHouse dictionaries defined in XML config files
// instead of via DDL commands. This approach stores credentials
// in ClickHouse server config rather than passing them through SQL,
// which is more secure for production deployments.
// ============================================================

Console.WriteLine("Config-Based External Dictionary Sample");
Console.WriteLine("=======================================\n");

Console.WriteLine("This sample demonstrates dictionaries defined in ClickHouse XML config");
Console.WriteLine("rather than created via SQL DDL. The key differences:\n");

PrintComparisonTable();

// Wait for services to be ready
Console.WriteLine("\nTo run this sample:");
Console.WriteLine("  1. Start services: docker compose up -d");
Console.WriteLine("  2. Wait for health checks (~30 seconds)");
Console.WriteLine("  3. Run this sample: dotnet run\n");

await using var context = new ConfigDictSampleContext();

try
{
    // ============================================================
    // Step 1: Verify dictionary status
    // ============================================================
    Console.WriteLine("--- Step 1: Checking Dictionary Status ---\n");

    var dictionaries = await context.Database
        .SqlQueryRaw<DictionaryInfo>("""
            SELECT
                name AS Name,
                status AS Status,
                element_count AS ElementCount,
                bytes_allocated AS BytesAllocated,
                last_exception AS LastException
            FROM system.dictionaries
            WHERE name IN ('countries', 'currencies')
            """)
        .ToListAsync();

    if (dictionaries.Count == 0)
    {
        Console.WriteLine("No dictionaries found. Make sure Docker services are running:");
        Console.WriteLine("  cd samples/ConfigBasedDictionarySample");
        Console.WriteLine("  docker compose up -d");
        Console.WriteLine("\nWait ~30 seconds for services to initialize, then try again.");
        return;
    }

    Console.WriteLine("Found dictionaries:\n");
    Console.WriteLine($"{"Name",-15} {"Status",-15} {"Elements",-12} {"Bytes",-15} {"Last Error"}");
    Console.WriteLine(new string('-', 80));

    foreach (var dict in dictionaries)
    {
        var error = string.IsNullOrEmpty(dict.LastException) ? "-" : dict.LastException[..Math.Min(30, dict.LastException.Length)];
        Console.WriteLine($"{dict.Name,-15} {dict.Status,-15} {dict.ElementCount,-12} {dict.BytesAllocated,-15} {error}");
    }

    // Check if dictionaries loaded successfully
    var failedDicts = dictionaries.Where(d => d.Status != "LOADED").ToList();
    if (failedDicts.Count > 0)
    {
        Console.WriteLine($"\nWarning: {failedDicts.Count} dictionary(ies) not loaded. Check PostgreSQL connection.");
        foreach (var dict in failedDicts)
        {
            if (!string.IsNullOrEmpty(dict.LastException))
            {
                Console.WriteLine($"  {dict.Name}: {dict.LastException}");
            }
        }
    }

    // ============================================================
    // Step 2: Create test data
    // ============================================================
    Console.WriteLine("\n--- Step 2: Creating Test Data ---\n");

    // Create orders table
    await context.Database.ExecuteSqlRawAsync("""
        CREATE TABLE IF NOT EXISTS orders (
            id UUID,
            country_id UInt64,
            currency_code String,
            amount Decimal(18, 2),
            order_date Date
        )
        ENGINE = MergeTree()
        ORDER BY (order_date, id)
        PARTITION BY toYYYYMM(order_date)
        """);

    // Clear existing data
    await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE orders");

    // Insert sample orders
    await context.Database.ExecuteSqlRawAsync("""
        INSERT INTO orders (id, country_id, currency_code, amount, order_date) VALUES
            (generateUUIDv4(), 1, 'USD', 1500.00, '2024-01-15'),
            (generateUUIDv4(), 1, 'USD', 2300.50, '2024-01-16'),
            (generateUUIDv4(), 3, 'GBP', 850.00, '2024-01-15'),
            (generateUUIDv4(), 4, 'EUR', 1200.00, '2024-01-17'),
            (generateUUIDv4(), 5, 'EUR', 950.75, '2024-01-17'),
            (generateUUIDv4(), 6, 'JPY', 150000.00, '2024-01-18'),
            (generateUUIDv4(), 7, 'AUD', 1800.00, '2024-01-18'),
            (generateUUIDv4(), 9, 'INR', 75000.00, '2024-01-19'),
            (generateUUIDv4(), 10, 'CNY', 8500.00, '2024-01-19'),
            (generateUUIDv4(), 2, 'CAD', 2100.00, '2024-01-20')
        """);

    Console.WriteLine("Inserted 10 sample orders.\n");

    // ============================================================
    // Step 3: LINQ queries with dictionary lookups
    // ============================================================
    Console.WriteLine("--- Step 3: LINQ Queries with Dictionary Lookups ---\n");

    // Query 1: Enrich orders with country and currency names (dictGet)
    Console.WriteLine("Query 1: Enrich orders with dictGet()\n");

    var enrichedOrders = await context.Orders
        .Select(o => new
        {
            o.Id,
            o.Amount,
            CountryName = context.CountryDict.Get(o.CountryId, c => c.Name),
            CountryRegion = context.CountryDict.Get(o.CountryId, c => c.Region),
            CurrencyName = context.CurrencyDict.Get(o.CurrencyCode, c => c.Name),
            CurrencySymbol = context.CurrencyDict.Get(o.CurrencyCode, c => c.Symbol)
        })
        .Take(5)
        .ToListAsync();

    Console.WriteLine($"{"Amount",-15} {"Country",-20} {"Region",-15} {"Currency",-20} {"Symbol"}");
    Console.WriteLine(new string('-', 80));

    foreach (var order in enrichedOrders)
    {
        Console.WriteLine($"{order.Amount,-15:N2} {order.CountryName,-20} {order.CountryRegion,-15} {order.CurrencyName,-20} {order.CurrencySymbol}");
    }

    // Query 2: Group by country region using dictionary lookup
    Console.WriteLine("\nQuery 2: Sales by region (GROUP BY with dictGet)\n");

    var salesByRegion = await context.Orders
        .GroupBy(o => context.CountryDict.Get(o.CountryId, c => c.Region))
        .Select(g => new
        {
            Region = g.Key,
            OrderCount = g.Count(),
            TotalSales = g.Sum(o => o.Amount)
        })
        .OrderByDescending(x => x.TotalSales)
        .ToListAsync();

    Console.WriteLine($"{"Region",-20} {"Orders",-10} {"Total Sales"}");
    Console.WriteLine(new string('-', 50));

    foreach (var region in salesByRegion)
    {
        Console.WriteLine($"{region.Region,-20} {region.OrderCount,-10} {region.TotalSales:N2}");
    }

    // Query 3: Filter by country region using dictionary
    Console.WriteLine("\nQuery 3: European orders only (WHERE with dictGet)\n");

    var europeanOrders = await context.Orders
        .Where(o => context.CountryDict.Get(o.CountryId, c => c.Region) == "Europe")
        .Select(o => new
        {
            o.Amount,
            Country = context.CountryDict.Get(o.CountryId, c => c.Name),
            Currency = context.CurrencyDict.Get(o.CurrencyCode, c => c.Symbol)
        })
        .ToListAsync();

    Console.WriteLine($"{"Amount",-15} {"Country",-20} {"Currency"}");
    Console.WriteLine(new string('-', 50));

    foreach (var order in europeanOrders)
    {
        Console.WriteLine($"{order.Currency}{order.Amount,-14:N2} {order.Country,-20}");
    }

    // Query 4: Check key existence (dictHas)
    Console.WriteLine("\nQuery 4: Filter with dictHas()\n");

    var validOrders = await context.Orders
        .Where(o => context.CountryDict.ContainsKey(o.CountryId))
        .CountAsync();

    Console.WriteLine($"Orders with valid country IDs: {validOrders}");

    // Query 5: Default values for missing keys (dictGetOrDefault)
    Console.WriteLine("\nQuery 5: Handle missing keys with dictGetOrDefault()\n");

    var ordersWithDefaults = await context.Orders
        .Select(o => new
        {
            o.Amount,
            // Use GetOrDefault to provide fallback for missing dictionary entries
            CountryName = context.CountryDict.GetOrDefault(o.CountryId, c => c.Name, "Unknown Country"),
            CurrencySymbol = context.CurrencyDict.GetOrDefault(o.CurrencyCode, c => c.Symbol, "?")
        })
        .Take(3)
        .ToListAsync();

    Console.WriteLine($"{"Amount",-15} {"Country",-25} {"Symbol"}");
    Console.WriteLine(new string('-', 50));

    foreach (var order in ordersWithDefaults)
    {
        Console.WriteLine($"{order.Amount,-15:N2} {order.CountryName,-25} {order.CurrencySymbol}");
    }

    // ============================================================
    // Step 4: Dictionary management
    // ============================================================
    Console.WriteLine("\n--- Step 4: Dictionary Management ---\n");

    // Get detailed status using fluent API
    var countryStatus = await context.CountryDict.GetStatusAsync();
    if (countryStatus != null)
    {
        Console.WriteLine("Countries dictionary status:");
        Console.WriteLine($"  Status: {countryStatus.Status}");
        Console.WriteLine($"  Element count: {countryStatus.ElementCount}");
        Console.WriteLine($"  Memory used: {countryStatus.BytesAllocated:N0} bytes");
        Console.WriteLine($"  Last update: {countryStatus.LastSuccessfulUpdateTime}");
    }

    // Demonstrate refresh using fluent API
    Console.WriteLine("\nRefreshing dictionaries...");
    await context.CountryDict.RefreshAsync();
    await context.CurrencyDict.RefreshAsync();
    Console.WriteLine("Dictionaries refreshed successfully.");
}
catch (Exception ex)
{
    Console.WriteLine($"\nError: {ex.Message}");
    Console.WriteLine("\nMake sure Docker services are running:");
    Console.WriteLine("  cd samples/ConfigBasedDictionarySample");
    Console.WriteLine("  docker compose up -d");
}

Console.WriteLine("\n--- Sample Complete ---\n");

// ============================================================
// Helper Methods
// ============================================================

static void PrintComparisonTable()
{
    Console.WriteLine($"{"Aspect",-25} {"DDL-Based",-30} {"Config-Based (this sample)"}");
    Console.WriteLine(new string('-', 90));
    Console.WriteLine($"{"Definition location",-25} {"SQL migrations / app startup",-30} {"ClickHouse XML config"}");
    Console.WriteLine($"{"Credential storage",-25} {"Environment vars / app config",-30} {"ClickHouse server config"}");
    Console.WriteLine($"{"Credential exposure",-25} {"May appear in SQL logs",-30} {"Never in SQL logs"}");
    Console.WriteLine($"{"Creation timing",-25} {"EnsureDictionariesAsync()",-30} {"ClickHouse startup"}");
    Console.WriteLine($"{"EF.CH query syntax",-25} {"dictGet(), dictHas()",-30} {"Identical"}");
    Console.WriteLine($"{"Best for",-25} {"Dynamic dictionaries, dev",-30} {"Production, secure deployments"}");
}

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Dictionary lookup entity for countries.
/// Maps to the 'countries' dictionary defined in XML config.
/// </summary>
public class Countries
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public ulong Population { get; set; }
    public bool IsActive { get; set; }
}

/// <summary>
/// Dictionary lookup entity for currencies.
/// Maps to the 'currencies' dictionary defined in XML config.
/// Uses string key (currency code).
/// </summary>
public class Currencies
{
    public string Code { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Symbol { get; set; } = string.Empty;
    public byte DecimalPlaces { get; set; }
}

/// <summary>
/// Orders table entity.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public ulong CountryId { get; set; }
    public string CurrencyCode { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateOnly OrderDate { get; set; }
}

/// <summary>
/// DTO for reading dictionary info from system.dictionaries.
/// </summary>
public record DictionaryInfo(
    string Name,
    string Status,
    ulong ElementCount,
    ulong BytesAllocated,
    string? LastException);

// ============================================================
// DbContext Definition
// ============================================================

public class ConfigDictSampleContext : DbContext
{
    // Orders table
    public DbSet<Order> Orders => Set<Order>();

    // Dictionary lookup entities (configured with HasNoKey)
    public DbSet<Countries> Countriess => Set<Countries>();
    public DbSet<Currencies> Currenciess => Set<Currencies>();

    // Dictionary accessors with explicit metadata
    // These use the constructor that takes DictionaryMetadata directly,
    // bypassing the need for AsDictionary() configuration since the
    // dictionaries are managed by ClickHouse config, not EF.CH.
    private ClickHouseDictionary<Countries, ulong>? _countryDict;
    private ClickHouseDictionary<Currencies, string>? _currencyDict;

    /// <summary>
    /// Countries dictionary accessor.
    /// The dictionary is defined in config/clickhouse/dictionaries.xml,
    /// not via SQL DDL. We use explicit metadata to tell EF.CH about it.
    /// </summary>
    public ClickHouseDictionary<Countries, ulong> CountryDict
        => _countryDict ??= new ClickHouseDictionary<Countries, ulong>(
            this,
            new DictionaryMetadata<Countries, ulong>(
                name: "countries",  // Must match the name in dictionaries.xml
                keyType: typeof(ulong),
                entityType: typeof(Countries),
                keyPropertyName: "Id"));

    /// <summary>
    /// Currencies dictionary accessor (string key).
    /// The dictionary is defined in config/clickhouse/dictionaries.xml.
    /// </summary>
    public ClickHouseDictionary<Currencies, string> CurrencyDict
        => _currencyDict ??= new ClickHouseDictionary<Currencies, string>(
            this,
            new DictionaryMetadata<Currencies, string>(
                name: "currencies",  // Must match the name in dictionaries.xml
                keyType: typeof(string),
                entityType: typeof(Currencies),
                keyPropertyName: "Code"));

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        // Connect to the ClickHouse instance from docker-compose
        options.UseClickHouse("Host=localhost;Database=default");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Orders table - this is managed by EF.CH
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.CountryId).HasColumnName("country_id");
            entity.Property(e => e.CurrencyCode).HasColumnName("currency_code");
            entity.Property(e => e.Amount).HasColumnName("amount");
            entity.Property(e => e.OrderDate).HasColumnName("order_date");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });

        // Dictionary entities - configured with dictionary annotations for LINQ translation.
        // The actual dictionaries exist in ClickHouse via XML config, we just need
        // entity types with proper annotations for the LINQ translator.
        modelBuilder.Entity<Countries>(entity =>
        {
            entity.HasNoKey();
            // Table name must match the dictionary name in ClickHouse config
            entity.ToTable("countries");
            // Mark as dictionary and set key columns for LINQ translation
            entity.HasAnnotation(ClickHouseAnnotationNames.Dictionary, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
            // Map properties to match dictionary attribute names
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.IsoCode).HasColumnName("iso_code");
            entity.Property(e => e.Region).HasColumnName("region");
            entity.Property(e => e.Population).HasColumnName("population");
            entity.Property(e => e.IsActive).HasColumnName("is_active");
        });

        modelBuilder.Entity<Currencies>(entity =>
        {
            entity.HasNoKey();
            // Table name must match the dictionary name in ClickHouse config
            entity.ToTable("currencies");
            // Mark as dictionary and set key columns for LINQ translation
            entity.HasAnnotation(ClickHouseAnnotationNames.Dictionary, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Code" });
            // Map properties to match dictionary attribute names
            entity.Property(e => e.Code).HasColumnName("code");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Symbol).HasColumnName("symbol");
            entity.Property(e => e.DecimalPlaces).HasColumnName("decimal_places");
        });
    }
}
