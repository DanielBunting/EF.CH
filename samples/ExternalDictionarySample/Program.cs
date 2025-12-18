using EF.CH.Dictionaries;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

// ============================================================
// External Dictionary Sample
// ============================================================
// Demonstrates ClickHouse dictionaries with external sources:
// - PostgreSQL database tables
// - MySQL database tables
// - HTTP endpoints (REST APIs)
//
// External dictionaries load reference data from external systems
// into ClickHouse's in-memory dictionary engine for fast lookups.
// ============================================================

Console.WriteLine("ClickHouse External Dictionary Sample");
Console.WriteLine("======================================\n");

// ============================================================
// Configuration Loading
// ============================================================
// External dictionaries resolve credentials at runtime from:
// 1. Environment variables (recommended for production)
// 2. IConfiguration (appsettings.json) profiles
// 3. Literal values (development/testing only)

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables()
    .Build();

Console.WriteLine("--- Configuration Examples ---\n");

// ============================================================
// 1. PostgreSQL Dictionary Source
// ============================================================

Console.WriteLine("1. PostgreSQL Dictionary Source:");
Console.WriteLine("""
   // Dictionary entity
   public class CountryLookup : IClickHouseDictionary
   {
       public ulong Id { get; set; }
       public string Name { get; set; } = string.Empty;
       public string IsoCode { get; set; } = string.Empty;
   }

   // Configuration (no source table needed - data comes from PostgreSQL)
   entity.AsDictionary<CountryLookup>(cfg => cfg
       .HasKey(x => x.Id)
       .FromPostgreSql(pg => pg
           .FromTable("countries", schema: "public")
           .Connection(c => c
               .HostPort(env: "PG_HOST")        // e.g., "pghost:5432"
               .Database(env: "PG_DATABASE")   // e.g., "mydb"
               .Credentials("PG_USER", "PG_PASSWORD"))
           .Where("is_active = true")
           .InvalidateQuery("SELECT max(updated_at) FROM countries"))
       .UseHashedLayout()
       .HasLifetime(minSeconds: 60, maxSeconds: 300)
       .HasDefault(x => x.Name, "Unknown"));
   """);

Console.WriteLine("\n   Generated DDL:");
Console.WriteLine("""
   CREATE DICTIONARY IF NOT EXISTS "country_lookup"
   (
       "Id" UInt64,
       "Name" String DEFAULT 'Unknown',
       "IsoCode" String
   )
   PRIMARY KEY "Id"
   SOURCE(POSTGRESQL(
       host 'pghost'
       port 5432
       user 'myuser'
       password 'mypassword'
       db 'mydb'
       table 'countries'
       schema 'public'
       where 'is_active = true'
       invalidate_query 'SELECT max(updated_at) FROM countries'
   ))
   LAYOUT(HASHED())
   LIFETIME(MIN 60 MAX 300)
   """);

// ============================================================
// 2. MySQL Dictionary Source
// ============================================================

Console.WriteLine("\n\n2. MySQL Dictionary Source:");
Console.WriteLine("""
   entity.AsDictionary<ProductLookup>(cfg => cfg
       .HasKey(x => x.ProductId)
       .FromMySql(mysql => mysql
           .FromTable("products")
           .Connection(c => c
               .Host(env: "MYSQL_HOST")
               .Port(value: 3306)
               .Database(env: "MYSQL_DATABASE")
               .Credentials("MYSQL_USER", "MYSQL_PASSWORD"))
           .Where("deleted_at IS NULL")
           .InvalidateQuery("SELECT MAX(updated_at) FROM products")
           .FailOnConnectionLoss(true))
       .UseHashedLayout()
       .HasLifetime(300));
   """);

Console.WriteLine("\n   Generated DDL:");
Console.WriteLine("""
   CREATE DICTIONARY IF NOT EXISTS "product_lookup"
   (
       "ProductId" Int32,
       "Name" String,
       "Price" Decimal(18, 4)
   )
   PRIMARY KEY "ProductId"
   SOURCE(MYSQL(
       host 'mysql-server'
       port 3306
       user 'root'
       password 'secret'
       db 'inventory'
       table 'products'
       where 'deleted_at IS NULL'
       invalidate_query 'SELECT MAX(updated_at) FROM products'
       fail_on_connection_loss 'true'
   ))
   LAYOUT(HASHED())
   LIFETIME(300)
   """);

// ============================================================
// 3. HTTP Dictionary Source
// ============================================================

Console.WriteLine("\n\n3. HTTP Dictionary Source:");
Console.WriteLine("""
   entity.AsDictionary<ExchangeRateLookup>(cfg => cfg
       .HasKey(x => x.CurrencyCode)
       .FromHttp(http => http
           .Url(env: "EXCHANGE_RATE_API_URL")  // e.g., "https://api.example.com/rates"
           .Format("JSONEachRow")
           .Credentials(userEnv: "API_USER", passwordEnv: "API_KEY")
           .Header("X-API-Version", "2.0")
           .HeaderFromEnv("Authorization", "API_AUTH_HEADER"))
       .UseLayout(DictionaryLayout.ComplexKeyHashed)
       .HasLifetime(minSeconds: 30, maxSeconds: 60));
   """);

Console.WriteLine("\n   Generated DDL:");
Console.WriteLine("""
   CREATE DICTIONARY IF NOT EXISTS "exchange_rate_lookup"
   (
       "CurrencyCode" String,
       "Rate" Float64,
       "UpdatedAt" DateTime64(3)
   )
   PRIMARY KEY "CurrencyCode"
   SOURCE(HTTP(
       url 'https://api.example.com/rates'
       format 'JSONEachRow'
       credentials(user 'apiuser' password 'apikey')
       headers('X-API-Version' '2.0', 'Authorization' 'Bearer token')
   ))
   LAYOUT(COMPLEX_KEY_HASHED())
   LIFETIME(MIN 30 MAX 60)
   """);

// ============================================================
// 4. Connection Profiles
// ============================================================

Console.WriteLine("\n\n4. Connection Profiles (appsettings.json):");
Console.WriteLine("""
   // appsettings.json:
   {
       "ExternalConnections": {
           "PostgresMain": {
               "HostPort": "pghost:5432",
               "Database": "mydb",
               "User": "dbuser",          // Direct value
               "PasswordEnv": "PG_PASSWORD" // From env var
           },
           "MySqlInventory": {
               "HostPort": "mysql:3306",
               "Database": "inventory",
               "UserEnv": "MYSQL_USER",
               "PasswordEnv": "MYSQL_PASSWORD"
           }
       }
   }

   // Usage with profiles:
   entity.AsDictionary<CountryLookup>(cfg => cfg
       .HasKey(x => x.Id)
       .FromPostgreSql(pg => pg
           .FromTable("countries")
           .Connection(c => c.UseProfile("PostgresMain")))
       .UseHashedLayout());
   """);

// ============================================================
// 5. Runtime Dictionary Creation
// ============================================================

Console.WriteLine("\n\n5. Runtime Dictionary Creation:");
Console.WriteLine("""
   // External dictionaries are NOT created in migrations because
   // they contain credentials. Instead, create them at runtime:

   // In Program.cs or application startup:
   await using var context = new MyDbContext(options);

   // Create all external dictionaries (safe to call multiple times)
   var count = await context.EnsureDictionariesAsync();
   Console.WriteLine($"Ensured {count} external dictionaries");

   // Or recreate dictionaries (drops and recreates)
   await context.RecreateDictionariesAsync();

   // Reload a specific dictionary from its source
   await context.ReloadDictionaryAsync<CountryLookup>();

   // Reload all dictionaries
   await context.ReloadAllDictionariesAsync();

   // Get DDL without executing (for debugging)
   var ddl = context.GetDictionaryDdl<CountryLookup>();
   Console.WriteLine(ddl);
   """);

// ============================================================
// 6. Credential Resolution Priority
// ============================================================

Console.WriteLine("\n\n6. Credential Resolution Priority:");
Console.WriteLine("""
   // Credentials are resolved at runtime in this order:
   //
   // 1. Literal values (if provided with value: parameter)
   //    .Host(value: "localhost")
   //
   // 2. Environment variables (if provided with env: parameter)
   //    .Host(env: "PG_HOST")  // reads Environment.GetEnvironmentVariable("PG_HOST")
   //
   // 3. IConfiguration profiles (if UseProfile is called)
   //    .Connection(c => c.UseProfile("PostgresMain"))
   //    // Reads from IConfiguration["ExternalConnections:PostgresMain:HostPort"]
   //
   // Profile values can themselves reference env vars:
   //    "PasswordEnv": "PG_PASSWORD"  // Profile tells us to read PG_PASSWORD env var
   """);

// ============================================================
// 7. Layout Options
// ============================================================

Console.WriteLine("\n\n7. Dictionary Layout Options:");
Console.WriteLine("""
   // Hashed - good default for most cases
   .UseHashedLayout()

   // Flat - best for sequential integer keys
   .UseFlatLayout(opts => opts.MaxArraySize = 100000)

   // Cache - for very large dictionaries (loads on demand)
   .UseCacheLayout(opts => opts.SizeInCells = 50000)

   // ComplexKeyHashed - for composite or string keys
   .UseLayout(DictionaryLayout.ComplexKeyHashed)

   // Range Hashed - for range lookups
   .UseLayout(DictionaryLayout.RangeHashed)
   """);

// ============================================================
// 8. Best Practices
// ============================================================

Console.WriteLine("\n\n8. Best Practices:");
Console.WriteLine("""
   1. Never store credentials in source code
      - Use environment variables for production
      - Use appsettings.json profiles with *Env suffixes

   2. Call EnsureDictionariesAsync() at application startup
      - Safe to call multiple times (uses IF NOT EXISTS)
      - Creates dictionaries with current credentials

   3. Use invalidate_query for auto-refresh
      - Dictionary checks this query to detect source changes
      - ClickHouse reloads data when query result changes

   4. Set appropriate LIFETIME values
      - Lower values for frequently changing data
      - Higher values reduce load on source systems

   5. Use HasDefault() for missing keys
      - Prevents NULL in query results
      - Provides predictable fallback values

   6. Choose the right layout
      - Hashed: general purpose, any key type
      - Flat: sequential UInt64 keys, most memory efficient
      - Cache: very large dictionaries, loads on demand
   """);

// ============================================================
// 9. Complete Working Example
// ============================================================

Console.WriteLine("\n\n9. Complete DbContext Example:");
Console.WriteLine("""
   public class ExternalDictionaryContext : DbContext
   {
       private readonly IConfiguration _configuration;

       public ExternalDictionaryContext(
           DbContextOptions<ExternalDictionaryContext> options,
           IConfiguration configuration) : base(options)
       {
           _configuration = configuration;
       }

       // Dictionary accessor for LINQ queries
       private ClickHouseDictionary<CountryLookup, ulong>? _countryDict;
       public ClickHouseDictionary<CountryLookup, ulong> CountryDict
           => _countryDict ??= new ClickHouseDictionary<CountryLookup, ulong>(this);

       protected override void OnModelCreating(ModelBuilder modelBuilder)
       {
           // External dictionary from PostgreSQL
           modelBuilder.Entity<CountryLookup>(entity =>
           {
               entity.AsDictionary<CountryLookup>(cfg => cfg
                   .HasKey(x => x.Id)
                   .FromPostgreSql(pg => pg
                       .FromTable("countries", schema: "public")
                       .Connection(c => c.UseProfile("PostgresMain"))
                       .InvalidateQuery("SELECT max(updated_at) FROM countries"))
                   .UseHashedLayout()
                   .HasLifetime(minSeconds: 60, maxSeconds: 300)
                   .HasDefault(x => x.Name, "Unknown"));
           });
       }
   }

   // Application startup:
   var builder = WebApplication.CreateBuilder(args);

   builder.Services.AddDbContext<ExternalDictionaryContext>(options =>
       options.UseClickHouse(builder.Configuration.GetConnectionString("ClickHouse")));

   var app = builder.Build();

   // Create external dictionaries at startup
   using (var scope = app.Services.CreateScope())
   {
       var context = scope.ServiceProvider.GetRequiredService<ExternalDictionaryContext>();
       await context.EnsureDictionariesAsync();
   }
   """);

// ============================================================
// Demo: Show DDL Generation
// ============================================================

Console.WriteLine("\n\n--- DDL Generation Demo ---\n");

try
{
    await using var context = new ExternalDictionarySampleContext(configuration);

    Console.WriteLine("Getting DDL for all external dictionaries:\n");

    var allDdl = context.GetAllDictionaryDdl(externalOnly: true);
    foreach (var (name, ddl) in allDdl)
    {
        Console.WriteLine($"=== {name} ===");
        Console.WriteLine(ddl);
        Console.WriteLine();
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Note: DDL generation requires proper environment setup. Error: {ex.Message}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Dictionary entity for country lookups from PostgreSQL.
/// </summary>
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
}

/// <summary>
/// Dictionary entity for product lookups from MySQL.
/// </summary>
public class ProductLookup : IClickHouseDictionary
{
    public int ProductId { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

/// <summary>
/// Dictionary entity for exchange rates from HTTP API.
/// </summary>
public class ExchangeRateLookup : IClickHouseDictionary
{
    public string CurrencyCode { get; set; } = string.Empty;
    public double Rate { get; set; }
    public DateTime UpdatedAt { get; set; }
}

/// <summary>
/// Orders table that references dictionaries.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public ulong CountryId { get; set; }
    public int ProductId { get; set; }
    public string CurrencyCode { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class ExternalDictionarySampleContext : DbContext
{
    private readonly IConfiguration? _configuration;

    public ExternalDictionarySampleContext(IConfiguration? configuration = null)
    {
        _configuration = configuration;
    }

    // Regular tables
    public DbSet<Order> Orders => Set<Order>();

    // Dictionary entities
    public DbSet<CountryLookup> CountryLookups => Set<CountryLookup>();
    public DbSet<ProductLookup> ProductLookups => Set<ProductLookup>();
    public DbSet<ExchangeRateLookup> ExchangeRateLookups => Set<ExchangeRateLookup>();

    // Dictionary accessors for LINQ queries
    private ClickHouseDictionary<CountryLookup, ulong>? _countryDict;
    private ClickHouseDictionary<ProductLookup, int>? _productDict;
    private ClickHouseDictionary<ExchangeRateLookup, string>? _exchangeRateDict;

    public ClickHouseDictionary<CountryLookup, ulong> CountryDict
        => _countryDict ??= new ClickHouseDictionary<CountryLookup, ulong>(this);

    public ClickHouseDictionary<ProductLookup, int> ProductDict
        => _productDict ??= new ClickHouseDictionary<ProductLookup, int>(this);

    public ClickHouseDictionary<ExchangeRateLookup, string> ExchangeRateDict
        => _exchangeRateDict ??= new ClickHouseDictionary<ExchangeRateLookup, string>(this);

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=external_dictionary_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // ============================================================
        // PostgreSQL Dictionary
        // ============================================================
        modelBuilder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup>(cfg => cfg
                .HasKey(x => x.Id)
                .FromPostgreSql(pg => pg
                    .FromTable("countries", schema: "public")
                    .Connection(c => c
                        .HostPort(env: "PG_HOST")
                        .Database(env: "PG_DATABASE")
                        .Credentials("PG_USER", "PG_PASSWORD"))
                    .Where("is_active = true")
                    .InvalidateQuery("SELECT max(updated_at) FROM countries"))
                .UseHashedLayout()
                .HasLifetime(minSeconds: 60, maxSeconds: 300)
                .HasDefault(x => x.Name, "Unknown")
                .HasDefault(x => x.IsoCode, "XX"));
        });

        // ============================================================
        // MySQL Dictionary
        // ============================================================
        modelBuilder.Entity<ProductLookup>(entity =>
        {
            entity.AsDictionary<ProductLookup>(cfg => cfg
                .HasKey(x => x.ProductId)
                .FromMySql(mysql => mysql
                    .FromTable("products")
                    .Connection(c => c
                        .Host(env: "MYSQL_HOST")
                        .Port(value: 3306)
                        .Database(env: "MYSQL_DATABASE")
                        .Credentials("MYSQL_USER", "MYSQL_PASSWORD"))
                    .Where("deleted_at IS NULL")
                    .InvalidateQuery("SELECT MAX(updated_at) FROM products")
                    .FailOnConnectionLoss(true))
                .UseHashedLayout()
                .HasLifetime(300)
                .HasDefault(x => x.Name, "Unknown Product")
                .HasDefault(x => x.Price, 0m));
        });

        // ============================================================
        // HTTP Dictionary
        // ============================================================
        modelBuilder.Entity<ExchangeRateLookup>(entity =>
        {
            entity.AsDictionary<ExchangeRateLookup>(cfg => cfg
                .HasKey(x => x.CurrencyCode)
                .FromHttp(http => http
                    .Url(env: "EXCHANGE_RATE_API_URL")
                    .Format("JSONEachRow")
                    .Credentials(userEnv: "API_USER", passwordEnv: "API_KEY")
                    .Header("X-API-Version", "2.0"))
                .UseLayout(DictionaryLayout.ComplexKeyHashed)
                .HasLifetime(minSeconds: 30, maxSeconds: 60)
                .HasDefault(x => x.Rate, 1.0));
        });

        // ============================================================
        // Orders Table
        // ============================================================
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
