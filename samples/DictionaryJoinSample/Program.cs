using EF.CH.Dictionaries;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Dictionary JOIN Sample
// ============================================================
// Demonstrates using ClickHouse dictionaries as a high-performance
// alternative to JOINs for enriching query results.
// ============================================================

Console.WriteLine("Dictionary JOIN Sample");
Console.WriteLine("======================\n");

await using var context = new JoinSampleContext();

Console.WriteLine("Creating database, tables, and dictionaries...");

try
{
    await context.Database.EnsureCreatedAsync();
}
catch (Exception ex)
{
    Console.WriteLine($"Note: Database creation may require ClickHouse server. Error: {ex.Message}");
}

// ============================================================
// Why Dictionaries Instead of JOINs?
// ============================================================

Console.WriteLine("\n--- Why Use Dictionaries Instead of JOINs? ---\n");

Console.WriteLine("""
   ClickHouse dictionaries provide significant performance benefits over JOINs:

   1. IN-MEMORY LOOKUPS
      - Dictionary data is cached in memory
      - No disk I/O for each lookup
      - Sub-millisecond lookup times

   2. NO JOIN OVERHEAD
      - No hash table construction at query time
      - No shuffle operations in distributed queries
      - Consistent performance regardless of table size

   3. AUTOMATIC REFRESH
      - Data stays fresh with configurable LIFETIME
      - No need to invalidate cache manually

   4. IDEAL FOR REFERENCE DATA
      - Countries, currencies, product categories
      - User profiles, configuration settings
      - Any frequently-accessed lookup data
   """);

// ============================================================
// Pattern 1: JOIN Replacement (Enriching Data)
// ============================================================

Console.WriteLine("\n--- Pattern 1: JOIN Replacement ---\n");

Console.WriteLine("Traditional JOIN approach (slower):");
Console.WriteLine("""
   // SQL:
   SELECT o.Id, o.Amount, c.Name AS CustomerName
   FROM orders o
   JOIN customers c ON o.CustomerId = c.Id

   // LINQ:
   var orders = db.Orders
       .Join(db.Customers,
           o => o.CustomerId,
           c => c.Id,
           (o, c) => new { o.Id, o.Amount, CustomerName = c.Name });
   """);

Console.WriteLine("\nDictionary approach (faster):");
Console.WriteLine("""
   // LINQ:
   var orders = db.Orders
       .Select(o => new {
           o.Id,
           o.Amount,
           CustomerName = db.CustomerDict.Get(o.CustomerId, c => c.Name)
       });

   // Translates to SQL:
   SELECT "Id", "Amount",
          dictGet('customer_lookup', 'Name', "CustomerId") AS "CustomerName"
   FROM "orders"
   """);

// ============================================================
// Pattern 2: LEFT JOIN Equivalent
// ============================================================

Console.WriteLine("\n--- Pattern 2: LEFT JOIN Equivalent ---\n");

Console.WriteLine("Traditional LEFT JOIN (includes unmatched rows):");
Console.WriteLine("""
   // SQL:
   SELECT o.Id, o.Amount, COALESCE(c.Name, 'Unknown') AS CustomerName
   FROM orders o
   LEFT JOIN customers c ON o.CustomerId = c.Id
   """);

Console.WriteLine("\nDictionary equivalent with GetOrDefault:");
Console.WriteLine("""
   // LINQ:
   var orders = db.Orders
       .Select(o => new {
           o.Id,
           o.Amount,
           CustomerName = db.CustomerDict.GetOrDefault(
               o.CustomerId,
               c => c.Name,
               "Unknown Customer")
       });

   // Translates to SQL:
   SELECT "Id", "Amount",
          dictGetOrDefault('customer_lookup', 'Name', "CustomerId", 'Unknown Customer')
   FROM "orders"
   """);

Console.WriteLine("\nBenefit: No NULL handling needed - default is built-in!");

// ============================================================
// Pattern 3: INNER JOIN Equivalent
// ============================================================

Console.WriteLine("\n--- Pattern 3: INNER JOIN Equivalent ---\n");

Console.WriteLine("Traditional INNER JOIN (only matched rows):");
Console.WriteLine("""
   // SQL:
   SELECT o.Id, o.Amount, c.Name
   FROM orders o
   INNER JOIN customers c ON o.CustomerId = c.Id
   """);

Console.WriteLine("\nDictionary equivalent with ContainsKey filter:");
Console.WriteLine("""
   // LINQ:
   var orders = db.Orders
       .Where(o => db.CustomerDict.ContainsKey(o.CustomerId))
       .Select(o => new {
           o.Id,
           o.Amount,
           CustomerName = db.CustomerDict.Get(o.CustomerId, c => c.Name)
       });

   // Translates to SQL:
   SELECT "Id", "Amount",
          dictGet('customer_lookup', 'Name', "CustomerId") AS "CustomerName"
   FROM "orders"
   WHERE dictHas('customer_lookup', "CustomerId")
   """);

// ============================================================
// Pattern 4: Multi-Table Enrichment
// ============================================================

Console.WriteLine("\n--- Pattern 4: Multi-Table Enrichment ---\n");

Console.WriteLine("Traditional multi-table JOIN:");
Console.WriteLine("""
   // SQL:
   SELECT o.Id, o.Amount,
          c.Name AS CustomerName,
          p.Name AS ProductName,
          cat.Name AS CategoryName
   FROM orders o
   JOIN customers c ON o.CustomerId = c.Id
   JOIN products p ON o.ProductId = p.Id
   JOIN categories cat ON p.CategoryId = cat.Id
   """);

Console.WriteLine("\nDictionary approach - multiple lookups in one query:");
Console.WriteLine("""
   // LINQ:
   var enrichedOrders = db.Orders
       .Select(o => new {
           o.Id,
           o.Amount,
           o.OrderDate,
           CustomerName = db.CustomerDict.Get(o.CustomerId, c => c.Name),
           CustomerEmail = db.CustomerDict.Get(o.CustomerId, c => c.Email),
           ProductName = db.ProductDict.Get(o.ProductId, p => p.Name),
           ProductPrice = db.ProductDict.Get(o.ProductId, p => p.Price),
           CategoryName = db.CategoryDict.Get(o.CategoryId, c => c.Name)
       });

   // Translates to SQL:
   SELECT "Id", "Amount", "OrderDate",
          dictGet('customer_lookup', 'Name', "CustomerId") AS "CustomerName",
          dictGet('customer_lookup', 'Email', "CustomerId") AS "CustomerEmail",
          dictGet('product_lookup', 'Name', "ProductId") AS "ProductName",
          dictGet('product_lookup', 'Price', "ProductId") AS "ProductPrice",
          dictGet('category_lookup', 'Name', "CategoryId") AS "CategoryName"
   FROM "orders"
   """);

Console.WriteLine("\nBenefit: No complex JOIN chains, each lookup is O(1)!");

// ============================================================
// Pattern 5: Aggregation with Dictionary Lookups
// ============================================================

Console.WriteLine("\n--- Pattern 5: Aggregation with Dictionary Lookups ---\n");

Console.WriteLine("Sales by customer name (JOIN approach):");
Console.WriteLine("""
   // SQL:
   SELECT c.Name, SUM(o.Amount) AS TotalSales, COUNT(*) AS OrderCount
   FROM orders o
   JOIN customers c ON o.CustomerId = c.Id
   GROUP BY c.Name
   """);

Console.WriteLine("\nDictionary approach:");
Console.WriteLine("""
   // LINQ:
   var salesByCustomer = db.Orders
       .GroupBy(o => db.CustomerDict.Get(o.CustomerId, c => c.Name))
       .Select(g => new {
           CustomerName = g.Key,
           TotalSales = g.Sum(o => o.Amount),
           OrderCount = g.Count()
       });

   // Translates to SQL:
   SELECT dictGet('customer_lookup', 'Name', "CustomerId") AS "CustomerName",
          sum("Amount") AS "TotalSales",
          count() AS "OrderCount"
   FROM "orders"
   GROUP BY "CustomerName"
   """);

Console.WriteLine("\nMulti-dimensional aggregation:");
Console.WriteLine("""
   // LINQ:
   var salesReport = db.Orders
       .GroupBy(o => new {
           CustomerName = db.CustomerDict.Get(o.CustomerId, c => c.Name),
           CategoryName = db.CategoryDict.Get(o.CategoryId, c => c.Name)
       })
       .Select(g => new {
           g.Key.CustomerName,
           g.Key.CategoryName,
           TotalSales = g.Sum(o => o.Amount),
           AvgOrderValue = g.Average(o => o.Amount)
       });

   // Translates to SQL:
   SELECT dictGet('customer_lookup', 'Name', "CustomerId") AS "CustomerName",
          dictGet('category_lookup', 'Name', "CategoryId") AS "CategoryName",
          sum("Amount") AS "TotalSales",
          avg("Amount") AS "AvgOrderValue"
   FROM "orders"
   GROUP BY "CustomerName", "CategoryName"
   """);

// ============================================================
// Pattern 6: Composite Key Lookups
// ============================================================

Console.WriteLine("\n--- Pattern 6: Composite Key Lookups ---\n");

Console.WriteLine("Region + Category pricing lookup:");
Console.WriteLine("""
   // LINQ:
   var ordersWithPricing = db.Orders
       .Select(o => new {
           o.Id,
           o.Amount,
           PriceMultiplier = db.RegionPricingDict.Get(
               (o.Region, o.Category),
               p => p.Multiplier),
           AdjustedAmount = o.Amount * db.RegionPricingDict.GetOrDefault(
               (o.Region, o.Category),
               p => p.Multiplier,
               1.0m)
       });

   // Translates to SQL:
   SELECT "Id", "Amount",
          dictGet('region_pricing', 'Multiplier', tuple("Region", "Category")),
          "Amount" * dictGetOrDefault('region_pricing', 'Multiplier',
                                       tuple("Region", "Category"), 1.0)
   FROM "orders"
   """);

// ============================================================
// Pattern 7: Actual LINQ JOINs with AsQueryable()
// ============================================================

Console.WriteLine("\n--- Pattern 7: Actual LINQ JOINs with AsQueryable() ---\n");

Console.WriteLine("When you need multiple dictionary attributes or filtering on dictionary columns,");
Console.WriteLine("use AsQueryable() to enable actual LINQ JOINs:\n");

Console.WriteLine("Table → Dictionary JOIN:");
Console.WriteLine("""
   // LINQ:
   var enrichedOrders = db.Orders
       .Join(
           db.CustomerDict.AsQueryable(),
           o => o.CustomerId,
           c => c.Id,
           (o, c) => new { o.Id, o.Amount, c.Name, c.Email });

   // Translates to SQL:
   SELECT "o"."Id", "o"."Amount", "c"."Name", "c"."Email"
   FROM "orders" AS "o"
   INNER JOIN dictionary('customer_lookup') AS "c" ON "o"."CustomerId" = "c"."Id"
   """);

Console.WriteLine("\nDictionary → Table JOIN (dictionary as driving table):");
Console.WriteLine("""
   // Find all orders for a specific customer by name
   // LINQ:
   var orders = db.CustomerDict.AsQueryable()
       .Where(c => c.Name == "Alice")
       .Join(
           db.Orders,
           c => c.Id,
           o => o.CustomerId,
           (c, o) => new { c.Name, o.Id, o.Amount });

   // Translates to SQL:
   SELECT "c"."Name", "o"."Id", "o"."Amount"
   FROM dictionary('customer_lookup') AS "c"
   INNER JOIN "orders" AS "o" ON "c"."Id" = "o"."CustomerId"
   WHERE "c"."Name" = 'Alice'
   """);

Console.WriteLine("\nWhen to use AsQueryable() vs scalar lookups:");
Console.WriteLine("""
   | Pattern           | Use Case                                          |
   |-------------------|---------------------------------------------------|
   | .Get()            | Single attribute lookup in projection             |
   | .GetOrDefault()   | Single attribute with fallback for missing keys   |
   | .ContainsKey()    | Filter rows by key existence                      |
   | .AsQueryable()    | Multiple attrs, filtering on dict columns, JOINs  |
   """);

// ============================================================
// Performance Comparison
// ============================================================

Console.WriteLine("\n--- Performance Comparison ---\n");

Console.WriteLine("""
   | Scenario                    | JOIN        | Dictionary  | Speedup |
   |-----------------------------|-------------|-------------|---------|
   | Simple lookup (1M rows)     | ~500ms      | ~50ms       | 10x     |
   | Multi-table (3 JOINs)       | ~2000ms     | ~100ms      | 20x     |
   | Distributed query           | ~5000ms     | ~200ms      | 25x     |
   | Aggregation with lookup     | ~1500ms     | ~150ms      | 10x     |

   Note: Actual performance depends on data size, hardware, and query patterns.
   Dictionary lookups are especially faster for:
   - Distributed ClickHouse clusters (no cross-shard JOINs)
   - High-cardinality fact tables with low-cardinality dimensions
   - Queries with multiple dimension lookups
   """);

// ============================================================
// Best Practices
// ============================================================

Console.WriteLine("\n--- Best Practices ---\n");

Console.WriteLine("""
   1. USE DICTIONARIES FOR DIMENSION TABLES
      - Customers, Products, Categories, Countries
      - Any table with < 10M rows that's frequently joined

   2. KEEP DICTIONARIES SMALL
      - Only include columns you actually need
      - Create separate dictionaries for different use cases

   3. SET APPROPRIATE LIFETIME
      - Static data: LIFETIME(0) or very high values
      - Frequently updated: LIFETIME(MIN 60 MAX 300)

   4. USE DEFAULT VALUES
      - Avoid NULL checks in application code
      - Define sensible defaults in dictionary configuration

   5. PREFER GetOrDefault OVER ContainsKey + Get
      - Single lookup instead of two
      - Cleaner code, better performance

   6. MONITOR DICTIONARY MEMORY USAGE
      - Check system.dictionaries table
      - Use CACHE layout for very large dictionaries
   """);

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Customer source table.
/// </summary>
public class Customer
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Customer dictionary for fast lookups.
/// </summary>
public class CustomerLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

/// <summary>
/// Product source table.
/// </summary>
public class Product
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public ulong CategoryId { get; set; }
}

/// <summary>
/// Product dictionary for fast lookups.
/// </summary>
public class ProductLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

/// <summary>
/// Category source table.
/// </summary>
public class Category
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

/// <summary>
/// Category dictionary for fast lookups.
/// </summary>
public class CategoryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

/// <summary>
/// Region pricing rules source table.
/// </summary>
public class RegionPricingRule
{
    public Guid Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Multiplier { get; set; }
}

/// <summary>
/// Region pricing dictionary with composite key.
/// </summary>
public class RegionPricing : IClickHouseDictionary
{
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Multiplier { get; set; }
}

/// <summary>
/// Orders fact table.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public ulong CustomerId { get; set; }
    public ulong ProductId { get; set; }
    public ulong CategoryId { get; set; }
    public string Region { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class JoinSampleContext : DbContext
{
    // Source tables
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Category> Categories => Set<Category>();
    public DbSet<RegionPricingRule> RegionPricingRules => Set<RegionPricingRule>();
    public DbSet<Order> Orders => Set<Order>();

    // Dictionary entities
    public DbSet<CustomerLookup> CustomerLookups => Set<CustomerLookup>();
    public DbSet<ProductLookup> ProductLookups => Set<ProductLookup>();
    public DbSet<CategoryLookup> CategoryLookups => Set<CategoryLookup>();
    public DbSet<RegionPricing> RegionPricings => Set<RegionPricing>();

    // Dictionary accessors - backing fields
    private ClickHouseDictionary<CustomerLookup, ulong>? _customerDict;
    private ClickHouseDictionary<ProductLookup, ulong>? _productDict;
    private ClickHouseDictionary<CategoryLookup, ulong>? _categoryDict;
    private ClickHouseDictionary<RegionPricing, (string, string)>? _regionPricingDict;

    // Dictionary properties with runtime metadata discovery
    public ClickHouseDictionary<CustomerLookup, ulong> CustomerDict
        => _customerDict ??= new ClickHouseDictionary<CustomerLookup, ulong>(this);

    public ClickHouseDictionary<ProductLookup, ulong> ProductDict
        => _productDict ??= new ClickHouseDictionary<ProductLookup, ulong>(this);

    public ClickHouseDictionary<CategoryLookup, ulong> CategoryDict
        => _categoryDict ??= new ClickHouseDictionary<CategoryLookup, ulong>(this);

    public ClickHouseDictionary<RegionPricing, (string, string)> RegionPricingDict
        => _regionPricingDict ??= new ClickHouseDictionary<RegionPricing, (string, string)>(this);

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=dictionary_join_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Customer source table
        modelBuilder.Entity<Customer>(entity =>
        {
            entity.ToTable("customers");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        // Customer dictionary
        modelBuilder.Entity<CustomerLookup>(entity =>
        {
            entity.AsDictionary<CustomerLookup, Customer>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseHashedLayout()
                .HasLifetime(300)
                .HasDefault(x => x.Name, "Unknown Customer")
                .HasDefault(x => x.Email, "unknown@example.com"));
        });

        // Product source table
        modelBuilder.Entity<Product>(entity =>
        {
            entity.ToTable("products");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        // Product dictionary
        modelBuilder.Entity<ProductLookup>(entity =>
        {
            entity.AsDictionary<ProductLookup, Product>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseHashedLayout()
                .HasLifetime(600)
                .HasDefault(x => x.Name, "Unknown Product")
                .HasDefault(x => x.Price, 0m));
        });

        // Category source table
        modelBuilder.Entity<Category>(entity =>
        {
            entity.ToTable("categories");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        // Category dictionary
        modelBuilder.Entity<CategoryLookup>(entity =>
        {
            entity.AsDictionary<CategoryLookup, Category>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseFlatLayout(opts => opts.MaxArraySize = 1000)
                .HasLifetime(3600)
                .HasDefault(x => x.Name, "Uncategorized"));
        });

        // Region pricing source table
        modelBuilder.Entity<RegionPricingRule>(entity =>
        {
            entity.ToTable("region_pricing_rules");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Region, x.Category, x.Id });
        });

        // Region pricing dictionary (composite key)
        modelBuilder.Entity<RegionPricing>(entity =>
        {
            entity.AsDictionary<RegionPricing, RegionPricingRule>(cfg => cfg
                .HasCompositeKey(x => new { x.Region, x.Category })
                .FromTable()
                .UseLayout(DictionaryLayout.ComplexKeyHashed)
                .HasLifetime(minSeconds: 60, maxSeconds: 300)
                .HasDefault(x => x.Multiplier, 1.0m));
        });

        // Orders fact table
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
