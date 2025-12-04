using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Array Type Sample
// ============================================================
// Demonstrates using Array columns in ClickHouse with EF.CH.
// Arrays map to Array(T) and support LINQ operations like
// Contains, Any, Length, First, and Last.
// ============================================================

Console.WriteLine("Array Type Sample");
Console.WriteLine("=================\n");

await using var context = new ProductDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert products with array properties
Console.WriteLine("Inserting products with tags and price tiers...\n");

var products = new[]
{
    new Product
    {
        Id = Guid.NewGuid(),
        Name = "Gaming Laptop",
        Tags = ["electronics", "computers", "gaming", "portable"],
        PriceTiers = [1299, 1499, 1799],
        Categories = ["Technology", "Gaming"]
    },
    new Product
    {
        Id = Guid.NewGuid(),
        Name = "Wireless Mouse",
        Tags = ["electronics", "accessories", "wireless"],
        PriceTiers = [29, 39, 49],
        Categories = ["Technology", "Accessories"]
    },
    new Product
    {
        Id = Guid.NewGuid(),
        Name = "USB-C Hub",
        Tags = ["electronics", "accessories", "usb"],
        PriceTiers = [49, 69],
        Categories = ["Technology", "Accessories"]
    },
    new Product
    {
        Id = Guid.NewGuid(),
        Name = "Standing Desk",
        Tags = ["furniture", "office", "ergonomic"],
        PriceTiers = [399, 499, 699],
        Categories = ["Furniture", "Office"]
    },
    new Product
    {
        Id = Guid.NewGuid(),
        Name = "Monitor Arm",
        Tags = ["furniture", "accessories", "ergonomic"],
        PriceTiers = [79, 99],
        Categories = ["Furniture", "Accessories"]
    }
};

context.Products.AddRange(products);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {products.Length} products.\n");

// Query: Contains - find products with specific tag
Console.WriteLine("--- Products with 'electronics' tag (Contains) ---");
var electronics = await context.Products
    .Where(p => p.Tags.Contains("electronics"))
    .Select(p => p.Name)
    .ToListAsync();

foreach (var name in electronics)
    Console.WriteLine($"  {name}");

// Query: Contains - multiple conditions
Console.WriteLine("\n--- Gaming electronics (Contains with multiple tags) ---");
var gamingElectronics = await context.Products
    .Where(p => p.Tags.Contains("electronics") && p.Tags.Contains("gaming"))
    .Select(p => p.Name)
    .ToListAsync();

foreach (var name in gamingElectronics)
    Console.WriteLine($"  {name}");

// Query: Any - find products with any tags
Console.WriteLine("\n--- Products with any tags (Any) ---");
var withTags = await context.Products
    .Where(p => p.Tags.Any())
    .CountAsync();
Console.WriteLine($"  {withTags} products have tags");

// Query: Length - filter by array length
Console.WriteLine("\n--- Products with 3+ price tiers (Length) ---");
var multiTier = await context.Products
    .Where(p => p.PriceTiers.Length >= 3)
    .Select(p => new { p.Name, Tiers = p.PriceTiers.Length })
    .ToListAsync();

foreach (var item in multiTier)
    Console.WriteLine($"  {item.Name}: {item.Tiers} price tiers");

// Query: First/Last - get array elements
Console.WriteLine("\n--- Lowest and highest price tiers (First/Last) ---");
var priceRange = await context.Products
    .Select(p => new
    {
        p.Name,
        LowestPrice = p.PriceTiers.First(),
        HighestPrice = p.PriceTiers.Last()
    })
    .ToListAsync();

foreach (var item in priceRange)
    Console.WriteLine($"  {item.Name}: ${item.LowestPrice} - ${item.HighestPrice}");

// Query: Group by array length
Console.WriteLine("\n--- Products grouped by tag count ---");
var tagGroups = await context.Products
    .GroupBy(p => p.Tags.Length)
    .Select(g => new { TagCount = g.Key, Products = g.Count() })
    .OrderBy(g => g.TagCount)
    .ToListAsync();

foreach (var group in tagGroups)
    Console.WriteLine($"  {group.TagCount} tags: {group.Products} product(s)");

// Query: Filter by first element
Console.WriteLine("\n--- Products with base price under $100 ---");
var affordable = await context.Products
    .Where(p => p.PriceTiers.First() < 100)
    .Select(p => new { p.Name, BasePrice = p.PriceTiers.First() })
    .ToListAsync();

foreach (var item in affordable)
    Console.WriteLine($"  {item.Name}: ${item.BasePrice}");

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[] Tags { get; set; } = [];              // Array(String)
    public int[] PriceTiers { get; set; } = [];           // Array(Int32)
    public List<string> Categories { get; set; } = [];    // Array(String)
}

// ============================================================
// DbContext Definition
// ============================================================
public class ProductDbContext : DbContext
{
    public DbSet<Product> Products => Set<Product>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=array_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(entity =>
        {
            entity.ToTable("Products");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
            // Array properties work automatically
        });
    }
}
