using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Lightweight UPDATE Sample
// ============================================================
// Demonstrates ClickHouse mutation-based updates via ExecuteUpdateAsync:
// - Single column update
// - Multiple column updates
// - Expression-based updates (computed values)
// - Note: SaveChanges() tracked updates remain blocked (by design)
// ============================================================

Console.WriteLine("Lightweight UPDATE Sample");
Console.WriteLine("========================\n");

await using var context = new ProductDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert products
Console.WriteLine("Inserting products...\n");

context.Products.AddRange(
    new Product { Id = Guid.NewGuid(), Name = "Laptop", Category = "electronics", Price = 999.99m, Status = "active" },
    new Product { Id = Guid.NewGuid(), Name = "Phone", Category = "electronics", Price = 699.99m, Status = "active" },
    new Product { Id = Guid.NewGuid(), Name = "Headphones", Category = "electronics", Price = 149.99m, Status = "active" },
    new Product { Id = Guid.NewGuid(), Name = "Novel", Category = "books", Price = 14.99m, Status = "active" },
    new Product { Id = Guid.NewGuid(), Name = "Textbook", Category = "books", Price = 79.99m, Status = "discontinued" }
);
await context.SaveChangesAsync();
Console.WriteLine("Inserted 5 products.\n");

// Show initial state
Console.WriteLine("--- Initial Products ---");
await PrintProductsAsync(context);

// Single column update: mark discontinued products as archived
Console.WriteLine("\n--- Update single column: Status = 'archived' for discontinued ---");
await context.Products
    .Where(p => p.Status == "discontinued")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "archived"));

// Wait for mutation to process
await Task.Delay(500);
await PrintProductsAsync(context);

// Multiple column update: rename category and update status
Console.WriteLine("\n--- Update multiple columns: books -> literature, status -> 'reviewed' ---");
await context.Products
    .Where(p => p.Category == "books")
    .ExecuteUpdateAsync(s => s
        .SetProperty(p => p.Category, "literature")
        .SetProperty(p => p.Status, "reviewed"));

await Task.Delay(500);
await PrintProductsAsync(context);

// Expression-based update: 10% price increase for electronics
Console.WriteLine("\n--- Expression update: 10% price increase for electronics ---");
await context.Products
    .Where(p => p.Category == "electronics")
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, p => p.Price * 1.10m));

await Task.Delay(500);
await PrintProductsAsync(context);

// Update all rows (no WHERE filter)
Console.WriteLine("\n--- Update all products: Status = 'final' ---");
await context.Products
    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, "final"));

await Task.Delay(500);
await PrintProductsAsync(context);

// Demonstrate that SaveChanges tracked updates still throw
Console.WriteLine("\n--- SaveChanges tracked update (expected to fail) ---");
try
{
    var product = await context.Products.FirstAsync();
    product.Name = "Changed Name";
    await context.SaveChangesAsync();
    Console.WriteLine("ERROR: Should have thrown!");
}
catch (Exception ex)
{
    Console.WriteLine($"Correctly threw: {ex.GetType().Name}");
    Console.WriteLine($"Message: {ex.Message}\n");
    Console.WriteLine("This is expected! Single-row tracked updates are intentionally blocked.");
    Console.WriteLine("Use ExecuteUpdateAsync for bulk mutations instead.");
}

Console.WriteLine("\nDone!");

// Helper method
static async Task PrintProductsAsync(ProductDbContext ctx)
{
    ctx.ChangeTracker.Clear();
    var all = await ctx.Products.OrderBy(p => p.Name).ToListAsync();
    Console.WriteLine($"Products ({all.Count}):");
    foreach (var p in all)
    {
        Console.WriteLine($"  {p.Name} [{p.Category}] ${p.Price:F2} ({p.Status})");
    }
}

// ============================================================
// Entity Definition
// ============================================================

public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Status { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class ProductDbContext : DbContext
{
    public DbSet<Product> Products => Set<Product>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=update_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(entity =>
        {
            entity.ToTable("Products");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}
