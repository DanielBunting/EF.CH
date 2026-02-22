// DeleteUpdateSample - Demonstrates DELETE and UPDATE operations via EF.CH
//
// 1. Lightweight DELETE  - ExecuteDeleteAsync with default strategy (DELETE FROM)
// 2. Mutation DELETE     - UseDeleteStrategy(Mutation), uses ALTER TABLE DELETE
// 3. ExecuteUpdateAsync  - ALTER TABLE UPDATE with SET expressions
// 4. Strategy comparison - Side-by-side demonstration of both strategies

using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("=== EF.CH Delete/Update Sample ===");
    Console.WriteLine();

    await DemoLightweightDelete(connectionString);
    await DemoMutationDelete(connectionString);
    await DemoExecuteUpdate(connectionString);
    await DemoStrategyComparison(connectionString);

    Console.WriteLine("=== All delete/update demos complete ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ---------------------------------------------------------------------------
// Helper: seed products data using BulkInsertAsync
// ---------------------------------------------------------------------------
static async Task SeedProducts(DbContext context, DbSet<Product> products)
{
    var data = new List<Product>
    {
        new() { ProductId = 1, Name = "Wireless Mouse", Category = "electronics", Price = 29.99 },
        new() { ProductId = 2, Name = "USB Cable", Category = "electronics", Price = 9.99 },
        new() { ProductId = 3, Name = "Mechanical Keyboard", Category = "electronics", Price = 89.99 },
        new() { ProductId = 4, Name = "Monitor Stand", Category = "accessories", Price = 45.00 },
        new() { ProductId = 5, Name = "Mouse Pad", Category = "accessories", Price = 12.99 },
        new() { ProductId = 6, Name = "Phone Case (old)", Category = "clearance", Price = 5.99 },
        new() { ProductId = 7, Name = "Screen Protector (old)", Category = "clearance", Price = 3.99 },
        new() { ProductId = 8, Name = "Laptop Bag", Category = "accessories", Price = 55.00 },
        new() { ProductId = 9, Name = "Webcam", Category = "electronics", Price = 69.99 },
        new() { ProductId = 10, Name = "Desk Lamp", Category = "clearance", Price = 15.99 },
    };

    await context.BulkInsertAsync(data);
}

// ---------------------------------------------------------------------------
// 1. Lightweight DELETE
// ---------------------------------------------------------------------------
static async Task DemoLightweightDelete(string connectionString)
{
    Console.WriteLine("--- 1. Lightweight DELETE (default strategy) ---");
    Console.WriteLine("Uses DELETE FROM ... WHERE ... syntax.");
    Console.WriteLine("Rows are marked as deleted immediately and filtered from queries.");
    Console.WriteLine("Physical deletion occurs during background merges.");
    Console.WriteLine();

    // Default strategy is Lightweight
    await using var context = new LightweightDeleteContext(connectionString);
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();
    await SeedProducts(context, context.Products);

    var beforeCount = await context.Products.CountAsync();
    Console.WriteLine($"Rows before delete: {beforeCount}");

    // ExecuteDeleteAsync with Lightweight strategy generates:
    //   DELETE FROM Products WHERE Price < 20
    var deleted = await context.Products
        .Where(p => p.Price < 20)
        .ExecuteDeleteAsync();

    Console.WriteLine($"ExecuteDeleteAsync returned: {deleted} rows affected");

    var afterCount = await context.Products.CountAsync();
    Console.WriteLine($"Rows after delete: {afterCount}");

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 2. Mutation DELETE
// ---------------------------------------------------------------------------
static async Task DemoMutationDelete(string connectionString)
{
    Console.WriteLine("--- 2. Mutation DELETE ---");
    Console.WriteLine("Uses ALTER TABLE ... DELETE WHERE ... syntax.");
    Console.WriteLine("Asynchronous operation that rewrites data parts in the background.");
    Console.WriteLine("Does not return an accurate affected row count.");
    Console.WriteLine();

    // Mutation strategy uses ALTER TABLE DELETE
    await using var context = new MutationDeleteContext(connectionString);
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();
    await SeedProducts(context, context.Products);

    var beforeCount = await context.Products.CountAsync();
    Console.WriteLine($"Rows before mutation delete: {beforeCount}");

    // ExecuteDeleteAsync with Mutation strategy generates:
    //   ALTER TABLE Products DELETE WHERE Category = 'clearance'
    var result = await context.Products
        .Where(p => p.Category == "clearance")
        .ExecuteDeleteAsync();

    Console.WriteLine($"ExecuteDeleteAsync returned: {result} (mutation does not return accurate count)");

    // Mutations are asynchronous -- wait for ClickHouse to process the mutation.
    // In production, you would check system.mutations for completion status.
    await Task.Delay(500);

    var afterCount = await context.Products.CountAsync();
    Console.WriteLine($"Rows after mutation delete (after 500ms wait): {afterCount}");

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 3. ExecuteUpdateAsync
// ---------------------------------------------------------------------------
static async Task DemoExecuteUpdate(string connectionString)
{
    Console.WriteLine("--- 3. ExecuteUpdateAsync ---");
    Console.WriteLine("Uses ALTER TABLE ... UPDATE ... WHERE ... syntax.");
    Console.WriteLine("Mutations rewrite data parts asynchronously.");
    Console.WriteLine();

    await using var context = new LightweightDeleteContext(connectionString);
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();
    await SeedProducts(context, context.Products);

    // Show before state
    var beforeItems = await context.Products
        .Where(p => p.Category == "electronics")
        .OrderBy(p => p.Name)
        .Select(p => new { p.Name, p.Price })
        .ToListAsync();

    Console.WriteLine("Electronics before update:");
    foreach (var item in beforeItems)
    {
        Console.WriteLine($"  {item.Name}: ${item.Price:F2}");
    }

    // ExecuteUpdateAsync generates:
    //   ALTER TABLE Products UPDATE Price = Price * 0.9 WHERE Category = 'electronics'
    var updated = await context.Products
        .Where(p => p.Category == "electronics")
        .ExecuteUpdateAsync(setters => setters
            .SetProperty(p => p.Price, p => p.Price * 0.9));

    Console.WriteLine($"ExecuteUpdateAsync returned: {updated}");

    // Mutations are asynchronous -- wait for ClickHouse to process
    await Task.Delay(500);

    var afterItems = await context.Products
        .Where(p => p.Category == "electronics")
        .OrderBy(p => p.Name)
        .Select(p => new { p.Name, p.Price })
        .ToListAsync();

    Console.WriteLine("Electronics after 10% price reduction:");
    foreach (var item in afterItems)
    {
        Console.WriteLine($"  {item.Name}: ${item.Price:F2}");
    }

    // Update multiple columns at once
    Console.WriteLine();
    Console.WriteLine("Updating multiple columns: move clearance items to 'archived' at half price...");
    await context.Products
        .Where(p => p.Category == "clearance")
        .ExecuteUpdateAsync(setters => setters
            .SetProperty(p => p.Price, p => p.Price * 0.5)
            .SetProperty(p => p.Category, "archived"));

    // Wait for the async mutation to complete
    await Task.Delay(500);

    var archivedItems = await context.Products
        .Where(p => p.Category == "archived")
        .OrderBy(p => p.Name)
        .Select(p => new { p.Name, p.Price })
        .ToListAsync();

    Console.WriteLine($"Archived items ({archivedItems.Count}):");
    foreach (var item in archivedItems)
    {
        Console.WriteLine($"  {item.Name}: ${item.Price:F2}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 4. Strategy Comparison
// ---------------------------------------------------------------------------
static async Task DemoStrategyComparison(string connectionString)
{
    Console.WriteLine("--- 4. Strategy Comparison ---");
    Console.WriteLine("Side-by-side comparison of Lightweight vs Mutation delete strategies.");
    Console.WriteLine();

    Console.WriteLine("Lightweight DELETE:");
    Console.WriteLine("  SQL:       DELETE FROM table WHERE condition");
    Console.WriteLine("  Behavior:  Synchronous, rows filtered immediately");
    Console.WriteLine("  Returns:   Affected row count");
    Console.WriteLine("  Best for:  Normal application deletes, small to medium batches");
    Console.WriteLine();

    Console.WriteLine("Mutation DELETE:");
    Console.WriteLine("  SQL:       ALTER TABLE table DELETE WHERE condition");
    Console.WriteLine("  Behavior:  Asynchronous, data parts rewritten in background");
    Console.WriteLine("  Returns:   Does not return accurate row count");
    Console.WriteLine("  Best for:  Bulk maintenance, partition-wide cleanup");
    Console.WriteLine();

    // Demonstrate Lightweight
    Console.WriteLine("Running Lightweight DELETE...");
    await using (var lwContext = new LightweightDeleteContext(connectionString))
    {
        await lwContext.Database.EnsureDeletedAsync();
        await lwContext.Database.EnsureCreatedAsync();
        await SeedProducts(lwContext, lwContext.Products);
        var before = await lwContext.Products.CountAsync();
        var result = await lwContext.Products.Where(p => p.Price < 30).ExecuteDeleteAsync();
        var after = await lwContext.Products.CountAsync();
        Console.WriteLine($"  Before: {before}, Deleted: {result}, After: {after}");
        await lwContext.Database.EnsureDeletedAsync();
    }

    // Demonstrate Mutation
    Console.WriteLine("Running Mutation DELETE...");
    await using (var mutContext = new MutationDeleteContext(connectionString))
    {
        await mutContext.Database.EnsureDeletedAsync();
        await mutContext.Database.EnsureCreatedAsync();
        await SeedProducts(mutContext, mutContext.Products);
        var before = await mutContext.Products.CountAsync();
        var result = await mutContext.Products.Where(p => p.Price < 30).ExecuteDeleteAsync();
        // Wait for the async mutation to complete
        await Task.Delay(500);
        var after = await mutContext.Products.CountAsync();
        Console.WriteLine($"  Before: {before}, Result: {result} (not accurate), After: {after}");
        await mutContext.Database.EnsureDeletedAsync();
    }

    Console.WriteLine();
    Console.WriteLine("Configuration in code:");
    Console.WriteLine("  // Lightweight (default):");
    Console.WriteLine("  options.UseClickHouse(conn);");
    Console.WriteLine();
    Console.WriteLine("  // Mutation:");
    Console.WriteLine("  options.UseClickHouse(conn, o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));");
    Console.WriteLine();
}

// ===========================================================================
// Entity and DbContext classes
// ===========================================================================

public class Product
{
    public ulong ProductId { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
    public double Price { get; set; }
}

// Lightweight DELETE strategy (default)
public class LightweightDeleteContext(string connectionString) : DbContext
{
    public DbSet<Product> Products => Set<Product>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString, o =>
            o.UseDeleteStrategy(ClickHouseDeleteStrategy.Lightweight));

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.ProductId });
        });
    }
}

// Mutation DELETE strategy
public class MutationDeleteContext(string connectionString) : DbContext
{
    public DbSet<Product> Products => Set<Product>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString, o =>
            o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation));

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.ProductId });
        });
    }
}
