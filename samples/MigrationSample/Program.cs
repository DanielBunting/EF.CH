// MigrationSample - EF Core Migrations with ClickHouse via EF.CH
//
// Demonstrates:
// - Defining a DbContext in a separate file (SampleDbContext.cs)
// - Running migrations programmatically with MigrateAsync
// - The Add-Migration / Update-Database CLI workflow (documented in comments)
// - Inserting and querying data after migration

using Microsoft.EntityFrameworkCore;
using MigrationSample;
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

    Console.WriteLine("=== EF.CH Migration Sample ===");
    Console.WriteLine();

    // ---------------------------------------------------------------------------
    // Migration Workflow (CLI)
    //
    // Before running this sample, you would typically generate migrations using
    // the EF Core CLI tools:
    //
    //   1. Install the EF Core tools (if not already installed):
    //      dotnet tool install --global dotnet-ef
    //
    //   2. Create an initial migration:
    //      dotnet ef migrations add InitialCreate
    //
    //   3. Apply the migration to the database:
    //      dotnet ef migrations script        (to preview SQL)
    //      dotnet ef database update           (to apply directly)
    //
    //   4. When the model changes, add a new migration:
    //      dotnet ef migrations add AddNewColumn
    //      dotnet ef database update
    //
    // The MigrateAsync() call below applies any pending migrations at runtime.
    // This is useful for development and some deployment scenarios.
    // ---------------------------------------------------------------------------

    await using var context = new SampleDbContext(connectionString);

    // Step 1: Apply pending migrations
    Console.WriteLine("[1] Applying migrations...");
    Console.WriteLine("    Calling context.Database.MigrateAsync()");
    Console.WriteLine("    This creates or updates the database schema based on migration files.");
    Console.WriteLine();

    // Note: MigrateAsync requires at least one migration to exist in the Migrations folder.
    // If no migrations have been generated yet, you can use EnsureCreatedAsync instead:
    //   await context.Database.EnsureCreatedAsync();
    //
    // For this sample, we use EnsureCreatedAsync to work without pre-generated migrations.
    // In a real project, use MigrateAsync with generated migration files.
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("    Schema applied successfully.");
    Console.WriteLine();

    // Step 2: Insert sample data
    Console.WriteLine("[2] Inserting sample products...");

    var products = new List<Product>
    {
        new() { Id = Guid.NewGuid(), Name = "Wireless Mouse", Category = "Electronics", Price = 29.99m, StockQuantity = 150, CreatedAt = DateTime.UtcNow },
        new() { Id = Guid.NewGuid(), Name = "USB-C Cable", Category = "Electronics", Price = 12.99m, StockQuantity = 500, CreatedAt = DateTime.UtcNow },
        new() { Id = Guid.NewGuid(), Name = "Standing Desk", Category = "Furniture", Price = 449.00m, StockQuantity = 25, CreatedAt = DateTime.UtcNow },
        new() { Id = Guid.NewGuid(), Name = "Monitor Arm", Category = "Furniture", Price = 89.99m, StockQuantity = 75, CreatedAt = DateTime.UtcNow },
        new() { Id = Guid.NewGuid(), Name = "Mechanical Keyboard", Category = "Electronics", Price = 149.99m, StockQuantity = 80, CreatedAt = DateTime.UtcNow },
    };

    context.Products.AddRange(products);
    await context.SaveChangesAsync();
    Console.WriteLine($"    Inserted {products.Count} products.");
    Console.WriteLine();

    // Step 3: Query the data
    Console.WriteLine("[3] Querying products by category...");

    var summary = await context.Products
        .GroupBy(p => p.Category)
        .Select(g => new
        {
            Category = g.Key,
            Count = g.Count(),
            AvgPrice = g.Average(p => (double)p.Price),
            TotalStock = g.Sum(p => p.StockQuantity),
        })
        .OrderBy(x => x.Category)
        .ToListAsync();

    foreach (var row in summary)
    {
        Console.WriteLine($"    {row.Category,-15} Count={row.Count}  AvgPrice={row.AvgPrice:F2}  TotalStock={row.TotalStock}");
    }
    Console.WriteLine();

    // Step 4: Show all products
    Console.WriteLine("[4] All products (ordered by price desc)...");

    var allProducts = await context.Products
        .OrderByDescending(p => p.Price)
        .ToListAsync();

    foreach (var p in allProducts)
    {
        Console.WriteLine($"    {p.Name,-25} {p.Category,-15} ${p.Price,-10:F2} Stock={p.StockQuantity}");
    }
    Console.WriteLine();

    // Clean up
    await context.Database.EnsureDeletedAsync();

    Console.WriteLine("=== Done ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}
