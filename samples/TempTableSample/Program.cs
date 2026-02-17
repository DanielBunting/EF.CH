using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Temporary Tables Sample
// ============================================================
// Demonstrates ClickHouse temporary tables with EF.CH:
// - Creating empty temp tables from entity schema
// - Inserting entities into temp tables
// - Populating temp tables from queries
// - Full LINQ composition on temp table queries
// - TempTableScope for managing multiple temp tables
// - Automatic cleanup via IAsyncDisposable
// ============================================================

Console.WriteLine("Temporary Tables Sample");
Console.WriteLine("========================\n");

await using var context = new SampleDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Clean up any existing data
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Orders");

// ============================================================
// Seed sample data
// ============================================================

Console.WriteLine("\n--- Seeding sample data ---");

var random = new Random(42);
var categories = new[] { "Electronics", "Clothing", "Books", "Home", "Sports" };
var baseDate = DateTime.UtcNow.Date;

var orders = Enumerable.Range(0, 10_000)
    .Select(i => new Order
    {
        Id = Guid.NewGuid(),
        OrderDate = baseDate.AddDays(-random.Next(60)).AddHours(random.Next(24)),
        Category = categories[random.Next(categories.Length)],
        Amount = Math.Round((decimal)(random.NextDouble() * 500), 2),
        CustomerId = $"customer_{random.Next(100):D3}"
    })
    .ToList();

await context.BulkInsertAsync(orders);
Console.WriteLine($"Inserted {orders.Count:N0} sample orders");

// ============================================================
// 1. Create empty temp table, insert entities, query back
// ============================================================

Console.WriteLine("\n--- 1. Create Empty Temp Table ---");

await using (var temp = await context.CreateTempTableAsync<Order>("temp_staging"))
{
    Console.WriteLine($"Created temp table: {temp.TableName}");

    // Insert a few entities manually
    var manualOrders = new List<Order>
    {
        new() { Id = Guid.NewGuid(), OrderDate = DateTime.UtcNow, Category = "Manual", Amount = 99.99m, CustomerId = "manual_001" },
        new() { Id = Guid.NewGuid(), OrderDate = DateTime.UtcNow, Category = "Manual", Amount = 149.99m, CustomerId = "manual_002" }
    };

    await temp.InsertAsync(manualOrders);
    Console.WriteLine($"Inserted {manualOrders.Count} entities");

    var count = await temp.Query().CountAsync();
    Console.WriteLine($"Temp table contains {count} rows");
}

Console.WriteLine("Temp table dropped on dispose");

// ============================================================
// 2. Create temp table from query (filter permanent table)
// ============================================================

Console.WriteLine("\n--- 2. Create Temp Table from Query ---");

var cutoffDate = DateTime.UtcNow.AddDays(-14);

await using (var temp = await context.Orders
    .Where(o => o.Category == "Electronics" && o.OrderDate > cutoffDate)
    .ToTempTableAsync(context, "temp_recent_electronics"))
{
    var count = await temp.Query().CountAsync();
    Console.WriteLine($"Temp table '{temp.TableName}' has {count} recent Electronics orders");

    var topOrders = await temp.Query()
        .OrderByDescending(o => o.Amount)
        .Take(5)
        .ToListAsync();

    Console.WriteLine("Top 5 by amount:");
    foreach (var order in topOrders)
    {
        Console.WriteLine($"  {order.CustomerId}: ${order.Amount:F2}");
    }
}

// ============================================================
// 3. LINQ composition on temp table queries
// ============================================================

Console.WriteLine("\n--- 3. LINQ Composition ---");

await using (var temp = await context.CreateTempTableAsync<Order>("temp_linq"))
{
    // Populate from multiple sources
    await context.Orders
        .Where(o => o.Category == "Books")
        .InsertIntoTempTableAsync(temp);

    await context.Orders
        .Where(o => o.Category == "Clothing")
        .InsertIntoTempTableAsync(temp);

    // Full LINQ on temp table
    var totalCount = await temp.Query().CountAsync();
    Console.WriteLine($"Total rows (Books + Clothing): {totalCount}");

    var avgAmount = await temp.Query().AverageAsync(o => (double)o.Amount);
    Console.WriteLine($"Average amount: ${avgAmount:F2}");

    var categoryCounts = await temp.Query()
        .GroupBy(o => o.Category)
        .Select(g => new { Category = g.Key, Count = g.Count() })
        .ToListAsync();

    foreach (var cat in categoryCounts)
    {
        Console.WriteLine($"  {cat.Category}: {cat.Count} orders");
    }
}

// ============================================================
// 4. TempTableScope for multi-step pipeline
// ============================================================

Console.WriteLine("\n--- 4. TempTableScope (Multi-Step Pipeline) ---");

await using (var scope = context.BeginTempTableScope())
{
    // Step 1: High-value orders into temp table
    var highValue = await scope.CreateFromQueryAsync(
        context.Orders.Where(o => o.Amount > 200));
    Console.WriteLine($"Step 1: {await highValue.Query().CountAsync()} high-value orders");

    // Step 2: Recent orders into another temp table
    var recent = await scope.CreateFromQueryAsync(
        context.Orders.Where(o => o.OrderDate > DateTime.UtcNow.AddDays(-7)));
    Console.WriteLine($"Step 2: {await recent.Query().CountAsync()} recent orders");

    // Query both temp tables independently
    var highValueCategories = await highValue.Query()
        .GroupBy(o => o.Category)
        .Select(g => new { Category = g.Key, Total = g.Sum(o => o.Amount) })
        .OrderByDescending(x => x.Total)
        .ToListAsync();

    Console.WriteLine("High-value orders by category:");
    foreach (var cat in highValueCategories)
    {
        Console.WriteLine($"  {cat.Category}: ${cat.Total:F2}");
    }
}

Console.WriteLine("All temp tables dropped with scope");

// ============================================================
// 5. Convenience extension: InsertIntoTempTableAsync
// ============================================================

Console.WriteLine("\n--- 5. InsertIntoTempTableAsync Extension ---");

await using (var temp = await context.CreateTempTableAsync<Order>("temp_convenience"))
{
    // Insert from query using the IQueryable extension
    await context.Orders
        .Where(o => o.Category == "Sports" && o.Amount > 100)
        .InsertIntoTempTableAsync(temp);

    var count = await temp.Query().CountAsync();
    Console.WriteLine($"Inserted {count} Sports orders over $100 via InsertIntoTempTableAsync");
}

Console.WriteLine("\nDone!");
Console.WriteLine("\nTo clean up: docker stop clickhouse-sample && docker rm clickhouse-sample");

// ============================================================
// Entity Definitions
// ============================================================

public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string CustomerId { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class SampleDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=temp_table_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
