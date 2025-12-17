using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Materialized View Sample
// ============================================================
// Demonstrates ClickHouse materialized views as INSERT triggers.
// When orders are inserted, daily summaries are automatically
// calculated and stored.
// ============================================================

Console.WriteLine("Materialized View Sample");
Console.WriteLine("========================\n");

await using var context = new SalesDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert orders
Console.WriteLine("Inserting orders...\n");

var orders = new[]
{
    new Order
    {
        Id = Guid.NewGuid(),
        OrderDate = DateTime.UtcNow.Date,
        ProductId = "PROD-001",
        Quantity = 5,
        Revenue = 99.95m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderDate = DateTime.UtcNow.Date,
        ProductId = "PROD-001",
        Quantity = 3,
        Revenue = 59.97m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderDate = DateTime.UtcNow.Date,
        ProductId = "PROD-002",
        Quantity = 1,
        Revenue = 149.99m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderDate = DateTime.UtcNow.Date.AddDays(-1),
        ProductId = "PROD-001",
        Quantity = 10,
        Revenue = 199.90m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderDate = DateTime.UtcNow.Date.AddDays(-1),
        ProductId = "PROD-002",
        Quantity = 2,
        Revenue = 299.98m
    }
};

context.Orders.AddRange(orders);
await context.SaveChangesAsync();

Console.WriteLine($"Inserted {orders.Length} orders.");
Console.WriteLine("Materialized view automatically aggregated the data.\n");

// Query the materialized view
Console.WriteLine("--- Daily Sales Summary (from materialized view) ---");
var summaries = await context.DailySales
    .OrderByDescending(s => s.Date)
    .ThenBy(s => s.ProductId)
    .ToListAsync();

foreach (var summary in summaries)
{
    Console.WriteLine($"  {summary.Date:yyyy-MM-dd} | {summary.ProductId}: " +
                      $"{summary.TotalQuantity} units, ${summary.TotalRevenue:F2}");
}

// Show raw orders for comparison
Console.WriteLine("\n--- Raw Orders (source table) ---");
var rawOrders = await context.Orders
    .OrderByDescending(o => o.OrderDate)
    .ThenBy(o => o.ProductId)
    .ToListAsync();

foreach (var order in rawOrders)
{
    Console.WriteLine($"  {order.OrderDate:yyyy-MM-dd} | {order.ProductId}: " +
                      $"{order.Quantity} units, ${order.Revenue:F2}");
}

// Add more orders to demonstrate automatic aggregation
Console.WriteLine("\n--- Adding more orders ---");
context.Orders.Add(new Order
{
    Id = Guid.NewGuid(),
    OrderDate = DateTime.UtcNow.Date,
    ProductId = "PROD-001",
    Quantity = 2,
    Revenue = 39.98m
});
await context.SaveChangesAsync();

Console.WriteLine("Added 1 more order.\n");

// Query updated summaries
Console.WriteLine("--- Updated Daily Sales Summary ---");
summaries = await context.DailySales
    .OrderByDescending(s => s.Date)
    .ThenBy(s => s.ProductId)
    .ToListAsync();

foreach (var summary in summaries)
{
    Console.WriteLine($"  {summary.Date:yyyy-MM-dd} | {summary.ProductId}: " +
                      $"{summary.TotalQuantity} units, ${summary.TotalRevenue:F2}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Source table for orders.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal Revenue { get; set; }
}

/// <summary>
/// Target table for materialized view - daily aggregates.
/// </summary>
public class DailySales
{
    public DateOnly Date { get; set; }
    public string ProductId { get; set; } = string.Empty;
    public long TotalQuantity { get; set; }
    public decimal TotalRevenue { get; set; }
}

/// <summary>
/// Target table for simple projection MV - demonstrates data transformation without GroupBy.
/// </summary>
public class ProcessedOrder
{
    public ulong ProductIdHash { get; set; }     // cityHash64(ProductId)
    public DateTime OrderDate { get; set; }
    public long Version { get; set; }            // toUnixTimestamp64Milli(OrderDate)
    public decimal Revenue { get; set; }
    public byte IsProcessed { get; set; }        // Constant = 1
}

// ============================================================
// DbContext Definition
// ============================================================

public class SalesDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<DailySales> DailySales => Set<DailySales>();
    public DbSet<ProcessedOrder> ProcessedOrders => Set<ProcessedOrder>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=materialized_view_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });

        // ============================================================
        // Materialized View Example 1: Aggregation with GroupBy (Raw SQL)
        // ============================================================
        modelBuilder.Entity<DailySales>(entity =>
        {
            entity.ToTable("DailySales_MV");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });

            // Raw SQL for aggregation (escape hatch for complex queries)
            entity.AsMaterializedViewRaw(
                sourceTable: "Orders",
                selectSql: @"
                    SELECT
                        toDate(""OrderDate"") AS ""Date"",
                        ""ProductId"",
                        sum(""Quantity"") AS ""TotalQuantity"",
                        sum(""Revenue"") AS ""TotalRevenue""
                    FROM ""Orders""
                    GROUP BY ""Date"", ""ProductId""
                ",
                populate: false);
        });

        // ============================================================
        // Materialized View Example 2: Simple Projection with LINQ (no GroupBy)
        // ============================================================
        // Demonstrates data transformation using ClickHouse functions:
        // - CityHash64(): Hash string to UInt64
        // - ToUnixTimestamp64Milli(): Convert DateTime to Int64 version
        // - Constants: Add computed columns (IsProcessed = 1)
        modelBuilder.Entity<ProcessedOrder>(entity =>
        {
            entity.ToTable("ProcessedOrders_MV");
            entity.UseReplacingMergeTree(
                x => x.Version,
                x => new { x.ProductIdHash, x.OrderDate });

            // LINQ-based projection (type-safe, no GroupBy needed)
            entity.AsMaterializedView<ProcessedOrder, Order>(
                query: orders => orders.Select(o => new ProcessedOrder
                {
                    ProductIdHash = o.ProductId.CityHash64(),            // Hash for efficient storage
                    OrderDate = o.OrderDate,
                    Version = o.OrderDate.ToUnixTimestamp64Milli(),      // Use date as version
                    Revenue = o.Revenue,
                    IsProcessed = 1                                      // Constant
                }),
                populate: false);
        });
    }
}
