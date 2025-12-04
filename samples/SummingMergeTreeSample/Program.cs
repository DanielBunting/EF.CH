using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// SummingMergeTree Sample
// ============================================================
// Demonstrates automatic aggregation using SummingMergeTree.
// Numeric columns are automatically summed when rows with the
// same ORDER BY key are merged.
// ============================================================

Console.WriteLine("SummingMergeTree Sample");
Console.WriteLine("=======================\n");

await using var context = new MetricsDbContext();

// Create the database and tables
Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Simulate daily sales data being inserted incrementally
var today = DateOnly.FromDateTime(DateTime.Today);
Console.WriteLine($"Inserting sales data for {today}...\n");

// Insert multiple sales for the same product on the same day
// These will be automatically summed during merges
var salesData = new[]
{
    new DailySales { Date = today, ProductId = "PROD-001", Quantity = 5, Revenue = 99.95m },
    new DailySales { Date = today, ProductId = "PROD-001", Quantity = 3, Revenue = 59.97m },
    new DailySales { Date = today, ProductId = "PROD-001", Quantity = 2, Revenue = 39.98m },
    new DailySales { Date = today, ProductId = "PROD-002", Quantity = 10, Revenue = 149.90m },
    new DailySales { Date = today, ProductId = "PROD-002", Quantity = 5, Revenue = 74.95m },
};

foreach (var sale in salesData)
{
    Console.WriteLine($"  {sale.ProductId}: qty={sale.Quantity}, revenue=${sale.Revenue}");
}

context.DailySales.AddRange(salesData);
await context.SaveChangesAsync();

// Query totals (will be summed even before merge completes)
Console.WriteLine("\n--- Totals by Product ---");
var productTotals = await context.DailySales
    .Where(s => s.Date == today)
    .GroupBy(s => s.ProductId)
    .Select(g => new
    {
        ProductId = g.Key,
        TotalQuantity = g.Sum(s => s.Quantity),
        TotalRevenue = g.Sum(s => s.Revenue)
    })
    .ToListAsync();

foreach (var product in productTotals)
{
    Console.WriteLine($"  {product.ProductId}: qty={product.TotalQuantity}, revenue=${product.TotalRevenue}");
}

// Add more sales and see totals update
Console.WriteLine("\n--- Adding more sales ---");
var moreSales = new[]
{
    new DailySales { Date = today, ProductId = "PROD-001", Quantity = 7, Revenue = 139.93m },
    new DailySales { Date = today, ProductId = "PROD-003", Quantity = 1, Revenue = 999.99m },
};

foreach (var sale in moreSales)
{
    Console.WriteLine($"  {sale.ProductId}: qty={sale.Quantity}, revenue=${sale.Revenue}");
}

context.DailySales.AddRange(moreSales);
await context.SaveChangesAsync();

// Query updated totals
Console.WriteLine("\n--- Updated Totals ---");
var updatedTotals = await context.DailySales
    .Where(s => s.Date == today)
    .GroupBy(s => s.ProductId)
    .Select(g => new
    {
        ProductId = g.Key,
        TotalQuantity = g.Sum(s => s.Quantity),
        TotalRevenue = g.Sum(s => s.Revenue)
    })
    .OrderBy(p => p.ProductId)
    .ToListAsync();

foreach (var product in updatedTotals)
{
    Console.WriteLine($"  {product.ProductId}: qty={product.TotalQuantity}, revenue=${product.TotalRevenue}");
}

// Grand total
var grandTotal = await context.DailySales
    .Where(s => s.Date == today)
    .GroupBy(s => 1)
    .Select(g => new
    {
        TotalQuantity = g.Sum(s => s.Quantity),
        TotalRevenue = g.Sum(s => s.Revenue)
    })
    .FirstAsync();

Console.WriteLine($"\n  GRAND TOTAL: qty={grandTotal.TotalQuantity}, revenue=${grandTotal.TotalRevenue}");

// Force merge to see physical row reduction
Console.WriteLine("\n--- Before OPTIMIZE ---");
var rowCountBefore = await context.DailySales
    .Where(s => s.Date == today)
    .CountAsync();
Console.WriteLine($"  Physical rows: {rowCountBefore}");

Console.WriteLine("\n--- Running OPTIMIZE FINAL ---");
await context.Database.ExecuteSqlRawAsync(@"OPTIMIZE TABLE ""DailySales"" FINAL");

var rowCountAfter = await context.DailySales
    .Where(s => s.Date == today)
    .CountAsync();
Console.WriteLine($"  Physical rows after merge: {rowCountAfter}");

// Now each product has exactly one row with summed values
Console.WriteLine("\n--- Final Data (one row per product) ---");
var finalData = await context.DailySales
    .Where(s => s.Date == today)
    .OrderBy(s => s.ProductId)
    .ToListAsync();

foreach (var row in finalData)
{
    Console.WriteLine($"  {row.ProductId}: qty={row.Quantity}, revenue=${row.Revenue}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================
public class DailySales
{
    public DateOnly Date { get; set; }
    public string ProductId { get; set; } = string.Empty;
    public long Quantity { get; set; }     // Will be summed
    public decimal Revenue { get; set; }   // Will be summed
}

// ============================================================
// DbContext Definition
// ============================================================
public class MetricsDbContext : DbContext
{
    public DbSet<DailySales> DailySales => Set<DailySales>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=summing_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DailySales>(entity =>
        {
            entity.ToTable("DailySales");
            entity.HasNoKey();  // Keyless - append-only aggregation

            // SummingMergeTree configuration:
            // - ORDER BY columns define the grouping key
            // - All numeric columns are summed during merges
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });

            // Partition by month for efficient data management
            entity.HasPartitionByMonth(x => x.Date);
        });
    }
}
