using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Set Operations Sample
// ============================================================
// Demonstrates ClickHouse set operations:
// - Concat (UNION ALL) — keeps duplicates
// - Union (UNION DISTINCT) — removes duplicates
// - Intersect — common rows
// - Except — rows in first but not second
// - UnionAll / UnionDistinct convenience extensions
// - SetOperationBuilder fluent API
// ============================================================

Console.WriteLine("Set Operations Sample");
Console.WriteLine("=====================\n");

await using var context = new SalesDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert sales data
Console.WriteLine("Inserting sales data...\n");

context.Sales.AddRange(
    // Q1 Sales
    new Sale { Id = Guid.NewGuid(), Product = "Laptop", Region = "North", Amount = 999.99m, Quarter = "Q1" },
    new Sale { Id = Guid.NewGuid(), Product = "Phone", Region = "North", Amount = 599.99m, Quarter = "Q1" },
    new Sale { Id = Guid.NewGuid(), Product = "Laptop", Region = "South", Amount = 899.99m, Quarter = "Q1" },
    // Q2 Sales
    new Sale { Id = Guid.NewGuid(), Product = "Laptop", Region = "North", Amount = 1099.99m, Quarter = "Q2" },
    new Sale { Id = Guid.NewGuid(), Product = "Tablet", Region = "South", Amount = 449.99m, Quarter = "Q2" },
    new Sale { Id = Guid.NewGuid(), Product = "Phone", Region = "South", Amount = 649.99m, Quarter = "Q2" },
    // Q3 Sales
    new Sale { Id = Guid.NewGuid(), Product = "Headphones", Region = "North", Amount = 149.99m, Quarter = "Q3" },
    new Sale { Id = Guid.NewGuid(), Product = "Laptop", Region = "North", Amount = 1199.99m, Quarter = "Q3" }
);
await context.SaveChangesAsync();
context.ChangeTracker.Clear();
Console.WriteLine("Inserted 8 sales records.\n");

// Concat (UNION ALL) — combine Q1 and Q2, keeping duplicates
Console.WriteLine("--- Concat (UNION ALL): Q1 + Q2 sales ---");
var q1 = context.Sales.Where(s => s.Quarter == "Q1");
var q2 = context.Sales.Where(s => s.Quarter == "Q2");

var concatResult = await q1.Concat(q2).OrderBy(s => s.Product).ToListAsync();
Console.WriteLine($"Total rows: {concatResult.Count} (Q1 + Q2 combined)");
foreach (var s in concatResult)
{
    Console.WriteLine($"  {s.Product} [{s.Region}] ${s.Amount:F2} ({s.Quarter})");
}

// Union (UNION DISTINCT) — unique products across Q1 and Q2
Console.WriteLine("\n--- Union (UNION DISTINCT): North + South regions ---");
var north = context.Sales.Where(s => s.Region == "North").Select(s => s.Product);
var south = context.Sales.Where(s => s.Region == "South").Select(s => s.Product);

var unionResult = await north.Union(south).OrderBy(p => p).ToListAsync();
Console.WriteLine($"Unique products across all regions: {unionResult.Count}");
foreach (var p in unionResult)
{
    Console.WriteLine($"  {p}");
}

// Intersect — products sold in BOTH North and South
Console.WriteLine("\n--- Intersect: Products sold in both North AND South ---");
var northProducts = context.Sales.Where(s => s.Region == "North").Select(s => s.Product);
var southProducts = context.Sales.Where(s => s.Region == "South").Select(s => s.Product);

var intersectResult = await northProducts.Intersect(southProducts).OrderBy(p => p).ToListAsync();
Console.WriteLine($"Products in both regions: {intersectResult.Count}");
foreach (var p in intersectResult)
{
    Console.WriteLine($"  {p}");
}

// Except — products sold in North but NOT South
Console.WriteLine("\n--- Except: Products in North but NOT South ---");
var exceptResult = await northProducts.Except(southProducts).OrderBy(p => p).ToListAsync();
Console.WriteLine($"North-exclusive products: {exceptResult.Count}");
foreach (var p in exceptResult)
{
    Console.WriteLine($"  {p}");
}

// UnionAll convenience: combine all three quarters
Console.WriteLine("\n--- UnionAll: Q1 + Q2 + Q3 (convenience extension) ---");
var q3 = context.Sales.Where(s => s.Quarter == "Q3");
var allQuarters = await q1.UnionAll(q2, q3).OrderBy(s => s.Quarter).ThenBy(s => s.Product).ToListAsync();
Console.WriteLine($"All sales across Q1/Q2/Q3: {allQuarters.Count} rows");
foreach (var s in allQuarters)
{
    Console.WriteLine($"  {s.Quarter}: {s.Product} [{s.Region}] ${s.Amount:F2}");
}

// SetOperationBuilder fluent API
Console.WriteLine("\n--- SetOperationBuilder: fluent chaining ---");
var highValue = context.Sales.Where(s => s.Amount > 500);
var northSales = context.Sales.Where(s => s.Region == "North");
var lowValue = context.Sales.Where(s => s.Amount < 200);

var builderResult = await highValue
    .AsSetOperation()
    .UnionAll(northSales)    // Add all North sales
    .Except(lowValue)        // Remove low-value items
    .Build()
    .OrderByDescending(s => s.Amount)
    .Take(10)
    .ToListAsync();

Console.WriteLine($"High-value + North (excluding low-value): {builderResult.Count} rows");
foreach (var s in builderResult)
{
    Console.WriteLine($"  {s.Product} [{s.Region}] ${s.Amount:F2} ({s.Quarter})");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================

public class Sale
{
    public Guid Id { get; set; }
    public string Product { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Quarter { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class SalesDbContext : DbContext
{
    public DbSet<Sale> Sales => Set<Sale>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=set_operations_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Sale>(entity =>
        {
            entity.ToTable("Sales");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}
