// QuickStartSample - Getting started with EF.CH (Entity Framework Core for ClickHouse)
//
// Demonstrates:
// - Defining an entity and DbContext with ClickHouse configuration
// - Using MergeTree engine with monthly partitioning
// - Creating tables with EnsureCreatedAsync
// - Inserting data with AddRange + SaveChangesAsync
// - Querying with GroupBy and aggregation
// - Basic LINQ queries with filtering and ordering

using EF.CH.Extensions;
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

    Console.WriteLine("=== EF.CH QuickStart Sample ===");
    Console.WriteLine();

    await using var context = new AppDbContext(connectionString);

    // Step 1: Create the table
    Console.WriteLine("[1] Creating table...");
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("    Table 'Events' created.");
    Console.WriteLine();

    // Step 2: Insert sample data
    Console.WriteLine("[2] Inserting sample data...");

    var now = DateTime.UtcNow;
    var events = new List<Event>
    {
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-1), EventType = "PageView", Amount = 0m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-2), EventType = "Purchase", Amount = 49.99m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-3), EventType = "PageView", Amount = 0m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-4), EventType = "Purchase", Amount = 129.99m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-5), EventType = "SignUp", Amount = 0m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-6), EventType = "Purchase", Amount = 79.50m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-7), EventType = "PageView", Amount = 0m },
        new() { Id = Guid.NewGuid(), Timestamp = now.AddHours(-8), EventType = "SignUp", Amount = 0m },
    };

    context.Events.AddRange(events);
    await context.SaveChangesAsync();
    Console.WriteLine($"    Inserted {events.Count} events.");
    Console.WriteLine();

    // Step 3: Query with GroupBy and aggregation
    Console.WriteLine("[3] Aggregating events by type...");

    var summary = await context.Events
        .GroupBy(e => e.EventType)
        .Select(g => new
        {
            EventType = g.Key,
            Count = g.Count(),
            TotalAmount = g.Sum(e => e.Amount),
        })
        .OrderByDescending(x => x.Count)
        .ToListAsync();

    foreach (var row in summary)
    {
        Console.WriteLine($"    {row.EventType,-12} Count={row.Count}  TotalAmount={row.TotalAmount:F2}");
    }
    Console.WriteLine();

    // Step 4: Filtered query
    Console.WriteLine("[4] Querying purchases over $50...");

    var bigPurchases = await context.Events
        .Where(e => e.EventType == "Purchase" && e.Amount > 50m)
        .OrderByDescending(e => e.Amount)
        .ToListAsync();

    foreach (var e in bigPurchases)
    {
        Console.WriteLine($"    {e.Timestamp:yyyy-MM-dd HH:mm:ss}  Amount={e.Amount:F2}");
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

// ===========================================================================
// Entity and DbContext definitions
// ===========================================================================

public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class AppDbContext(string connectionString) : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasKey(e => e.Id);

            // Use MergeTree engine ordered by Timestamp then Id
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });

            // Partition data by month for efficient time-range queries
            entity.HasPartitionByMonth<Event, DateTime>(x => x.Timestamp);
        });
    }
}
