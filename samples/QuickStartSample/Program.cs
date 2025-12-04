using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// EF.CH Quick Start Sample
// ============================================================
// This sample demonstrates the minimum code needed to:
// 1. Connect to ClickHouse
// 2. Create a table with MergeTree engine
// 3. Insert data
// 4. Query data
// ============================================================

Console.WriteLine("EF.CH Quick Start Sample");
Console.WriteLine("========================\n");

// Create database context
await using var context = new QuickStartDbContext();

// Create the database and tables (or use migrations in production)
Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert some sample data
Console.WriteLine("Inserting sample data...");
var events = new[]
{
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-5),
        EventType = "page_view",
        UserId = "user-001",
        Data = "{\"page\": \"/home\"}"
    },
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-3),
        EventType = "button_click",
        UserId = "user-001",
        Data = "{\"button\": \"signup\"}"
    },
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-1),
        EventType = "page_view",
        UserId = "user-002",
        Data = "{\"page\": \"/pricing\"}"
    }
};

context.Events.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Length} events.\n");

// Query data
Console.WriteLine("Querying recent events...");
var recentEvents = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddHours(-1))
    .OrderByDescending(e => e.Timestamp)
    .ToListAsync();

Console.WriteLine($"Found {recentEvents.Count} events:\n");
foreach (var evt in recentEvents)
{
    Console.WriteLine($"  [{evt.Timestamp:HH:mm:ss}] {evt.EventType} by {evt.UserId}");
}

// Aggregation example
Console.WriteLine("\nEvent counts by type:");
var eventCounts = await context.Events
    .GroupBy(e => e.EventType)
    .Select(g => new { EventType = g.Key, Count = g.Count() })
    .ToListAsync();

foreach (var count in eventCounts)
{
    Console.WriteLine($"  {count.EventType}: {count.Count}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public string? Data { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================
public class QuickStartDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        // Connect to ClickHouse
        // Default connection: localhost:8123, database: quickstart
        options.UseClickHouse("Host=localhost;Database=quickstart");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            // Table name
            entity.ToTable("Events");

            // Primary key for EF Core tracking
            entity.HasKey(e => e.Id);

            // REQUIRED: Every ClickHouse table needs an engine with ORDER BY
            // This creates a MergeTree table ordered by Timestamp, then Id
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });

            // OPTIONAL: Partition by month for better query performance
            // Data is organized into monthly partitions
            entity.HasPartitionByMonth(x => x.Timestamp);
        });
    }
}
