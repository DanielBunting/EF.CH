// -----------------------------------------------------------------
// ParameterizedViewSample - Parameterized Views with EF.CH
// -----------------------------------------------------------------
// Demonstrates:
//   1. Create a parameterized view (CreateParameterizedViewAsync)
//   2. Query a parameterized view (FromParameterizedView)
//   3. Drop a parameterized view (DropParameterizedViewAsync)
//   4. Idempotent creation (EnsureParameterizedViewsAsync)
// -----------------------------------------------------------------

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// Start ClickHouse container
var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.");

try
{
    var connectionString = container.GetConnectionString();
    await SeedData(connectionString);

    await DemoCreateParameterizedView(connectionString);
    await DemoQueryParameterizedView(connectionString);
    await DemoDropParameterizedView(connectionString);
    await DemoEnsureParameterizedViews(connectionString);
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// -----------------------------------------------------------------
// Seed sample data
// -----------------------------------------------------------------
static async Task SeedData(string connectionString)
{
    await using var context = new ViewDemoContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    await context.BulkInsertAsync(new List<Event>
    {
        new() { EventId = 1,  UserId = 100, EventType = "page_view", Category = "web",    Amount = 0,     Timestamp = new DateTime(2024, 1, 15, 10, 0, 0) },
        new() { EventId = 2,  UserId = 100, EventType = "click",     Category = "web",    Amount = 0,     Timestamp = new DateTime(2024, 1, 15, 10, 5, 0) },
        new() { EventId = 3,  UserId = 100, EventType = "purchase",  Category = "web",    Amount = 49.99m, Timestamp = new DateTime(2024, 1, 15, 10, 10, 0) },
        new() { EventId = 4,  UserId = 200, EventType = "page_view", Category = "mobile", Amount = 0,     Timestamp = new DateTime(2024, 1, 15, 11, 0, 0) },
        new() { EventId = 5,  UserId = 200, EventType = "page_view", Category = "mobile", Amount = 0,     Timestamp = new DateTime(2024, 1, 16, 9, 0, 0) },
        new() { EventId = 6,  UserId = 200, EventType = "purchase",  Category = "mobile", Amount = 29.99m, Timestamp = new DateTime(2024, 1, 16, 9, 30, 0) },
        new() { EventId = 7,  UserId = 300, EventType = "page_view", Category = "web",    Amount = 0,     Timestamp = new DateTime(2024, 2, 1, 8, 0, 0) },
        new() { EventId = 8,  UserId = 300, EventType = "click",     Category = "web",    Amount = 0,     Timestamp = new DateTime(2024, 2, 1, 8, 15, 0) },
        new() { EventId = 9,  UserId = 300, EventType = "purchase",  Category = "web",    Amount = 99.99m, Timestamp = new DateTime(2024, 2, 1, 8, 30, 0) },
        new() { EventId = 10, UserId = 100, EventType = "page_view", Category = "web",    Amount = 0,     Timestamp = new DateTime(2024, 2, 15, 14, 0, 0) },
        new() { EventId = 11, UserId = 100, EventType = "purchase",  Category = "web",    Amount = 75.00m, Timestamp = new DateTime(2024, 2, 15, 14, 30, 0) },
        new() { EventId = 12, UserId = 400, EventType = "page_view", Category = "mobile", Amount = 0,     Timestamp = new DateTime(2024, 3, 1, 12, 0, 0) },
    });

    Console.WriteLine("Seeded 12 event records.\n");
}

// -----------------------------------------------------------------
// Demo 1: Create a Parameterized View
// -----------------------------------------------------------------
static async Task DemoCreateParameterizedView(string connectionString)
{
    Console.WriteLine("=== 1. Create Parameterized View ===\n");

    await using var context = new ViewDemoContext(connectionString);

    // Create a parameterized view that filters events by user and date range.
    // Parameters use ClickHouse syntax: {name:Type}
    await context.Database.CreateParameterizedViewAsync(
        "user_events_view",
        """
        SELECT EventId, UserId, EventType, Category, Amount, Timestamp
        FROM events
        WHERE UserId = {user_id:UInt64}
          AND Timestamp >= {start_date:DateTime}
        """);

    Console.WriteLine("Created parameterized view: user_events_view");
    Console.WriteLine("Parameters: user_id (UInt64), start_date (DateTime)");
    Console.WriteLine();

    // Create a second view for category summary with more parameters
    await context.Database.CreateParameterizedViewAsync(
        "category_summary_view",
        """
        SELECT
            Category,
            count() AS EventCount,
            sum(Amount) AS TotalAmount
        FROM events
        WHERE Timestamp >= {start_date:DateTime}
          AND Timestamp < {end_date:DateTime}
        GROUP BY Category
        """);

    Console.WriteLine("Created parameterized view: category_summary_view");
    Console.WriteLine("Parameters: start_date (DateTime), end_date (DateTime)");
}

// -----------------------------------------------------------------
// Demo 2: Query a Parameterized View
// -----------------------------------------------------------------
static async Task DemoQueryParameterizedView(string connectionString)
{
    Console.WriteLine("\n=== 2. Query Parameterized View ===\n");

    await using var context = new ViewDemoContext(connectionString);

    // Query using anonymous object for parameters.
    // Property names are converted to snake_case automatically
    // (UserId -> user_id, StartDate -> start_date).
    Console.WriteLine("--- Query with anonymous object parameters ---");
    var userEvents = await context.FromParameterizedView<EventView>(
            "user_events_view",
            new { UserId = 100UL, StartDate = new DateTime(2024, 1, 1) })
        .ToListAsync();

    Console.WriteLine($"Events for user 100 since 2024-01-01: {userEvents.Count} results");
    foreach (var evt in userEvents)
    {
        Console.WriteLine($"  [{evt.EventId}] {evt.EventType} ({evt.Category}) - ${evt.Amount:F2} at {evt.Timestamp:yyyy-MM-dd HH:mm}");
    }

    // Query with further LINQ composition (Where, OrderBy, etc.)
    Console.WriteLine("\n--- Query with LINQ composition ---");
    var purchases = await context.FromParameterizedView<EventView>(
            "user_events_view",
            new { UserId = 100UL, StartDate = new DateTime(2024, 1, 1) })
        .Where(e => e.EventType == "purchase")
        .OrderByDescending(e => e.Amount)
        .ToListAsync();

    Console.WriteLine($"Purchases by user 100: {purchases.Count} results");
    foreach (var evt in purchases)
    {
        Console.WriteLine($"  [{evt.EventId}] ${evt.Amount:F2} at {evt.Timestamp:yyyy-MM-dd HH:mm}");
    }

    // Query using dictionary parameters
    Console.WriteLine("\n--- Query with dictionary parameters ---");
    var parameters = new Dictionary<string, object?>
    {
        ["user_id"] = 200UL,
        ["start_date"] = new DateTime(2024, 1, 1)
    };
    var user200Events = await context.FromParameterizedView<EventView>(
            "user_events_view",
            parameters)
        .ToListAsync();

    Console.WriteLine($"Events for user 200: {user200Events.Count} results");
    foreach (var evt in user200Events)
    {
        Console.WriteLine($"  [{evt.EventId}] {evt.EventType} at {evt.Timestamp:yyyy-MM-dd HH:mm}");
    }

    // Query the category summary view
    Console.WriteLine("\n--- Category summary (January 2024) ---");
    var summary = await context.FromParameterizedView<CategorySummaryView>(
            "category_summary_view",
            new
            {
                StartDate = new DateTime(2024, 1, 1),
                EndDate = new DateTime(2024, 2, 1)
            })
        .OrderByDescending(s => s.TotalAmount)
        .ToListAsync();

    foreach (var row in summary)
    {
        Console.WriteLine($"  {row.Category}: {row.EventCount} events, ${row.TotalAmount:F2} total");
    }
}

// -----------------------------------------------------------------
// Demo 3: Drop a Parameterized View
// -----------------------------------------------------------------
static async Task DemoDropParameterizedView(string connectionString)
{
    Console.WriteLine("\n=== 3. Drop Parameterized View ===\n");

    await using var context = new ViewDemoContext(connectionString);

    // Drop with IF EXISTS (default)
    await context.Database.DropParameterizedViewAsync("category_summary_view");
    Console.WriteLine("Dropped view: category_summary_view (IF EXISTS)");

    // Drop with explicit ifExists: false would throw if the view does not exist
    await context.Database.DropParameterizedViewAsync("user_events_view", ifExists: true);
    Console.WriteLine("Dropped view: user_events_view (IF EXISTS)");

    // Dropping a non-existent view with ifExists: true is safe
    await context.Database.DropParameterizedViewAsync("nonexistent_view", ifExists: true);
    Console.WriteLine("Dropped view: nonexistent_view (IF EXISTS, no error)");
}

// -----------------------------------------------------------------
// Demo 4: EnsureParameterizedViewsAsync (Idempotent)
// -----------------------------------------------------------------
static async Task DemoEnsureParameterizedViews(string connectionString)
{
    Console.WriteLine("\n=== 4. EnsureParameterizedViewsAsync ===\n");

    await using var context = new ViewDemoContext(connectionString);

    Console.WriteLine("EnsureParameterizedViewsAsync creates all views configured via");
    Console.WriteLine("the fluent API in OnModelCreating. It is idempotent (uses IF NOT EXISTS).\n");

    Console.WriteLine("Usage pattern (at application startup):");
    Console.WriteLine("""
      await context.Database.EnsureCreatedAsync();
      await context.Database.EnsureParameterizedViewsAsync();
    """);

    Console.WriteLine("\nNote: This method only creates views that are configured with full");
    Console.WriteLine("metadata (view definition and parameters) via the fluent API.");
    Console.WriteLine("Views created via CreateParameterizedViewAsync are not tracked here.\n");

    // Recreate views manually for verification
    await context.Database.CreateParameterizedViewAsync(
        "user_events_view",
        """
        SELECT EventId, UserId, EventType, Category, Amount, Timestamp
        FROM events
        WHERE UserId = {user_id:UInt64}
          AND Timestamp >= {start_date:DateTime}
        """,
        ifNotExists: true);

    Console.WriteLine("Recreated user_events_view with IF NOT EXISTS.");

    // Verify it works
    var events = await context.FromParameterizedView<EventView>(
            "user_events_view",
            new { UserId = 300UL, StartDate = new DateTime(2024, 1, 1) })
        .ToListAsync();

    Console.WriteLine($"Verification query returned {events.Count} events for user 300.");
}

// -----------------------------------------------------------------
// Entities
// -----------------------------------------------------------------

// Source table entity
public class Event
{
    public ulong EventId { get; set; }
    public ulong UserId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime Timestamp { get; set; }
}

// View result entity (must be keyless for FromSqlRaw)
public class EventView
{
    public ulong EventId { get; set; }
    public ulong UserId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime Timestamp { get; set; }
}

// Category summary view result
public class CategorySummaryView
{
    public string Category { get; set; } = string.Empty;
    public ulong EventCount { get; set; }
    public decimal TotalAmount { get; set; }
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class ViewDemoContext(string connectionString) : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("events");
            entity.UseMergeTree(x => new { x.Timestamp, x.EventId });
            entity.HasPartitionByMonth(x => x.Timestamp);
        });

        // Keyless entity for parameterized view results.
        // This entity does not map to a table -- it is used with FromParameterizedView.
        modelBuilder.Entity<EventView>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable((string?)null); // No table mapping
        });

        // Keyless entity for category summary view results
        modelBuilder.Entity<CategorySummaryView>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable((string?)null);
        });
    }
}
