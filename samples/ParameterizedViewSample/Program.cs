using EF.CH.Extensions;
using EF.CH.ParameterizedViews;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Parameterized View Sample
// ============================================================
// Demonstrates ClickHouse parameterized views for efficient
// filtered queries with runtime parameters.
// ============================================================

Console.WriteLine("Parameterized View Sample");
Console.WriteLine("=========================\n");

await using var context = new AnalyticsDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Create the source table
Console.WriteLine("Creating source table...");
await context.Database.ExecuteSqlRawAsync(@"
    CREATE TABLE IF NOT EXISTS events (
        event_id UInt64,
        event_type String,
        user_id UInt64,
        timestamp DateTime64(3),
        value Decimal(18, 4)
    ) ENGINE = MergeTree()
    ORDER BY (user_id, timestamp)
    PARTITION BY toYYYYMM(timestamp)
");

// Create the parameterized view using raw SQL (traditional approach)
Console.WriteLine("Creating parameterized view (raw SQL)...");
await context.Database.CreateParameterizedViewAsync(
    "user_events_view",
    @"SELECT event_id AS ""EventId"",
             event_type AS ""EventType"",
             user_id AS ""UserId"",
             timestamp AS ""Timestamp"",
             value AS ""Value""
      FROM events
      WHERE user_id = {user_id:UInt64}
        AND timestamp >= {start_date:DateTime}
        AND timestamp < {end_date:DateTime}",
    ifNotExists: true);

// Insert sample data
Console.WriteLine("Inserting sample events...\n");
await context.Database.ExecuteSqlRawAsync(@"
    INSERT INTO events (event_id, event_type, user_id, timestamp, value) VALUES
    (1, 'page_view', 100, '2024-01-15 10:00:00', 0),
    (2, 'click', 100, '2024-01-15 10:05:00', 0),
    (3, 'purchase', 100, '2024-01-15 10:10:00', 99.99),
    (4, 'page_view', 100, '2024-01-16 11:00:00', 0),
    (5, 'click', 100, '2024-01-16 11:15:00', 0),
    (6, 'purchase', 100, '2024-01-16 11:20:00', 149.99),
    (7, 'page_view', 100, '2024-01-17 09:00:00', 0),
    (8, 'page_view', 100, '2024-01-20 14:00:00', 0),
    (9, 'page_view', 200, '2024-01-15 08:00:00', 0),
    (10, 'click', 200, '2024-01-15 08:30:00', 0),
    (11, 'purchase', 200, '2024-01-15 09:00:00', 49.99),
    (12, 'page_view', 300, '2024-01-16 12:00:00', 0),
    (13, 'click', 300, '2024-01-16 12:05:00', 0)
");

// ============================================================
// Example 1: Basic Query with Parameters (Traditional)
// ============================================================
Console.WriteLine("--- Example 1: Basic Query (Traditional) ---");
Console.WriteLine("Getting all events for user 100 in Jan 15-17:\n");

var basicResults = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new
    {
        user_id = 100UL,
        start_date = new DateTime(2024, 1, 15),
        end_date = new DateTime(2024, 1, 18)
    })
    .OrderBy(e => e.Timestamp)
    .ToListAsync();

foreach (var evt in basicResults)
{
    Console.WriteLine($"  {evt.Timestamp:yyyy-MM-dd HH:mm:ss} | {evt.EventType,-10} | Value: ${evt.Value:F2}");
}

// ============================================================
// Example 2: Strongly-Typed View Access (NEW!)
// ============================================================
Console.WriteLine("\n--- Example 2: Strongly-Typed View Access (NEW!) ---");
Console.WriteLine("Using ClickHouseParameterizedView<T> for type-safe access:\n");

// Access the view through the strongly-typed accessor
var recentPurchases = await context.UserEventsView
    .Query(new { user_id = 100UL, start_date = new DateTime(2024, 1, 1), end_date = new DateTime(2024, 2, 1) })
    .Where(e => e.EventType == "purchase")
    .OrderByDescending(e => e.Timestamp)
    .ToListAsync();

foreach (var evt in recentPurchases)
{
    Console.WriteLine($"  {evt.Timestamp:yyyy-MM-dd HH:mm:ss} | Purchase: ${evt.Value:F2}");
}

// Convenience methods
var purchaseCount = await context.UserEventsView.CountAsync(
    new { user_id = 100UL, start_date = new DateTime(2024, 1, 1), end_date = new DateTime(2024, 2, 1) });
Console.WriteLine($"\n  Total events for user 100: {purchaseCount}");

// ============================================================
// Example 3: LINQ Composition - Where + OrderBy + Take
// ============================================================
Console.WriteLine("\n--- Example 3: LINQ Composition ---");
Console.WriteLine("Getting last 3 purchase events for user 100:\n");

var purchaseEvents = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new
    {
        user_id = 100UL,
        start_date = new DateTime(2024, 1, 1),
        end_date = new DateTime(2024, 2, 1)
    })
    .Where(e => e.EventType == "purchase")
    .OrderByDescending(e => e.Timestamp)
    .Take(3)
    .ToListAsync();

foreach (var evt in purchaseEvents)
{
    Console.WriteLine($"  {evt.Timestamp:yyyy-MM-dd HH:mm:ss} | Purchase: ${evt.Value:F2}");
}

// ============================================================
// Example 4: Aggregation with GroupBy
// ============================================================
Console.WriteLine("\n--- Example 4: Aggregation ---");
Console.WriteLine("Event type summary for user 100 in January:\n");

var summary = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new
    {
        user_id = 100UL,
        start_date = new DateTime(2024, 1, 1),
        end_date = new DateTime(2024, 2, 1)
    })
    .GroupBy(e => e.EventType)
    .Select(g => new
    {
        EventType = g.Key,
        Count = g.Count(),
        TotalValue = g.Sum(e => e.Value)
    })
    .OrderBy(x => x.EventType)
    .ToListAsync();

foreach (var item in summary)
{
    Console.WriteLine($"  {item.EventType,-12}: {item.Count} events, Total: ${item.TotalValue:F2}");
}

// ============================================================
// Example 5: Different User Query
// ============================================================
Console.WriteLine("\n--- Example 5: Different User ---");
Console.WriteLine("Events for user 200:\n");

var user200Events = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    new
    {
        user_id = 200UL,
        start_date = new DateTime(2024, 1, 1),
        end_date = new DateTime(2024, 2, 1)
    })
    .OrderBy(e => e.Timestamp)
    .ToListAsync();

foreach (var evt in user200Events)
{
    Console.WriteLine($"  {evt.Timestamp:yyyy-MM-dd HH:mm:ss} | {evt.EventType,-10} | Value: ${evt.Value:F2}");
}

// ============================================================
// Example 6: Using Dictionary Parameters
// ============================================================
Console.WriteLine("\n--- Example 6: Dictionary Parameters ---");
Console.WriteLine("Dynamic parameter query for user 300:\n");

var dynamicParams = new Dictionary<string, object?>
{
    ["user_id"] = 300UL,
    ["start_date"] = new DateTime(2024, 1, 1),
    ["end_date"] = new DateTime(2024, 2, 1)
};

var dynamicResults = await context.FromParameterizedView<UserEventView>(
    "user_events_view",
    dynamicParams)
    .ToListAsync();

foreach (var evt in dynamicResults)
{
    Console.WriteLine($"  {evt.Timestamp:yyyy-MM-dd HH:mm:ss} | {evt.EventType,-10}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Source entity representing the events table.
/// </summary>
public class Event
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

/// <summary>
/// Result entity for the parameterized view.
/// </summary>
public class UserEventView
{
    public ulong EventId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public DateTime Timestamp { get; set; }
    public decimal Value { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class AnalyticsDbContext : DbContext
{
    public DbSet<UserEventView> UserEventViews => Set<UserEventView>();

    // Strongly-typed view accessor (NEW!)
    private ClickHouseParameterizedView<UserEventView>? _userEventsView;
    public ClickHouseParameterizedView<UserEventView> UserEventsView
        => _userEventsView ??= new ClickHouseParameterizedView<UserEventView>(this);

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=parameterized_view_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Configure the result entity for the parameterized view
        modelBuilder.Entity<UserEventView>(entity =>
        {
            entity.HasParameterizedView("user_events_view");
        });
    }
}
