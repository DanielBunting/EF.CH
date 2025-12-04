using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Map Type Sample
// ============================================================
// Demonstrates using Map (Dictionary) columns in ClickHouse.
// Maps store key-value pairs and are useful for flexible
// metadata, labels, and configuration data.
// ============================================================

Console.WriteLine("Map Type Sample");
Console.WriteLine("===============\n");

await using var context = new EventDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert events with map properties
Console.WriteLine("Inserting events with metadata and counters...\n");

var events = new[]
{
    new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-10),
        EventType = "page_view",
        Metadata = new Dictionary<string, string>
        {
            ["url"] = "/products/laptop",
            ["referrer"] = "https://google.com",
            ["user_agent"] = "Mozilla/5.0 Chrome/120"
        },
        Counters = new Dictionary<string, int>
        {
            ["scroll_depth"] = 75,
            ["time_on_page"] = 45
        }
    },
    new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-8),
        EventType = "button_click",
        Metadata = new Dictionary<string, string>
        {
            ["url"] = "/products/laptop",
            ["button_id"] = "add-to-cart",
            ["button_text"] = "Add to Cart"
        },
        Counters = new Dictionary<string, int>
        {
            ["click_x"] = 450,
            ["click_y"] = 320
        }
    },
    new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-5),
        EventType = "page_view",
        Metadata = new Dictionary<string, string>
        {
            ["url"] = "/checkout",
            ["referrer"] = "/products/laptop"
        },
        Counters = new Dictionary<string, int>
        {
            ["scroll_depth"] = 100,
            ["time_on_page"] = 120
        }
    },
    new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-2),
        EventType = "purchase",
        Metadata = new Dictionary<string, string>
        {
            ["url"] = "/checkout/success",
            ["order_id"] = "ORD-12345",
            ["payment_method"] = "credit_card"
        },
        Counters = new Dictionary<string, int>
        {
            ["items_count"] = 2,
            ["total_cents"] = 149900
        }
    },
    new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-1),
        EventType = "error",
        Metadata = new Dictionary<string, string>
        {
            ["url"] = "/api/users",
            ["error_code"] = "500",
            ["error_message"] = "Internal server error"
        },
        Counters = new Dictionary<string, int>
        {
            ["response_time_ms"] = 5000
        }
    }
};

context.Events.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Length} events.\n");

// Query: Access specific map key
Console.WriteLine("--- URLs from all events (Map key access) ---");
var urls = await context.Events
    .Select(e => new { e.EventType, Url = e.Metadata["url"] })
    .ToListAsync();

foreach (var item in urls)
    Console.WriteLine($"  [{item.EventType}] {item.Url}");

// Query: Filter by map value
Console.WriteLine("\n--- Page views from Google (Filter by map value) ---");
var fromGoogle = await context.Events
    .Where(e => e.EventType == "page_view")
    .Where(e => e.Metadata["referrer"].Contains("google"))
    .Select(e => e.Metadata["url"])
    .ToListAsync();

foreach (var url in fromGoogle)
    Console.WriteLine($"  {url}");

// Query: Filter by map key existence
Console.WriteLine("\n--- Events with error_code (ContainsKey) ---");
var errors = await context.Events
    .Where(e => e.Metadata.ContainsKey("error_code"))
    .Select(e => new
    {
        e.EventType,
        ErrorCode = e.Metadata["error_code"],
        Message = e.Metadata["error_message"]
    })
    .ToListAsync();

foreach (var error in errors)
    Console.WriteLine($"  [{error.EventType}] {error.ErrorCode}: {error.Message}");

// Query: Access counter values
Console.WriteLine("\n--- Events with scroll depth > 50% ---");
var highEngagement = await context.Events
    .Where(e => e.Counters.ContainsKey("scroll_depth"))
    .Where(e => e.Counters["scroll_depth"] > 50)
    .Select(e => new
    {
        Url = e.Metadata["url"],
        ScrollDepth = e.Counters["scroll_depth"],
        TimeOnPage = e.Counters["time_on_page"]

    })
    .ToListAsync();

foreach (var item in highEngagement)
    Console.WriteLine($"  {item.Url}: {item.ScrollDepth}% scroll, {item.TimeOnPage}s");

// Query: Group by map value
Console.WriteLine("\n--- Events by URL ---");
var byUrl = await context.Events
    .GroupBy(e => e.Metadata["url"])
    .Select(g => new { Url = g.Key, Count = g.Count() })
    .OrderByDescending(x => x.Count)
    .ToListAsync();

foreach (var item in byUrl)
    Console.WriteLine($"  {item.Url}: {item.Count} event(s)");

// Query: Purchase details
Console.WriteLine("\n--- Purchase events with order details ---");
var purchases = await context.Events
    .Where(e => e.EventType == "purchase")
    .Select(e => new
    {
        OrderId = e.Metadata["order_id"],
        PaymentMethod = e.Metadata["payment_method"],
        Items = e.Counters["items_count"],
        TotalCents = e.Counters["total_cents"]
    })
    .ToListAsync();

foreach (var p in purchases)
    Console.WriteLine($"  Order {p.OrderId}: {p.Items} items, ${p.TotalCents / 100.0:F2} via {p.PaymentMethod}");

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================
public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public Dictionary<string, string> Metadata { get; set; } = new();  // Map(String, String)
    public Dictionary<string, int> Counters { get; set; } = new();     // Map(String, Int32)
}

// ============================================================
// DbContext Definition
// ============================================================
public class EventDbContext : DbContext
{
    public DbSet<AnalyticsEvent> Events => Set<AnalyticsEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=map_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AnalyticsEvent>(entity =>
        {
            entity.ToTable("AnalyticsEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByMonth(x => x.Timestamp);
            // Map properties work automatically
        });
    }
}
