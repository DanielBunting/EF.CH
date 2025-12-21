using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;

// ============================================================
// JSON Type Sample
// ============================================================
// Demonstrates native JSON type support with ClickHouse 24.8+.
// Uses extension methods (GetPath, HasPath, GetPathOrDefault)
// that translate to native subcolumn syntax.
//
// Requirements: ClickHouse 24.8+
// ============================================================

Console.WriteLine("JSON Type Sample");
Console.WriteLine("================\n");

await using var context = new EventDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert events with JSON payloads
Console.WriteLine("Inserting events with JSON payloads...\n");

var events = new[]
{
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddHours(-5),
        EventType = "user_signup",
        Payload = JsonSerializer.SerializeToElement(new
        {
            user = new { email = "alice@example.com", name = "Alice Smith", tier = "premium" },
            source = "organic",
            metrics = new { score = 85, attempts = 1 }
        })
    },
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddHours(-4),
        EventType = "purchase",
        Payload = JsonSerializer.SerializeToElement(new
        {
            user = new { email = "alice@example.com", name = "Alice Smith" },
            amount = 149.99,
            items = new[] { "widget", "gadget" },
            shipping = new { city = "Seattle", country = "USA" }
        })
    },
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddHours(-3),
        EventType = "user_signup",
        Payload = JsonSerializer.SerializeToElement(new
        {
            user = new { email = "bob@example.com", name = "Bob Jones", tier = "free" },
            source = "referral",
            referrer_id = "user-123"
        })
    },
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddHours(-2),
        EventType = "purchase",
        Payload = JsonSerializer.SerializeToElement(new
        {
            user = new { email = "bob@example.com", name = "Bob Jones" },
            amount = 29.99,
            items = new[] { "accessory" },
            promo_code = "WELCOME10"
        })
    },
    new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddHours(-1),
        EventType = "page_view",
        Payload = JsonSerializer.SerializeToElement(new
        {
            user = new { email = "alice@example.com" },
            page = "/products",
            duration_seconds = 45
        })
    }
};

context.Events.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Length} events.\n");

// Query: GetPath - extract typed values from JSON
Console.WriteLine("--- User emails from all events (GetPath) ---");
var userEmails = await context.Events
    .Select(e => new
    {
        e.EventType,
        Email = e.Payload.GetPath<string>("user.email")
    })
    .ToListAsync();

foreach (var item in userEmails)
    Console.WriteLine($"  {item.EventType}: {item.Email}");

// Query: Filter by JSON path value
Console.WriteLine("\n--- Premium tier signups (filter by JSON path) ---");
var premiumSignups = await context.Events
    .Where(e => e.EventType == "user_signup")
    .Where(e => e.Payload.GetPath<string>("user.tier") == "premium")
    .Select(e => new
    {
        Name = e.Payload.GetPath<string>("user.name"),
        Email = e.Payload.GetPath<string>("user.email")
    })
    .ToListAsync();

foreach (var item in premiumSignups)
    Console.WriteLine($"  {item.Name} ({item.Email})");

// Query: HasPath - check if path exists
Console.WriteLine("\n--- Events with promo codes (HasPath) ---");
var promoEvents = await context.Events
    .Where(e => e.Payload.HasPath("promo_code"))
    .Select(e => new
    {
        e.EventType,
        PromoCode = e.Payload.GetPath<string>("promo_code")
    })
    .ToListAsync();

foreach (var item in promoEvents)
    Console.WriteLine($"  {item.EventType}: {item.PromoCode}");

// Query: GetPathOrDefault - fallback for missing values
// Note: Only testing with events that have the metrics.score path
Console.WriteLine("\n--- Metrics scores (GetPath with HasPath filter) ---");
var scores = await context.Events
    .Where(e => e.Payload.HasPath("metrics.score"))
    .Select(e => new
    {
        e.EventType,
        Score = e.Payload.GetPath<long>("metrics.score")
    })
    .ToListAsync();

foreach (var item in scores)
    Console.WriteLine($"  {item.EventType}: score = {item.Score}");

// Query: Nested object access
Console.WriteLine("\n--- Shipping cities from purchases (nested path) ---");
var shippingInfo = await context.Events
    .Where(e => e.EventType == "purchase")
    .Where(e => e.Payload.HasPath("shipping.city"))
    .Select(e => new
    {
        Email = e.Payload.GetPath<string>("user.email"),
        City = e.Payload.GetPath<string>("shipping.city"),
        Country = e.Payload.GetPath<string>("shipping.country")
    })
    .ToListAsync();

foreach (var item in shippingInfo)
    Console.WriteLine($"  {item.Email}: {item.City}, {item.Country}");

// Note: Array index access (items[0]) works with JSON subcolumn syntax but
// requires the path to be explicitly typed in ClickHouse 24.8's Dynamic type system.
// For now, we skip this query as it requires additional type casting.
// See ClickHouse docs for JSON Dynamic type handling.

// Note: Aggregate functions on JSON paths require explicit type casting in ClickHouse 24.8
// because JSON dynamic paths return the "Dynamic" type. For aggregation scenarios,
// consider extracting values to regular columns or using ClickHouse's CAST functions.

// Query: Order by JSON path (works for SELECT, not aggregates)
Console.WriteLine("\n--- Purchases ordered by amount (descending) ---");
var orderedPurchases = await context.Events
    .Where(e => e.EventType == "purchase")
    .OrderByDescending(e => e.Payload.GetPath<double>("amount"))
    .Select(e => new
    {
        Email = e.Payload.GetPath<string>("user.email"),
        Amount = e.Payload.GetPath<double>("amount")
    })
    .ToListAsync();

foreach (var item in orderedPurchases)
    Console.WriteLine($"  {item.Email}: ${item.Amount:F2}");

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public JsonElement Payload { get; set; }  // JSON column
}

// ============================================================
// Typed POCO Example (alternative approach)
// ============================================================
public class OrderMetadata
{
    public string CustomerName { get; set; } = string.Empty;
    public string CustomerEmail { get; set; } = string.Empty;
    public ShippingAddress? ShippingAddress { get; set; }
}

public class ShippingAddress
{
    public string City { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
}

public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }

    [ClickHouseJson(IsTyped = true)]
    public OrderMetadata Metadata { get; set; } = new();
}

// ============================================================
// DbContext Definition
// ============================================================
public class EventDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=json_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });

            // Configure JSON column with optional parameters
            entity.Property(e => e.Payload)
                .HasColumnType("JSON")
                .HasMaxDynamicPaths(1024);  // Optional: default is 1024
        });
    }
}
