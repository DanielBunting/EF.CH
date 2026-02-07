using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// CTE (Common Table Expression) Sample
// ============================================================
// Demonstrates ClickHouse CTEs via AsCte():
// - Basic CTE with filter
// - CTE with ordering and limit
// - CTE for analytical queries
// - Generates WITH "name" AS (...) SELECT ... FROM "name"
// ============================================================

Console.WriteLine("CTE (Common Table Expression) Sample");
Console.WriteLine("=====================================\n");

await using var context = new AnalyticsDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert analytics events
Console.WriteLine("Inserting analytics events...\n");

var random = new Random(42);
var categories = new[] { "page_view", "click", "purchase", "signup", "logout" };
var regions = new[] { "US", "EU", "APAC" };

var events = new List<AnalyticsEvent>();
for (var i = 0; i < 100; i++)
{
    events.Add(new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        EventType = categories[random.Next(categories.Length)],
        Region = regions[random.Next(regions.Length)],
        Amount = Math.Round((decimal)(random.NextDouble() * 500), 2),
        CreatedAt = DateTime.UtcNow.AddHours(-random.Next(720)) // Last 30 days
    });
}

context.Events.AddRange(events);
await context.SaveChangesAsync();
context.ChangeTracker.Clear();
Console.WriteLine($"Inserted {events.Count} events.\n");

// Basic CTE: filter recent events
Console.WriteLine("--- Basic CTE: Recent purchases ---");
Console.WriteLine("Query: .Where(recent).AsCte(\"recent_purchases\").OrderBy().Take()");
Console.WriteLine("SQL: WITH \"recent_purchases\" AS (SELECT ... WHERE ...) SELECT ... FROM \"recent_purchases\"\n");

var cutoff = DateTime.UtcNow.AddDays(-7);
var recentPurchases = await context.Events
    .Where(e => e.EventType == "purchase" && e.CreatedAt > cutoff)
    .AsCte("recent_purchases")
    .OrderByDescending(e => e.Amount)
    .Take(10)
    .ToListAsync();

Console.WriteLine($"Top {recentPurchases.Count} recent purchases:");
foreach (var e in recentPurchases)
{
    Console.WriteLine($"  [{e.Region}] ${e.Amount:F2} at {e.CreatedAt:yyyy-MM-dd HH:mm}");
}

// CTE with complex filter
Console.WriteLine("\n--- CTE: High-value events by region ---");
Console.WriteLine("Wraps complex filter as a named CTE for clarity.\n");

var highValue = await context.Events
    .Where(e => e.Amount > 200 && e.EventType != "logout")
    .AsCte("high_value")
    .OrderBy(e => e.Region)
    .ThenByDescending(e => e.Amount)
    .ToListAsync();

Console.WriteLine($"High-value events: {highValue.Count}");
var byRegion = highValue.GroupBy(e => e.Region);
foreach (var group in byRegion)
{
    Console.WriteLine($"  {group.Key}: {group.Count()} events, total ${group.Sum(e => e.Amount):F2}");
}

// CTE for analytical query pattern
Console.WriteLine("\n--- CTE: Analytical query with Take ---");
Console.WriteLine("Uses CTE to logically separate data preparation from result shaping.\n");

var analyticsResult = await context.Events
    .Where(e => e.Region == "US")
    .AsCte("us_events")
    .OrderByDescending(e => e.CreatedAt)
    .Take(20)
    .ToListAsync();

Console.WriteLine($"Latest {analyticsResult.Count} US events:");
foreach (var e in analyticsResult.Take(5))
{
    Console.WriteLine($"  {e.EventType} ${e.Amount:F2} at {e.CreatedAt:yyyy-MM-dd HH:mm}");
}
if (analyticsResult.Count > 5)
{
    Console.WriteLine($"  ... and {analyticsResult.Count - 5} more");
}

Console.WriteLine("\n--- Notes ---");
Console.WriteLine("- AsCte() wraps the preceding query as a WITH clause");
Console.WriteLine("- Operations after AsCte() operate on the CTE reference");
Console.WriteLine("- Single CTE per query (multi-CTE planned for future)");
Console.WriteLine("- No recursive CTEs (limited ClickHouse support)");

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================

public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime CreatedAt { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class AnalyticsDbContext : DbContext
{
    public DbSet<AnalyticsEvent> Events => Set<AnalyticsEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=cte_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AnalyticsEvent>(entity =>
        {
            entity.ToTable("AnalyticsEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
            entity.HasPartitionByMonth(x => x.CreatedAt);
        });
    }
}
