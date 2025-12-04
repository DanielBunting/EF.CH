using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Query Modifiers Sample
// ============================================================
// Demonstrates ClickHouse-specific query modifiers:
// - Final() for ReplacingMergeTree deduplication
// - Sample() for probabilistic sampling
// - WithSettings() for query-level settings
// ============================================================

Console.WriteLine("Query Modifiers Sample");
Console.WriteLine("======================\n");

await using var context = new AnalyticsDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert user profiles with multiple versions
Console.WriteLine("Inserting user profiles (with version updates)...\n");

var userId1 = Guid.NewGuid();
var userId2 = Guid.NewGuid();
var userId3 = Guid.NewGuid();

// User 1: Multiple versions
context.Users.Add(new UserProfile
{
    Id = userId1,
    Email = "alice@example.com",
    Name = "Alice",
    UpdatedAt = DateTime.UtcNow.AddHours(-2)
});
await context.SaveChangesAsync();

// Update user 1 (insert new version)
context.Users.Add(new UserProfile
{
    Id = userId1,
    Email = "alice@example.com",
    Name = "Alice Smith",  // Updated name
    UpdatedAt = DateTime.UtcNow
});
await context.SaveChangesAsync();

// User 2: Single version
context.Users.Add(new UserProfile
{
    Id = userId2,
    Email = "bob@example.com",
    Name = "Bob",
    UpdatedAt = DateTime.UtcNow.AddHours(-1)
});

// User 3: Multiple versions
context.Users.Add(new UserProfile
{
    Id = userId3,
    Email = "charlie@example.com",
    Name = "Charlie",
    UpdatedAt = DateTime.UtcNow.AddHours(-3)
});
await context.SaveChangesAsync();

context.Users.Add(new UserProfile
{
    Id = userId3,
    Email = "charlie.updated@example.com",  // Updated email
    Name = "Charlie Brown",  // Updated name
    UpdatedAt = DateTime.UtcNow.AddMinutes(-30)
});
await context.SaveChangesAsync();

Console.WriteLine("Inserted users with version history.\n");

// Insert events for sampling
Console.WriteLine("Inserting events for sampling demo...\n");

var random = new Random(42);
var events = new List<AnalyticsEvent>();

for (var i = 0; i < 1000; i++)
{
    events.Add(new AnalyticsEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = DateTime.UtcNow.AddMinutes(-random.Next(10000)),
        EventType = random.Next(3) switch
        {
            0 => "page_view",
            1 => "click",
            _ => "purchase"
        },
        UserId = random.Next(100).ToString(),
        Value = random.NextDouble() * 100
    });
}

context.Events.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Count} events.\n");

// Demonstrate Final() - without Final, may see duplicate rows
Console.WriteLine("--- Without Final() - may show duplicate versions ---");
var usersWithoutFinal = await context.Users
    .OrderBy(u => u.Email)
    .ToListAsync();

foreach (var user in usersWithoutFinal)
{
    Console.WriteLine($"  {user.Email}: {user.Name} (updated {user.UpdatedAt:HH:mm:ss})");
}
Console.WriteLine($"Total rows: {usersWithoutFinal.Count}\n");

// With Final() - forces deduplication
Console.WriteLine("--- With Final() - shows only latest versions ---");
var usersWithFinal = await context.Users
    .Final()  // Forces on-the-fly deduplication
    .OrderBy(u => u.Email)
    .ToListAsync();

foreach (var user in usersWithFinal)
{
    Console.WriteLine($"  {user.Email}: {user.Name} (updated {user.UpdatedAt:HH:mm:ss})");
}
Console.WriteLine($"Total rows: {usersWithFinal.Count}\n");

// Demonstrate Sample() - probabilistic sampling
Console.WriteLine("--- Sample(0.1) - approximately 10% of events ---");
var sampledEvents = await context.Events
    .Sample(0.1)  // Sample ~10% of rows
    .ToListAsync();

var purchaseCount = sampledEvents.Count(e => e.EventType == "purchase");
Console.WriteLine($"Sampled {sampledEvents.Count} events (expected ~100)");
Console.WriteLine($"Purchase events in sample: {purchaseCount}");
Console.WriteLine($"Extrapolated total purchases: ~{purchaseCount * 10}\n");

// Demonstrate WithSettings() - query-level settings
Console.WriteLine("--- WithSettings() - controlling query execution ---");
var eventsWithSettings = await context.Events
    .WithSettings(new Dictionary<string, object>
    {
        ["max_threads"] = 2,
        ["max_execution_time"] = 30
    })
    .Where(e => e.EventType == "purchase")
    .ToListAsync();

Console.WriteLine($"Found {eventsWithSettings.Count} purchase events with custom settings.\n");

// Single setting shorthand
Console.WriteLine("--- WithSetting() - single setting shorthand ---");
var limitedEvents = await context.Events
    .WithSetting("max_rows_to_read", 500)
    .Take(100)
    .ToListAsync();

Console.WriteLine($"Retrieved {limitedEvents.Count} events with row limit setting.\n");

// Combining modifiers
Console.WriteLine("--- Combining Final() with other operations ---");
var activeUsers = await context.Users
    .Final()
    .Where(u => u.Email.Contains("@example.com"))
    .OrderByDescending(u => u.UpdatedAt)
    .ToListAsync();

Console.WriteLine($"Found {activeUsers.Count} deduplicated active users.\n");

Console.WriteLine("Done!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// User profile using ReplacingMergeTree for update semantics.
/// </summary>
public class UserProfile
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }
}

/// <summary>
/// Analytics event with SAMPLE BY for probabilistic sampling.
/// </summary>
public class AnalyticsEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public double Value { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class AnalyticsDbContext : DbContext
{
    public DbSet<UserProfile> Users => Set<UserProfile>();
    public DbSet<AnalyticsEvent> Events => Set<AnalyticsEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=query_modifiers_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Users: ReplacingMergeTree for deduplication
        // Final() forces on-the-fly deduplication
        modelBuilder.Entity<UserProfile>(entity =>
        {
            entity.ToTable("Users");
            entity.HasKey(e => e.Id);

            // ReplacingMergeTree keeps latest version by UpdatedAt
            entity.UseReplacingMergeTree(
                versionColumnExpression: x => x.UpdatedAt,
                orderByExpression: x => x.Id);
        });

        // Events: MergeTree with SAMPLE BY for probabilistic sampling
        modelBuilder.Entity<AnalyticsEvent>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByMonth(x => x.Timestamp);

            // Enable sampling - required for Sample() to work
            entity.HasSampleBy("intHash32(Id)");
        });
    }
}
