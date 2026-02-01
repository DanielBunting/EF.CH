using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// ARRAY JOIN Sample
// ============================================================
// Demonstrates ClickHouse ARRAY JOIN for array explosion:
// - Basic ARRAY JOIN to explode tags
// - LEFT ARRAY JOIN to preserve events with no tags
// - Combining with GROUP BY for tag analytics
//
// Note: ARRAY JOIN uses raw SQL because EF Core's query pipeline
// doesn't support custom join types in LINQ extensions.
// ============================================================

Console.WriteLine("ARRAY JOIN Sample");
Console.WriteLine("==================\n");

await using var context = new EventsDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureDeletedAsync();
await context.Database.EnsureCreatedAsync();

// Insert events with tags
Console.WriteLine("Inserting events with tags...\n");

var events = new List<Event>
{
    new() { Id = Guid.NewGuid(), Name = "Login", Tags = new[] { "auth", "user", "security" } },
    new() { Id = Guid.NewGuid(), Name = "Purchase", Tags = new[] { "commerce", "payment" } },
    new() { Id = Guid.NewGuid(), Name = "Logout", Tags = new[] { "auth", "user" } },
    new() { Id = Guid.NewGuid(), Name = "SystemCheck", Tags = Array.Empty<string>() },  // No tags
};
context.Events.AddRange(events);
await context.SaveChangesAsync();

Console.WriteLine($"Inserted {events.Count} events.\n");

// Basic ARRAY JOIN - explode tags using raw SQL
Console.WriteLine("--- ARRAY JOIN: Explode Tags ---");
var arrayJoinSql = @"
    SELECT e.Name, tag AS Tag
    FROM ""Events"" AS e
    ARRAY JOIN e.Tags AS tag
    ORDER BY e.Name, tag";

var exploded = await context.Database.SqlQueryRaw<EventWithTag>(arrayJoinSql).ToListAsync();

foreach (var row in exploded)
{
    Console.WriteLine($"  {row.Name}: {row.Tag}");
}
Console.WriteLine($"Total rows: {exploded.Count} (SystemCheck excluded - no tags)\n");

// LEFT ARRAY JOIN - preserve events with empty arrays
Console.WriteLine("--- LEFT ARRAY JOIN: Include Events Without Tags ---");
var leftArrayJoinSql = @"
    SELECT e.Name, IF(tag = '', '(no tag)', tag) AS Tag
    FROM ""Events"" AS e
    LEFT ARRAY JOIN e.Tags AS tag
    ORDER BY e.Name";

var allExploded = await context.Database.SqlQueryRaw<EventWithTag>(leftArrayJoinSql).ToListAsync();

foreach (var row in allExploded)
{
    Console.WriteLine($"  {row.Name}: {row.Tag}");
}
Console.WriteLine($"Total rows: {allExploded.Count} (includes SystemCheck)\n");

// Tag analytics with GROUP BY
Console.WriteLine("--- Tag Analytics: ARRAY JOIN + GROUP BY ---");
var tagCountsSql = @"
    SELECT tag AS Tag, count() AS Count
    FROM ""Events"" AS e
    ARRAY JOIN e.Tags AS tag
    GROUP BY tag
    ORDER BY Count DESC, tag";

var tagCounts = await context.Database.SqlQueryRaw<TagCount>(tagCountsSql).ToListAsync();

foreach (var row in tagCounts)
{
    Console.WriteLine($"  {row.Tag}: {row.Count} events");
}

// Filter for specific tags
Console.WriteLine("\n--- Filter: Only 'auth' Tagged Events ---");
var filterSql = @"
    SELECT e.Name, tag AS Tag
    FROM ""Events"" AS e
    ARRAY JOIN e.Tags AS tag
    WHERE tag = 'auth'";

var authEvents = await context.Database.SqlQueryRaw<EventWithTag>(filterSql).ToListAsync();

foreach (var row in authEvents)
{
    Console.WriteLine($"  {row.Name}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Result Types for Raw SQL Queries
// ============================================================

public class EventWithTag
{
    public string Name { get; set; } = string.Empty;
    public string Tag { get; set; } = string.Empty;
}

public class TagCount
{
    public string Tag { get; set; } = string.Empty;
    public ulong Count { get; set; }
}

// ============================================================
// Entity Definitions
// ============================================================

public class Event
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[] Tags { get; set; } = Array.Empty<string>();
}

// ============================================================
// DbContext Definition
// ============================================================

public class EventsDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=array_join_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}
