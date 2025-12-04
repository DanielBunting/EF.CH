using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Delete Strategies Sample
// ============================================================
// Demonstrates ClickHouse delete operations:
// - Single entity delete via change tracker
// - Bulk delete via ExecuteDeleteAsync
// - Lightweight vs Mutation delete strategies
// ============================================================

Console.WriteLine("Delete Strategies Sample");
Console.WriteLine("========================\n");

// Use lightweight delete (default)
await using var context = new EventDbContext(ClickHouseDeleteStrategy.Lightweight);

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert events
Console.WriteLine("Inserting events...\n");

var events = new[]
{
    new Event { Id = Guid.NewGuid(), Name = "Event A", Category = "important", CreatedAt = DateTime.UtcNow.AddDays(-10) },
    new Event { Id = Guid.NewGuid(), Name = "Event B", Category = "temporary", CreatedAt = DateTime.UtcNow.AddDays(-5) },
    new Event { Id = Guid.NewGuid(), Name = "Event C", Category = "temporary", CreatedAt = DateTime.UtcNow.AddDays(-3) },
    new Event { Id = Guid.NewGuid(), Name = "Event D", Category = "important", CreatedAt = DateTime.UtcNow.AddDays(-1) },
    new Event { Id = Guid.NewGuid(), Name = "Event E", Category = "temporary", CreatedAt = DateTime.UtcNow },
};

context.Events.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Length} events.\n");

// Show initial state
Console.WriteLine("--- Initial Events ---");
await PrintEventsAsync(context);

// Single entity delete via change tracker
Console.WriteLine("\n--- Delete single entity via change tracker ---");
var eventToDelete = await context.Events.FirstAsync(e => e.Name == "Event A");
Console.WriteLine($"Deleting: {eventToDelete.Name}");

context.Events.Remove(eventToDelete);
await context.SaveChangesAsync();

context.ChangeTracker.Clear();
await PrintEventsAsync(context);

// Bulk delete via ExecuteDeleteAsync
Console.WriteLine("\n--- Bulk delete temporary events via ExecuteDeleteAsync ---");
Console.WriteLine("Deleting all events with Category = 'temporary'...");

await context.Events
    .Where(e => e.Category == "temporary")
    .ExecuteDeleteAsync();

await PrintEventsAsync(context);

// Demonstrate the difference between strategies
Console.WriteLine("\n--- Delete Strategy Comparison ---");
Console.WriteLine("Lightweight (default): DELETE FROM table WHERE ...");
Console.WriteLine("  - Instant marking, filtered immediately");
Console.WriteLine("  - Physical deletion during background merges");
Console.WriteLine("  - Best for normal operations\n");

Console.WriteLine("Mutation: ALTER TABLE table DELETE WHERE ...");
Console.WriteLine("  - Async operation, rewrites data parts");
Console.WriteLine("  - Does not return affected row count");
Console.WriteLine("  - Best for bulk maintenance only\n");

// Re-insert for mutation demo
Console.WriteLine("--- Re-inserting events for mutation strategy demo ---");
context.Events.AddRange(new[]
{
    new Event { Id = Guid.NewGuid(), Name = "Mutation Test 1", Category = "test", CreatedAt = DateTime.UtcNow },
    new Event { Id = Guid.NewGuid(), Name = "Mutation Test 2", Category = "test", CreatedAt = DateTime.UtcNow },
});
await context.SaveChangesAsync();

await PrintEventsAsync(context);

// Create new context with mutation strategy
Console.WriteLine("\n--- Using Mutation strategy ---");
await using var mutationContext = new EventDbContext(ClickHouseDeleteStrategy.Mutation);

await mutationContext.Events
    .Where(e => e.Category == "test")
    .ExecuteDeleteAsync();

Console.WriteLine("Mutation delete issued (async operation).\n");

// Wait a moment for mutation to complete
await Task.Delay(1000);

await PrintEventsAsync(mutationContext);

Console.WriteLine("\nDone!");

// Helper method
static async Task PrintEventsAsync(EventDbContext ctx)
{
    ctx.ChangeTracker.Clear();
    var all = await ctx.Events.OrderBy(e => e.CreatedAt).ToListAsync();
    Console.WriteLine($"Events ({all.Count}):");
    foreach (var e in all)
    {
        Console.WriteLine($"  {e.Name} [{e.Category}] - {e.CreatedAt:yyyy-MM-dd}");
    }
}

// ============================================================
// Entity Definition
// ============================================================

public class Event
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class EventDbContext : DbContext
{
    private readonly ClickHouseDeleteStrategy _deleteStrategy;

    public EventDbContext(ClickHouseDeleteStrategy deleteStrategy)
    {
        _deleteStrategy = deleteStrategy;
    }

    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse(
            "Host=localhost;Database=delete_sample",
            o => o.UseDeleteStrategy(_deleteStrategy));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
        });
    }
}
