using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// OPTIMIZE TABLE Sample
// ============================================================
// Demonstrates forcing merges in ClickHouse using OPTIMIZE TABLE.
// This is useful for:
// - ReplacingMergeTree: Force deduplication
// - SummingMergeTree: Force aggregation
// - TTL: Force expiration of old data
// ============================================================

Console.WriteLine("OPTIMIZE TABLE Sample");
Console.WriteLine("=====================\n");

await using var context = new UserDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert user with initial version
Console.WriteLine("\nInserting user profile...");
var userId = Guid.NewGuid();

context.Users.Add(new UserProfile
{
    Id = userId,
    Email = "alice@example.com",
    Name = "Alice",
    UpdatedAt = DateTime.UtcNow.AddHours(-2)
});
await context.SaveChangesAsync();

Console.WriteLine($"Inserted user: Alice (Id: {userId})");

// "Update" by inserting a new version
Console.WriteLine("\nUpdating user (inserting new version)...");
context.Users.Add(new UserProfile
{
    Id = userId,  // Same ID
    Email = "alice@example.com",
    Name = "Alice Smith",  // Updated name
    UpdatedAt = DateTime.UtcNow  // Newer timestamp
});
await context.SaveChangesAsync();

Console.WriteLine("Inserted updated version: Alice Smith");

// Query WITHOUT FINAL - may see both versions
Console.WriteLine("\n--- Query without FINAL (may show duplicates) ---");
context.ChangeTracker.Clear();
var usersBeforeOptimize = await context.Users.ToListAsync();
Console.WriteLine($"Row count: {usersBeforeOptimize.Count}");
foreach (var user in usersBeforeOptimize.OrderBy(u => u.UpdatedAt))
{
    Console.WriteLine($"  {user.Name} (updated: {user.UpdatedAt:HH:mm:ss})");
}

// Query WITH FINAL - shows deduplicated result
Console.WriteLine("\n--- Query with FINAL (shows latest version only) ---");
var usersWithFinal = await context.Users.Final().ToListAsync();
Console.WriteLine($"Row count: {usersWithFinal.Count}");
foreach (var user in usersWithFinal)
{
    Console.WriteLine($"  {user.Name} (updated: {user.UpdatedAt:HH:mm:ss})");
}

// OPTIMIZE TABLE FINAL - force merge to persist deduplication
Console.WriteLine("\n--- Running OPTIMIZE TABLE FINAL ---");
Console.WriteLine("This forces ClickHouse to merge data parts and deduplicate...");

await context.Database.OptimizeTableFinalAsync<UserProfile>();

Console.WriteLine("Optimization complete!");

// Query again without FINAL - should now show single row
Console.WriteLine("\n--- Query without FINAL (after optimization) ---");
context.ChangeTracker.Clear();
var usersAfterOptimize = await context.Users.ToListAsync();
Console.WriteLine($"Row count: {usersAfterOptimize.Count}");
foreach (var user in usersAfterOptimize)
{
    Console.WriteLine($"  {user.Name} (updated: {user.UpdatedAt:HH:mm:ss})");
}

// Demonstrate partition-specific optimization
Console.WriteLine("\n--- Demonstrating partition optimization ---");

// Insert events across partitions
context.Events.AddRange(new[]
{
    new Event { Id = Guid.NewGuid(), Timestamp = new DateTime(2024, 1, 15), EventType = "login" },
    new Event { Id = Guid.NewGuid(), Timestamp = new DateTime(2024, 2, 15), EventType = "logout" },
});
await context.SaveChangesAsync();

Console.WriteLine("Inserted events in January and February partitions.");

// Optimize only January partition
Console.WriteLine("Optimizing January 2024 partition only...");
await context.Database.OptimizeTablePartitionFinalAsync<Event>("202401");
Console.WriteLine("Partition 202401 optimized.");

// Demonstrate advanced options
Console.WriteLine("\n--- Using advanced options ---");
await context.Database.OptimizeTableAsync<Event>(o => o
    .WithPartition("202402")
    .WithFinal()
    .WithDeduplicate());
Console.WriteLine("Partition 202402 optimized with DEDUPLICATE.");

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

public class UserProfile
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }
}

public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class UserDbContext : DbContext
{
    public DbSet<UserProfile> Users => Set<UserProfile>();
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=optimize_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // ReplacingMergeTree - keeps latest row by UpdatedAt
        modelBuilder.Entity<UserProfile>(entity =>
        {
            entity.ToTable("Users");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree(
                versionColumnExpression: x => x.UpdatedAt,
                orderByExpression: x => x.Id);
        });

        // Partitioned MergeTree for events
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByMonth(x => x.Timestamp);
        });
    }
}
