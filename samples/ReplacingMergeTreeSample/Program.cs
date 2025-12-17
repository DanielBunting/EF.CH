using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// ReplacingMergeTree Sample
// ============================================================
// Demonstrates "update" semantics using ReplacingMergeTree.
// ClickHouse doesn't support UPDATE, but ReplacingMergeTree
// deduplicates rows by key, keeping the highest version.
// ============================================================

Console.WriteLine("ReplacingMergeTree Sample");
Console.WriteLine("=========================\n");

await using var context = new UserDbContext();

// Create the database and tables
Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert initial user
var userId = Guid.NewGuid();
Console.WriteLine($"Creating user with ID: {userId}");

context.Users.Add(new User
{
    Id = userId,
    Email = "alice@example.com",
    Name = "Alice",
    Bio = "Original bio",
    CreatedAt = DateTime.UtcNow,
    UpdatedAt = DateTime.UtcNow
});
await context.SaveChangesAsync();

// Query without FINAL (may see duplicates before merge)
Console.WriteLine("\nAfter initial insert:");
var userBeforeUpdate = await context.Users.FirstOrDefaultAsync(u => u.Id == userId);
Console.WriteLine($"  Name: {userBeforeUpdate?.Name}, Bio: {userBeforeUpdate?.Bio}");

// "Update" the user by inserting a new version
Console.WriteLine("\n\"Updating\" user by inserting new version...");
context.Users.Add(new User
{
    Id = userId,  // Same ID - will be deduplicated
    Email = "alice@example.com",
    Name = "Alice Smith",  // Updated name
    Bio = "Updated bio - I love ClickHouse!",  // Updated bio
    CreatedAt = userBeforeUpdate!.CreatedAt,  // Keep original
    UpdatedAt = DateTime.UtcNow  // New version (higher = newer)
});
await context.SaveChangesAsync();

// Query with FINAL to see deduplicated result
Console.WriteLine("\nAfter update (with FINAL for accurate results):");
var userAfterUpdate = await context.Users
    .Final()  // Force deduplication
    .FirstOrDefaultAsync(u => u.Id == userId);
Console.WriteLine($"  Name: {userAfterUpdate?.Name}");
Console.WriteLine($"  Bio: {userAfterUpdate?.Bio}");
Console.WriteLine($"  Updated: {userAfterUpdate?.UpdatedAt}");

// Demonstrate multiple updates
Console.WriteLine("\nPerforming multiple updates...");
for (int i = 1; i <= 3; i++)
{
    await Task.Delay(100);  // Small delay to ensure different timestamps
    context.Users.Add(new User
    {
        Id = userId,
        Email = "alice@example.com",
        Name = $"Alice Smith (v{i + 1})",
        Bio = $"Bio version {i + 1}",
        CreatedAt = userAfterUpdate!.CreatedAt,
        UpdatedAt = DateTime.UtcNow
    });
    await context.SaveChangesAsync();
    Console.WriteLine($"  Inserted version {i + 1}");
}

// Without FINAL - may see multiple rows
Console.WriteLine("\nQuery WITHOUT FINAL (may see unmerged rows):");
var allVersions = await context.Users
    .Where(u => u.Id == userId)
    .ToListAsync();
Console.WriteLine($"  Found {allVersions.Count} row(s)");

// With FINAL - guaranteed single row with latest version
Console.WriteLine("\nQuery WITH FINAL (deduplicated):");
var latestVersion = await context.Users
    .Final()
    .FirstOrDefaultAsync(u => u.Id == userId);
Console.WriteLine($"  Name: {latestVersion?.Name}");
Console.WriteLine($"  Bio: {latestVersion?.Bio}");

// Force merge to physically deduplicate (expensive, don't do often)
Console.WriteLine("\nForcing OPTIMIZE FINAL (physically deduplicates)...");
await context.Database.ExecuteSqlRawAsync(@"OPTIMIZE TABLE ""Users"" FINAL");

Console.WriteLine("\nAfter OPTIMIZE:");
var afterOptimize = await context.Users
    .Where(u => u.Id == userId)
    .ToListAsync();
Console.WriteLine($"  Found {afterOptimize.Count} row(s)");
Console.WriteLine($"  Name: {afterOptimize.First().Name}");

Console.WriteLine("\nDone!");

// ============================================================
// Demonstrate is_deleted Column (ClickHouse 23.2+)
// ============================================================
Console.WriteLine("\n\n========================================");
Console.WriteLine("is_deleted Column Demo (ClickHouse 23.2+)");
Console.WriteLine("========================================\n");

await using var deletableContext = new DeletableUserDbContext();
await deletableContext.Database.EnsureCreatedAsync();

// Insert a user
var deletableUserId = Guid.NewGuid();
Console.WriteLine($"Creating user with ID: {deletableUserId}");
deletableContext.DeletableUsers.Add(new DeletableUser
{
    Id = deletableUserId,
    Name = "Bob",
    Email = "bob@example.com",
    Version = 1,
    IsDeleted = 0  // Active
});
await deletableContext.SaveChangesAsync();

// Query shows 1 active user
var activeCount = await deletableContext.Database
    .SqlQueryRaw<long>(@"SELECT count() as Value FROM ""DeletableUsers"" FINAL WHERE ""IsDeleted"" = 0")
    .FirstAsync();
Console.WriteLine($"Active users: {activeCount}");

// Delete by inserting row with IsDeleted = 1
Console.WriteLine("\nDeleting user (inserting row with IsDeleted = 1)...");
deletableContext.DeletableUsers.Add(new DeletableUser
{
    Id = deletableUserId,
    Name = "Bob",
    Email = "bob@example.com",
    Version = 2,       // Higher version wins
    IsDeleted = 1      // Mark as deleted
});
await deletableContext.SaveChangesAsync();

// Query with FINAL - automatically excludes deleted rows
activeCount = await deletableContext.Database
    .SqlQueryRaw<long>(@"SELECT count() as Value FROM ""DeletableUsers"" FINAL WHERE ""IsDeleted"" = 0")
    .FirstAsync();
Console.WriteLine($"Active users after delete: {activeCount}");

// Before OPTIMIZE, both rows exist
var totalRows = await deletableContext.Database
    .SqlQueryRaw<long>(@"SELECT count() as Value FROM ""DeletableUsers""")
    .FirstAsync();
Console.WriteLine($"Total rows (before OPTIMIZE): {totalRows}");

// OPTIMIZE physically removes deleted rows
Console.WriteLine("\nRunning OPTIMIZE FINAL...");
await deletableContext.Database.ExecuteSqlRawAsync(@"OPTIMIZE TABLE ""DeletableUsers"" FINAL");

totalRows = await deletableContext.Database
    .SqlQueryRaw<long>(@"SELECT count() as Value FROM ""DeletableUsers""")
    .FirstAsync();
Console.WriteLine($"Total rows (after OPTIMIZE): {totalRows}");
Console.WriteLine("Row was physically removed!");

Console.WriteLine("\nis_deleted Demo Done!");

// ============================================================
// Entity Definition
// ============================================================
public class User
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string? Bio { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }  // Version column - higher = newer
}

/// <summary>
/// Entity with is_deleted column for physical deletion (ClickHouse 23.2+).
/// </summary>
public class DeletableUser
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public long Version { get; set; }
    public byte IsDeleted { get; set; }  // UInt8: 0 = active, 1 = deleted
}

// ============================================================
// DbContext Definitions
// ============================================================

/// <summary>
/// Basic ReplacingMergeTree example - version column only.
/// </summary>
public class UserDbContext : DbContext
{
    public DbSet<User> Users => Set<User>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=replacing_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("Users");
            entity.HasKey(e => e.Id);

            // ReplacingMergeTree configuration:
            // - versionColumn: UpdatedAt (higher timestamp = newer version)
            // - orderBy: Id (deduplication key)
            entity.UseReplacingMergeTree(
                x => x.UpdatedAt,
                x => x.Id);
        });
    }
}

/// <summary>
/// ReplacingMergeTree with is_deleted column (ClickHouse 23.2+).
/// Rows with IsDeleted=1 are excluded by FINAL and physically removed during merge.
/// </summary>
public class DeletableUserDbContext : DbContext
{
    public DbSet<DeletableUser> DeletableUsers => Set<DeletableUser>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=replacing_sample_deletable");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DeletableUser>(entity =>
        {
            entity.ToTable("DeletableUsers");
            entity.HasKey(e => e.Id);

            // ReplacingMergeTree with is_deleted column:
            // - versionColumn: Version (higher = newer)
            // - isDeletedColumn: IsDeleted (UInt8: 0 = active, 1 = deleted)
            // - orderBy: Id (deduplication key)
            entity.UseReplacingMergeTree(
                x => x.Version,
                x => x.IsDeleted,
                x => x.Id);
        });
    }
}
