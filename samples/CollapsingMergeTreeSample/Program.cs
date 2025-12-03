using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// CollapsingMergeTree Sample
// ============================================================
// Demonstrates state tracking using CollapsingMergeTree.
// Rows with Sign=+1 add state, Sign=-1 cancels matching state.
// This enables efficient "updates" and "deletes" in ClickHouse.
// ============================================================

Console.WriteLine("CollapsingMergeTree Sample");
Console.WriteLine("==========================\n");

await using var context = new SessionDbContext();

// Create the database and tables
Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Track user sessions - each user has an active session with stats
Console.WriteLine("--- Tracking User Sessions ---\n");

// User 1 starts a session
Console.WriteLine("User 1 starts session (pageViews=1, duration=10)");
context.Sessions.Add(new UserSession
{
    UserId = 1,
    PageViews = 1,
    DurationSeconds = 10,
    Sign = 1  // +1 = add state
});
await context.SaveChangesAsync();

// User 2 starts a session
Console.WriteLine("User 2 starts session (pageViews=1, duration=5)");
context.Sessions.Add(new UserSession
{
    UserId = 2,
    PageViews = 1,
    DurationSeconds = 5,
    Sign = 1
});
await context.SaveChangesAsync();

// Query current state (sum with sign)
await PrintCurrentSessions("After initial sessions");

// User 1's session updates - cancel old, add new
Console.WriteLine("\n--- User 1 session update ---");
Console.WriteLine("Cancel old: (pageViews=1, duration=10, sign=-1)");
Console.WriteLine("Add new:    (pageViews=5, duration=60, sign=+1)");

context.Sessions.AddRange(new[]
{
    // Cancel the old state (must match exactly)
    new UserSession
    {
        UserId = 1,
        PageViews = 1,         // Must match original
        DurationSeconds = 10,  // Must match original
        Sign = -1              // Cancel
    },
    // Add the new state
    new UserSession
    {
        UserId = 1,
        PageViews = 5,         // Updated value
        DurationSeconds = 60,  // Updated value
        Sign = 1               // Add
    }
});
await context.SaveChangesAsync();

await PrintCurrentSessions("After User 1 update");

// User 2 ends session (cancel without replacement)
Console.WriteLine("\n--- User 2 session ends ---");
Console.WriteLine("Cancel: (pageViews=1, duration=5, sign=-1)");

context.Sessions.Add(new UserSession
{
    UserId = 2,
    PageViews = 1,
    DurationSeconds = 5,
    Sign = -1  // Cancel, no replacement = session ended
});
await context.SaveChangesAsync();

await PrintCurrentSessions("After User 2 ends");

// User 1 continues browsing
Console.WriteLine("\n--- User 1 continues ---");
context.Sessions.AddRange(new[]
{
    new UserSession { UserId = 1, PageViews = 5, DurationSeconds = 60, Sign = -1 },
    new UserSession { UserId = 1, PageViews = 10, DurationSeconds = 120, Sign = 1 }
});
await context.SaveChangesAsync();

await PrintCurrentSessions("After User 1 continues");

// Show physical rows before merge
Console.WriteLine("\n--- Physical Storage ---");
var physicalRows = await context.Sessions.CountAsync();
Console.WriteLine($"Physical rows before OPTIMIZE: {physicalRows}");

// Force merge
await context.Database.ExecuteSqlRawAsync(@"OPTIMIZE TABLE ""UserSessions"" FINAL");

physicalRows = await context.Sessions.CountAsync();
Console.WriteLine($"Physical rows after OPTIMIZE:  {physicalRows}");

// Show remaining rows
Console.WriteLine("\n--- Remaining Physical Rows ---");
var remaining = await context.Sessions.ToListAsync();
foreach (var row in remaining)
{
    Console.WriteLine($"  UserId={row.UserId}, PageViews={row.PageViews}, Duration={row.DurationSeconds}s, Sign={row.Sign}");
}

Console.WriteLine("\nDone!");

// Helper to print current session states
async Task PrintCurrentSessions(string label)
{
    Console.WriteLine($"\n{label}:");

    var sessions = await context.Sessions
        .GroupBy(s => s.UserId)
        .Select(g => new
        {
            UserId = g.Key,
            PageViews = g.Sum(s => s.PageViews * s.Sign),
            Duration = g.Sum(s => s.DurationSeconds * s.Sign),
            IsActive = g.Sum(s => s.Sign) > 0
        })
        .ToListAsync();

    foreach (var session in sessions.OrderBy(s => s.UserId))
    {
        var status = session.IsActive ? "ACTIVE" : "ended";
        Console.WriteLine($"  User {session.UserId}: pageViews={session.PageViews}, duration={session.Duration}s [{status}]");
    }

    var totals = sessions.Where(s => s.IsActive).ToList();
    Console.WriteLine($"  --- Active sessions: {totals.Count}, Total pageViews: {totals.Sum(s => s.PageViews)}");
}

// ============================================================
// Entity Definition
// ============================================================
public class UserSession
{
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public int DurationSeconds { get; set; }
    public sbyte Sign { get; set; }  // +1 = add, -1 = cancel
}

// ============================================================
// DbContext Definition
// ============================================================
public class SessionDbContext : DbContext
{
    public DbSet<UserSession> Sessions => Set<UserSession>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=collapsing_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserSession>(entity =>
        {
            entity.ToTable("UserSessions");
            entity.HasNoKey();

            // CollapsingMergeTree configuration:
            // - signColumn: Sign (+1/-1 for add/cancel)
            // - orderByColumn: UserId (collapsing key)
            entity.UseCollapsingMergeTree(
                signColumn: x => x.Sign,
                orderByColumn: x => x.UserId);
        });
    }
}
