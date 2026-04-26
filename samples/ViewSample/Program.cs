// -----------------------------------------------------------------
// ViewSample - Plain ClickHouse Views with EF.CH (LINQ-first)
// -----------------------------------------------------------------
// Demonstrates the LINQ-driven view fluent API:
//   1. AsView<TView, TSource> — define the view body with LINQ
//   2. EnsureViewAsync<T>     — deploy a single LINQ-defined view
//   3. EnsureViewsAsync       — deploy every LINQ-defined view in the model
//   4. AsView with OR REPLACE — re-deploy a view body without dropping
//   5. LINQ composition on the result via context.Set<T>() / FromView<T>
// -----------------------------------------------------------------

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:25.6")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();
    await SeedData(connectionString);

    await DemoAsViewWithLinq(connectionString);
    await DemoLinqComposedOnView(connectionString);
    await DemoEnsureViewsAsync(connectionString);
    await DemoOrReplaceFromLinq(connectionString);
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

static async Task SeedData(string connectionString)
{
    await using var context = new ViewDemoContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    await context.BulkInsertAsync(new List<User>
    {
        new() { UserId = 1, Name = "Alice",   IsActive = true,  LastSeen = DateTime.UtcNow.AddDays(-1) },
        new() { UserId = 2, Name = "Bob",     IsActive = false, LastSeen = DateTime.UtcNow.AddDays(-30) },
        new() { UserId = 3, Name = "Carol",   IsActive = true,  LastSeen = DateTime.UtcNow.AddHours(-2) },
        new() { UserId = 4, Name = "Dan",     IsActive = true,  LastSeen = DateTime.UtcNow.AddDays(-3) },
        new() { UserId = 5, Name = "Eve",     IsActive = false, LastSeen = DateTime.UtcNow.AddDays(-90) },
    });

    Console.WriteLine("Seeded 5 user records.\n");
}

// -----------------------------------------------------------------
// Demo 1: Define a view body with LINQ (AsView<TView, TSource>)
// -----------------------------------------------------------------
//
// The view definition lives in OnModelCreating using a strongly-typed
// .Select(...) projection and .Where(...) predicate. EF.CH translates
// the LINQ to a CREATE VIEW DDL statement when EnsureViewAsync runs.
// -----------------------------------------------------------------
static async Task DemoAsViewWithLinq(string connectionString)
{
    Console.WriteLine("=== 1. AsView<TView, TSource> with LINQ ===\n");

    await using var context = new ViewDemoContext(connectionString);

    await context.Database.EnsureViewAsync<ActiveUserView>();
    Console.WriteLine("Generated DDL from the LINQ view definition:");
    Console.WriteLine(context.Database.GetViewSql<ActiveUserView>());
    Console.WriteLine();

    var actives = await context.Set<ActiveUserView>()
        .OrderBy(u => u.UserId)
        .ToListAsync();

    Console.WriteLine($"Active users (queried via Set<ActiveUserView>()): {actives.Count}");
    foreach (var u in actives)
        Console.WriteLine($"  {u.UserId}: {u.Name}");
}

// -----------------------------------------------------------------
// Demo 2: LINQ composition on a LINQ-defined view
// -----------------------------------------------------------------
//
// RecentUserView is an AsView with multi-clause LINQ WHERE. Once it
// exists in ClickHouse, you can stack further LINQ on top — Where,
// OrderBy, Take, Select — exactly like a regular DbSet.
// -----------------------------------------------------------------
static async Task DemoLinqComposedOnView(string connectionString)
{
    Console.WriteLine("\n=== 2. LINQ composition on a LINQ-defined view ===\n");

    await using var context = new ViewDemoContext(connectionString);

    await context.Database.EnsureViewAsync<RecentUserView>();

    Console.WriteLine("View DDL (multi-clause WHERE built from LINQ):");
    Console.WriteLine(context.Database.GetViewSql<RecentUserView>());
    Console.WriteLine();

    // Compose further LINQ on top of the view.
    var topRecent = await context.Set<RecentUserView>()
        .Where(u => u.Name.StartsWith("A") || u.Name.StartsWith("C"))
        .OrderByDescending(u => u.LastSeen)
        .Take(5)
        .Select(u => new { u.Name, u.LastSeen })
        .ToListAsync();

    Console.WriteLine($"Recent users matching A*/C* (LINQ-composed): {topRecent.Count}");
    foreach (var u in topRecent)
        Console.WriteLine($"  {u.Name} — last seen {u.LastSeen:yyyy-MM-dd HH:mm}");
}

// -----------------------------------------------------------------
// Demo 3: EnsureViewsAsync — batch deploy every LINQ-defined view
// -----------------------------------------------------------------
static async Task DemoEnsureViewsAsync(string connectionString)
{
    Console.WriteLine("\n=== 3. EnsureViewsAsync ===\n");

    await using var context = new ViewDemoContext(connectionString);

    var created = await context.Database.EnsureViewsAsync();
    Console.WriteLine(
        $"EnsureViewsAsync re-deployed {created} LINQ-defined view(s) " +
        "(OR REPLACE makes it idempotent on every startup).");
}

// -----------------------------------------------------------------
// Demo 4: Re-deploy a LINQ-defined view body via OR REPLACE
// -----------------------------------------------------------------
//
// To "edit" a view, change its LINQ definition in OnModelCreating and
// re-run EnsureViewAsync. Because the AsView config opted into
// .OrReplace(), no DROP is needed.
// -----------------------------------------------------------------
static async Task DemoOrReplaceFromLinq(string connectionString)
{
    Console.WriteLine("\n=== 4. OR REPLACE re-deployment from LINQ ===\n");

    await using var context = new ViewDemoContext(connectionString);

    await context.Database.EnsureViewAsync<RecentUserView>();
    Console.WriteLine("Re-deployed RecentUserView via the LINQ definition (CREATE OR REPLACE).");

    var count = await context.Set<RecentUserView>().CountAsync();
    Console.WriteLine($"RecentUserView row count after re-deployment: {count}");
}

// -----------------------------------------------------------------
// Entities
// -----------------------------------------------------------------

public class User
{
    public ulong UserId { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool IsActive { get; set; }
    public DateTime LastSeen { get; set; }
}

public class ActiveUserView
{
    public ulong UserId { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class RecentUserView
{
    public ulong UserId { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime LastSeen { get; set; }
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class ViewDemoContext(string connectionString) : DbContext
{
    public DbSet<User> Users => Set<User>();
    public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();
    public DbSet<RecentUserView> RecentUsers => Set<RecentUserView>();

    private static readonly DateTime Cutoff = DateTime.UtcNow.AddDays(-7);

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.UserId);
            entity.UseMergeTree(e => e.UserId);
        });

        // View body defined entirely in LINQ:
        //   SELECT UserId, Name FROM users WHERE IsActive
        modelBuilder.Entity<ActiveUserView>(entity =>
        {
            entity.AsView<ActiveUserView, User>(cfg => cfg
                .HasName("active_users")
                .FromTable()
                .Select(u => new ActiveUserView
                {
                    UserId = u.UserId,
                    Name = u.Name
                })
                .Where(u => u.IsActive)
                .OrReplace());
        });

        // Multi-clause WHERE built with LINQ — captured `Cutoff` is baked into
        // the DDL as a literal at view-creation time:
        //   SELECT UserId, Name, LastSeen FROM users
        //   WHERE IsActive AND LastSeen >= '<cutoff>'
        modelBuilder.Entity<RecentUserView>(entity =>
        {
            entity.AsView<RecentUserView, User>(cfg => cfg
                .HasName("recent_users")
                .FromTable()
                .Select(u => new RecentUserView
                {
                    UserId = u.UserId,
                    Name = u.Name,
                    LastSeen = u.LastSeen
                })
                .Where(u => u.IsActive)
                .Where(u => u.LastSeen >= Cutoff)
                .OrReplace());
        });
    }
}
