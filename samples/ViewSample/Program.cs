// -----------------------------------------------------------------
// ViewSample - Plain ClickHouse Views with EF.CH
// -----------------------------------------------------------------
// Demonstrates:
//   1. HasView + FromView (annotation-only mapping, raw runtime DDL)
//   2. AsView fluent API (LINQ-driven SELECT, EnsureViewAsync)
//   3. AsViewRaw + EnsureViewsAsync (raw SQL body, batch deployment)
//   4. CreateViewAsync / DropViewAsync runtime helpers (OR REPLACE)
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

    await DemoHasViewAndFromView(connectionString);
    await DemoAsViewFluent(connectionString);
    await DemoEnsureViewsAsync(connectionString);
    await DemoOrReplace(connectionString);
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
// Demo 1: HasView + FromView (raw runtime DDL)
// -----------------------------------------------------------------
static async Task DemoHasViewAndFromView(string connectionString)
{
    Console.WriteLine("=== 1. HasView + FromView ===\n");

    await using var context = new ViewDemoContext(connectionString);

    await context.Database.CreateViewAsync(
        "all_users_view",
        """
        SELECT user_id AS "UserId", name AS "Name", is_active AS "IsActive", last_seen AS "LastSeen"
        FROM users
        """,
        ifNotExists: true);

    var rows = await context.FromView<UserView>("all_users_view")
        .OrderBy(u => u.UserId)
        .ToListAsync();

    Console.WriteLine($"FromView returned {rows.Count} users:");
    foreach (var u in rows)
        Console.WriteLine($"  {u.UserId}: {u.Name} (active={u.IsActive})");
}

// -----------------------------------------------------------------
// Demo 2: AsView fluent API + EnsureViewAsync
// -----------------------------------------------------------------
static async Task DemoAsViewFluent(string connectionString)
{
    Console.WriteLine("\n=== 2. AsView fluent + EnsureViewAsync ===\n");

    await using var context = new ViewDemoContext(connectionString);

    // ActiveUserView is configured with AsView<ActiveUserView, User>(...) below.
    await context.Database.EnsureViewAsync<ActiveUserView>();
    Console.WriteLine("Created view 'active_users' from fluent AsView<TView, TSource> config.");
    Console.WriteLine("Generated DDL:");
    Console.WriteLine(context.Database.GetViewSql<ActiveUserView>());
    Console.WriteLine();

    // Query through stock EF Core (entity is mapped to a view via ToView under the hood).
    var actives = await context.Set<ActiveUserView>()
        .OrderBy(u => u.UserId)
        .ToListAsync();

    Console.WriteLine($"context.Set<ActiveUserView>() returned {actives.Count} active users:");
    foreach (var u in actives)
        Console.WriteLine($"  {u.UserId}: {u.Name}");
}

// -----------------------------------------------------------------
// Demo 3: EnsureViewsAsync — batch creation of all configured views
// -----------------------------------------------------------------
static async Task DemoEnsureViewsAsync(string connectionString)
{
    Console.WriteLine("\n=== 3. EnsureViewsAsync ===\n");

    await using var context = new ViewDemoContext(connectionString);

    var created = await context.Database.EnsureViewsAsync();
    Console.WriteLine($"EnsureViewsAsync re-deployed {created} view(s) (OR REPLACE makes it idempotent).");
}

// -----------------------------------------------------------------
// Demo 4: CreateViewAsync with OR REPLACE
// -----------------------------------------------------------------
static async Task DemoOrReplace(string connectionString)
{
    Console.WriteLine("\n=== 4. CreateViewAsync with OR REPLACE ===\n");

    await using var context = new ViewDemoContext(connectionString);

    await context.Database.CreateViewAsync(
        "user_count_view",
        "SELECT count() AS \"Count\" FROM users",
        orReplace: true);
    Console.WriteLine("Created user_count_view.");

    // Replace the body — OR REPLACE means we don't need to drop first.
    await context.Database.CreateViewAsync(
        "user_count_view",
        "SELECT countIf(is_active = 1) AS \"Count\" FROM users",
        orReplace: true);
    Console.WriteLine("Replaced user_count_view body (active-only count).");

    var counts = await context.FromView<UserCountView>("user_count_view").ToListAsync();
    Console.WriteLine($"Active user count: {counts[0].Count}");

    await context.Database.DropViewAsync("user_count_view");
    await context.Database.DropViewAsync("all_users_view");
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

public class UserView
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

public class UserCountView
{
    public ulong Count { get; set; }
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class ViewDemoContext(string connectionString) : DbContext
{
    public DbSet<User> Users => Set<User>();
    public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();

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
            entity.Property(e => e.UserId).HasColumnName("user_id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.IsActive).HasColumnName("is_active");
            entity.Property(e => e.LastSeen).HasColumnName("last_seen");
        });

        // Plain HasView mapping — used by FromView<UserView>("all_users_view").
        modelBuilder.Entity<UserView>(e => e.HasView("all_users_view"));

        // Fluent AsView with LINQ-driven SELECT and OR REPLACE.
        modelBuilder.Entity<ActiveUserView>(entity =>
        {
            entity.AsView<ActiveUserView, User>(cfg => cfg
                .HasName("active_users")
                .FromTable()
                .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
                .Where(u => u.IsActive)
                .OrReplace());
        });

        // Keyless mapping for the count view.
        modelBuilder.Entity<UserCountView>(e => e.HasView("user_count_view"));
    }
}
