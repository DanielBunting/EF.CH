// EnginesSample - Demonstrates 5 ClickHouse engine types via EF.CH
//
// 1. MergeTree           - Basic columnar storage with ORDER BY and PARTITION BY
// 2. ReplacingMergeTree  - Row deduplication by version column, queried with .Final()
// 3. SummingMergeTree    - Automatic summation of numeric columns during merges
// 4. AggregatingMergeTree - Intermediate aggregate state storage (-State/-Merge pattern)
// 5. CollapsingMergeTree - State tracking via +1/-1 sign column

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("=== EF.CH Engines Sample ===");
    Console.WriteLine();

    await DemoMergeTree(connectionString);
    await DemoReplacingMergeTree(connectionString);
    await DemoSummingMergeTree(connectionString);
    await DemoAggregatingMergeTree(connectionString);
    await DemoCollapsingMergeTree(connectionString);

    Console.WriteLine("=== All engine demos complete ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ---------------------------------------------------------------------------
// 1. MergeTree
// ---------------------------------------------------------------------------
static async Task DemoMergeTree(string connectionString)
{
    Console.WriteLine("--- 1. MergeTree Engine ---");
    Console.WriteLine("Basic columnar storage with ORDER BY and PARTITION BY.");
    Console.WriteLine();

    await using var context = new MergeTreeContext(connectionString);

    // EnsureCreatedAsync generates the CREATE TABLE DDL from the model configuration:
    //   CREATE TABLE PageViews (...) ENGINE = MergeTree()
    //   PARTITION BY toYYYYMM(ViewedAt) ORDER BY (ViewedAt, Id)
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert data
    var now = DateTime.UtcNow;
    var views = new List<PageView>
    {
        new() { Id = 1, Path = "/home", UserId = 100, ViewedAt = now.AddMinutes(-5), DurationMs = 1200 },
        new() { Id = 2, Path = "/products", UserId = 101, ViewedAt = now.AddMinutes(-4), DurationMs = 3400 },
        new() { Id = 3, Path = "/home", UserId = 102, ViewedAt = now.AddMinutes(-3), DurationMs = 800 },
        new() { Id = 4, Path = "/checkout", UserId = 100, ViewedAt = now.AddMinutes(-2), DurationMs = 5600 },
        new() { Id = 5, Path = "/products", UserId = 103, ViewedAt = now.AddMinutes(-1), DurationMs = 2100 },
    };

    await context.BulkInsertAsync(views);
    Console.WriteLine($"Inserted {views.Count} page views.");

    // Query: count views per path
    var pathCounts = await context.PageViews
        .GroupBy(v => v.Path)
        .Select(g => new { Path = g.Key, Count = g.Count() })
        .OrderByDescending(x => x.Count)
        .ToListAsync();

    Console.WriteLine("Page view counts:");
    foreach (var item in pathCounts)
    {
        Console.WriteLine($"  {item.Path}: {item.Count}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 2. ReplacingMergeTree
// ---------------------------------------------------------------------------
static async Task DemoReplacingMergeTree(string connectionString)
{
    Console.WriteLine("--- 2. ReplacingMergeTree Engine ---");
    Console.WriteLine("Deduplicates rows by ORDER BY key, keeping the highest version.");
    Console.WriteLine();

    await using var context = new ReplacingMergeTreeContext(connectionString);

    // EnsureCreatedAsync generates:
    //   CREATE TABLE UserProfiles (...) ENGINE = ReplacingMergeTree(Version)
    //   ORDER BY (UserId)
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert initial profiles
    await context.BulkInsertAsync(new List<UserProfile>
    {
        new() { UserId = 1, Name = "Alice", Email = "alice@example.com", Version = 1 },
        new() { UserId = 2, Name = "Bob", Email = "bob@example.com", Version = 1 },
        new() { UserId = 3, Name = "Charlie", Email = "charlie@example.com", Version = 1 },
    });

    // Insert updates (same UserId, higher version)
    await context.BulkInsertAsync(new List<UserProfile>
    {
        new() { UserId = 1, Name = "Alice Updated", Email = "alice.new@example.com", Version = 2 },
        new() { UserId = 2, Name = "Bob Updated", Email = "bob.new@example.com", Version = 3 },
    });
    Console.WriteLine("Inserted 3 profiles, then 2 updates with higher versions.");

    // Without FINAL: may see duplicates (both old and new versions)
    var allRows = await context.UserProfiles
        .Where(u => u.UserId == 1)
        .OrderBy(u => u.Version)
        .ToListAsync();
    Console.WriteLine($"Without FINAL, UserId=1 rows: {allRows.Count}");
    foreach (var row in allRows)
    {
        Console.WriteLine($"  Name={row.Name}, Version={row.Version}");
    }

    // With FINAL: on-the-fly deduplication shows only the latest version
    var deduped = await context.UserProfiles
        .Final()
        .OrderBy(u => u.UserId)
        .ToListAsync();
    Console.WriteLine($"With FINAL, total rows: {deduped.Count}");
    foreach (var row in deduped)
    {
        Console.WriteLine($"  UserId={row.UserId}, Name={row.Name}, Version={row.Version}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 3. SummingMergeTree
// ---------------------------------------------------------------------------
static async Task DemoSummingMergeTree(string connectionString)
{
    Console.WriteLine("--- 3. SummingMergeTree Engine ---");
    Console.WriteLine("Automatically sums numeric columns for rows with the same ORDER BY key.");
    Console.WriteLine();

    await using var context = new SummingMergeTreeContext(connectionString);

    // EnsureCreatedAsync generates:
    //   CREATE TABLE PageCounters (...) ENGINE = SummingMergeTree()
    //   ORDER BY (Path, Hour)
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert multiple increments for the same key
    var hour = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc);
    var counters = new List<PageCounter>
    {
        new() { Path = "/home", Hour = hour, Views = 10, UniqueUsers = 5 },
        new() { Path = "/home", Hour = hour, Views = 15, UniqueUsers = 8 },
        new() { Path = "/home", Hour = hour, Views = 7, UniqueUsers = 3 },
        new() { Path = "/products", Hour = hour, Views = 20, UniqueUsers = 12 },
        new() { Path = "/products", Hour = hour, Views = 5, UniqueUsers = 2 },
    };

    await context.BulkInsertAsync(counters);
    Console.WriteLine($"Inserted {counters.Count} counter increments.");

    // Force merge to trigger summation
    await context.Database.OptimizeTableFinalAsync<PageCounter>();
    // Allow time for the merge to complete
    await Task.Delay(500);

    var results = await context.PageCounters
        .OrderBy(c => c.Path)
        .ToListAsync();

    Console.WriteLine("After OPTIMIZE TABLE FINAL (values are auto-summed):");
    foreach (var row in results)
    {
        Console.WriteLine($"  Path={row.Path}, Views={row.Views}, UniqueUsers={row.UniqueUsers}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 4. AggregatingMergeTree
// ---------------------------------------------------------------------------
static async Task DemoAggregatingMergeTree(string connectionString)
{
    Console.WriteLine("--- 4. AggregatingMergeTree Engine ---");
    Console.WriteLine("Stores intermediate aggregate states using -State/-Merge functions.");
    Console.WriteLine("Typically used as the target of a materialized view.");
    Console.WriteLine();

    await using var context = new AggregatingMergeTreeContext(connectionString);

    // EnsureCreatedAsync generates the source table, target table, and materialized view.
    // The AsMaterializedViewRaw in the model wires them together:
    //   CREATE TABLE RawEventsAgg (...) ENGINE = MergeTree() ORDER BY (EventType, Timestamp)
    //   CREATE TABLE EventStats (...) ENGINE = AggregatingMergeTree() ORDER BY (EventType)
    //   CREATE MATERIALIZED VIEW ... TO EventStats AS SELECT ... FROM RawEventsAgg ...
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert raw events -- the MV will aggregate them automatically
    var events = new List<RawEvent>
    {
        new() { EventType = "purchase", Amount = 99.99, Timestamp = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc) },
        new() { EventType = "purchase", Amount = 149.50, Timestamp = new DateTime(2025, 1, 15, 10, 5, 0, DateTimeKind.Utc) },
        new() { EventType = "purchase", Amount = 29.99, Timestamp = new DateTime(2025, 1, 15, 10, 10, 0, DateTimeKind.Utc) },
        new() { EventType = "refund", Amount = 49.99, Timestamp = new DateTime(2025, 1, 15, 10, 15, 0, DateTimeKind.Utc) },
        new() { EventType = "refund", Amount = 19.99, Timestamp = new DateTime(2025, 1, 15, 10, 20, 0, DateTimeKind.Utc) },
    };

    await context.BulkInsertAsync(events);
    Console.WriteLine("Inserted 5 raw events into source table.");
    Console.WriteLine("Materialized view automatically stores aggregate states.");

    // Query the aggregated view using -Merge functions via raw SQL
    // -Merge functions finalize the intermediate aggregate states
    var stats = await context.Database.SqlQueryRaw<AggResult>("""
        SELECT
            EventType,
            countMerge(EventCount) AS Count,
            sumMerge(TotalAmount) AS Total
        FROM EventStats
        GROUP BY EventType
        ORDER BY EventType
    """).ToListAsync();

    Console.WriteLine("Querying aggregated view with -Merge functions:");
    foreach (var stat in stats)
    {
        Console.WriteLine($"  EventType={stat.EventType}, Count={stat.Count}, Total={stat.Total:F2}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 5. CollapsingMergeTree
// ---------------------------------------------------------------------------
static async Task DemoCollapsingMergeTree(string connectionString)
{
    Console.WriteLine("--- 5. CollapsingMergeTree Engine ---");
    Console.WriteLine("Tracks state changes with +1 (insert) / -1 (cancel) sign column.");
    Console.WriteLine();

    await using var context = new CollapsingMergeTreeContext(connectionString);

    // EnsureCreatedAsync generates:
    //   CREATE TABLE UserBalances (...) ENGINE = CollapsingMergeTree(Sign)
    //   ORDER BY (UserId)
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Insert initial state (Sign = +1)
    await context.BulkInsertAsync(new List<UserBalance>
    {
        new() { UserId = 1, Balance = 100.00, UpdatedAt = DateTime.UtcNow.AddMinutes(-10), Sign = 1 },
        new() { UserId = 2, Balance = 250.00, UpdatedAt = DateTime.UtcNow.AddMinutes(-10), Sign = 1 },
    });
    Console.WriteLine("Inserted initial balances: User1=100, User2=250");

    // Update User1's balance: cancel old row (-1) and insert new row (+1)
    await context.BulkInsertAsync(new List<UserBalance>
    {
        // Cancel old state
        new() { UserId = 1, Balance = 100.00, UpdatedAt = DateTime.UtcNow.AddMinutes(-10), Sign = -1 },
        // Insert new state
        new() { UserId = 1, Balance = 175.50, UpdatedAt = DateTime.UtcNow, Sign = 1 },
    });
    Console.WriteLine("Updated User1 balance: cancel 100, insert 175.50");

    // Before merge: all rows are visible
    var beforeMerge = await context.UserBalances
        .Where(u => u.UserId == 1)
        .OrderBy(u => u.Sign)
        .ToListAsync();
    Console.WriteLine($"Before OPTIMIZE, User1 rows: {beforeMerge.Count}");
    foreach (var row in beforeMerge)
    {
        Console.WriteLine($"  Balance={row.Balance}, Sign={row.Sign}");
    }

    // Force merge to collapse +1/-1 pairs
    await context.Database.OptimizeTableFinalAsync<UserBalance>();
    await Task.Delay(500);

    var afterMerge = await context.UserBalances
        .OrderBy(u => u.UserId)
        .ToListAsync();
    Console.WriteLine($"After OPTIMIZE TABLE FINAL, total rows: {afterMerge.Count}");
    foreach (var row in afterMerge)
    {
        Console.WriteLine($"  UserId={row.UserId}, Balance={row.Balance}, Sign={row.Sign}");
    }

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine();
}

// ===========================================================================
// Entity classes and DbContext classes
// ===========================================================================

// --- MergeTree ---

public class PageView
{
    public ulong Id { get; set; }
    public string Path { get; set; } = "";
    public ulong UserId { get; set; }
    public DateTime ViewedAt { get; set; }
    public uint DurationMs { get; set; }
}

public class MergeTreeContext(string connectionString) : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.ViewedAt, x.Id })
                .HasPartitionByMonth(x => x.ViewedAt);
        });
    }
}

// --- ReplacingMergeTree ---

public class UserProfile
{
    public ulong UserId { get; set; }
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public ulong Version { get; set; }
}

public class ReplacingMergeTreeContext(string connectionString) : DbContext
{
    public DbSet<UserProfile> UserProfiles => Set<UserProfile>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserProfile>(entity =>
        {
            entity.HasNoKey();
            entity.UseReplacingMergeTree(x => x.Version, x => new { x.UserId });
        });
    }
}

// --- SummingMergeTree ---

public class PageCounter
{
    public string Path { get; set; } = "";
    public DateTime Hour { get; set; }
    public ulong Views { get; set; }
    public ulong UniqueUsers { get; set; }
}

public class SummingMergeTreeContext(string connectionString) : DbContext
{
    public DbSet<PageCounter> PageCounters => Set<PageCounter>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageCounter>(entity =>
        {
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Path, x.Hour });
        });
    }
}

// --- AggregatingMergeTree ---

public class RawEvent
{
    public string EventType { get; set; } = "";
    public double Amount { get; set; }
    public DateTime Timestamp { get; set; }
}

public class EventStat
{
    public string EventType { get; set; } = "";
    public byte[] EventCount { get; set; } = [];
    public byte[] TotalAmount { get; set; } = [];
}

public class AggResult
{
    public string EventType { get; set; } = "";
    public ulong Count { get; set; }
    public double Total { get; set; }
}

public class AggregatingMergeTreeContext(string connectionString) : DbContext
{
    public DbSet<RawEvent> RawEvents => Set<RawEvent>();
    public DbSet<EventStat> EventStats => Set<EventStat>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table: standard MergeTree
        modelBuilder.Entity<RawEvent>(entity =>
        {
            entity.ToTable("RawEventsAgg");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.EventType, x.Timestamp });
        });

        // Target table: AggregatingMergeTree with aggregate function columns
        modelBuilder.Entity<EventStat>(entity =>
        {
            entity.HasNoKey();
            entity.UseAggregatingMergeTree(x => new { x.EventType });
            entity.Property(x => x.EventCount).HasAggregateFunction("count", typeof(ulong));
            entity.Property(x => x.TotalAmount).HasAggregateFunction("sum", typeof(double));

            // Define the materialized view that populates this table from RawEventsAgg
            entity.AsMaterializedViewRaw(
                sourceTable: "RawEventsAgg",
                selectSql: @"SELECT
                    EventType,
                    countState() AS EventCount,
                    sumState(Amount) AS TotalAmount
                FROM RawEventsAgg
                GROUP BY EventType");
        });

        modelBuilder.Entity<AggResult>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable((string?)null);
        });
    }
}

// --- CollapsingMergeTree ---

public class UserBalance
{
    public ulong UserId { get; set; }
    public double Balance { get; set; }
    public DateTime UpdatedAt { get; set; }
    public sbyte Sign { get; set; }
}

public class CollapsingMergeTreeContext(string connectionString) : DbContext
{
    public DbSet<UserBalance> UserBalances => Set<UserBalance>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UserBalance>(entity =>
        {
            entity.HasNoKey();
            entity.UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId });
        });
    }
}
