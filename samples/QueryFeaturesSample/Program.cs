// QueryFeaturesSample - Demonstrates ClickHouse-specific query features via EF.CH
//
// 1. FINAL          - On-the-fly deduplication for ReplacingMergeTree
// 2. SAMPLE         - Probabilistic sampling for approximate results
// 3. WithSettings   - ClickHouse query-level settings and WithRawFilter
// 4. LimitBy        - Top-N per group without window functions
// 5. CTE            - Common Table Expressions via .AsCte()
// 6. Set Operations - UnionAll and UnionDistinct
// 7. GROUP BY modifiers - WithRollup, WithCube, WithTotals
// 8. Text Search    - Token-based and fuzzy text search

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

    Console.WriteLine("=== EF.CH Query Features Sample ===");
    Console.WriteLine();

    await using var context = new EventsContext(connectionString);

    // Create the table from the model configuration (ReplacingMergeTree with SAMPLE BY)
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    await SetupData(context);

    await DemoFinal(context);
    await DemoSample(context);
    await DemoWithSettings(context);
    await DemoLimitBy(context);
    await DemoCte(context);
    await DemoSetOperations(context);
    await DemoGroupByModifiers(context);
    await DemoTextSearch(context);

    await context.Database.EnsureDeletedAsync();

    Console.WriteLine("=== All query feature demos complete ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ---------------------------------------------------------------------------
// Setup: Insert diverse sample data
// ---------------------------------------------------------------------------
static async Task SetupData(EventsContext context)
{
    Console.WriteLine("Setting up sample data...");

    var now = DateTime.UtcNow;
    var categories = new[] { "web", "mobile", "api", "backend" };
    var eventTypes = new[] { "click", "view", "purchase", "error", "login" };
    var messages = new[]
    {
        "User clicked the submit button on checkout page",
        "Error: connection timeout on database server",
        "Purchase completed for order #12345",
        "Login successful from new device token detected",
        "Page view on /products with search filter active",
        "API rate limit exceeded for endpoint /api/users",
        "Mobile app crash report for version 3.2.1",
        "Backend service health check passed successfully",
    };

    var events = new List<Event>();
    var random = new Random(42);
    for (ulong i = 1; i <= 200; i++)
    {
        events.Add(new Event
        {
            Id = i,
            EventType = eventTypes[random.Next(eventTypes.Length)],
            Category = categories[random.Next(categories.Length)],
            UserId = (ulong)(random.Next(1, 20)),
            Score = Math.Round(random.NextDouble() * 100, 2),
            Message = messages[random.Next(messages.Length)],
            Active = (byte)(random.Next(4) > 0 ? 1 : 0), // 75% active
            CreatedAt = now.AddMinutes(-random.Next(1, 1440)),
            Version = 1,
        });
    }

    await context.BulkInsertAsync(events);

    // Insert some duplicate rows with higher version for FINAL demo
    var updates = events.Take(10).Select(e => new Event
    {
        Id = e.Id,
        EventType = e.EventType,
        Category = e.Category,
        UserId = e.UserId,
        Score = e.Score + 10, // Modified score
        Message = e.Message + " (updated)",
        Active = e.Active,
        CreatedAt = e.CreatedAt,
        Version = 2,
    }).ToList();

    await context.BulkInsertAsync(updates);
    Console.WriteLine($"Inserted 200 events + 10 updates (Version=2).");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 1. FINAL
// ---------------------------------------------------------------------------
static async Task DemoFinal(EventsContext context)
{
    Console.WriteLine("--- 1. FINAL ---");
    Console.WriteLine("Forces on-the-fly deduplication for ReplacingMergeTree.");
    Console.WriteLine();

    // Without FINAL: may return duplicates (both Version=1 and Version=2)
    var withoutFinal = await context.Events
        .Where(e => e.Id <= 10)
        .CountAsync();

    // With FINAL: only the latest version per ORDER BY key
    // Generates: SELECT ... FROM Events FINAL WHERE ...
    var withFinal = await context.Events
        .Final()
        .Where(e => e.Id <= 10)
        .CountAsync();

    Console.WriteLine($"Rows for Id <= 10 without FINAL: {withoutFinal}");
    Console.WriteLine($"Rows for Id <= 10 with FINAL:    {withFinal}");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 2. SAMPLE
// ---------------------------------------------------------------------------
static async Task DemoSample(EventsContext context)
{
    Console.WriteLine("--- 2. SAMPLE ---");
    Console.WriteLine("Probabilistic sampling for approximate results on large datasets.");
    Console.WriteLine();

    var totalCount = await context.Events.Final().CountAsync();

    // Sample 50% of the data
    // Generates: SELECT ... FROM Events FINAL SAMPLE 0.5
    var sampledCount = await context.Events
        .Final()
        .Sample(0.5)
        .CountAsync();

    Console.WriteLine($"Total rows (with FINAL): {totalCount}");
    Console.WriteLine($"Sampled rows (50%):      {sampledCount}");
    Console.WriteLine($"Actual sample ratio:     {(double)sampledCount / totalCount:P1}");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 3. WithSettings / WithRawFilter
// ---------------------------------------------------------------------------
static async Task DemoWithSettings(EventsContext context)
{
    Console.WriteLine("--- 3. WithSettings / WithRawFilter ---");
    Console.WriteLine("Pass ClickHouse settings to queries, or inject raw SQL conditions.");
    Console.WriteLine();

    // WithSetting: sets ClickHouse query-level settings
    // Generates: SELECT ... FROM Events FINAL SETTINGS max_threads = 2
    var withSettings = await context.Events
        .Final()
        .Where(e => e.Category == "web")
        .WithSetting("max_threads", "2")
        .CountAsync();
    Console.WriteLine($"Web events (with max_threads=2 setting): {withSettings}");

    // WithSettings: multiple settings via dictionary
    var withMultipleSettings = await context.Events
        .Final()
        .Where(e => e.Score > 50)
        .WithSettings(new Dictionary<string, object>
        {
            ["max_threads"] = "4",
            ["max_block_size"] = "1000"
        })
        .CountAsync();
    Console.WriteLine($"High-score events (with multiple settings): {withMultipleSettings}");

    // WithRawFilter: inject raw SQL as an additional WHERE condition
    // Generates: SELECT ... WHERE ... AND "Score" > 75 AND "Category" = 'web'
    var rawFiltered = await context.Events
        .Final()
        .Where(e => e.EventType == "click")
        .WithRawFilter("\"Score\" > 75 AND \"Category\" = 'web'")
        .ToListAsync();
    Console.WriteLine($"Click events with raw filter (Score>75, web): {rawFiltered.Count}");

    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 4. LimitBy
// ---------------------------------------------------------------------------
static async Task DemoLimitBy(EventsContext context)
{
    Console.WriteLine("--- 4. LimitBy ---");
    Console.WriteLine("Returns top N rows per group without window functions.");
    Console.WriteLine();

    // Get top 3 events per category, ordered by score
    // Generates: SELECT ... FROM Events FINAL ORDER BY Score DESC LIMIT 3 BY Category
    // Note: LimitBy must be the terminal query operation — project client-side after ToList
    var topPerCategory = await context.Events
        .Final()
        .OrderByDescending(e => e.Score)
        .LimitBy(3, e => e.Category)
        .ToListAsync();

    Console.WriteLine("Top 3 events per category by score:");
    var currentCategory = "";
    foreach (var item in topPerCategory.OrderBy(x => x.Category))
    {
        if (item.Category != currentCategory)
        {
            currentCategory = item.Category;
            Console.WriteLine($"  [{currentCategory}]");
        }
        Console.WriteLine($"    {item.EventType}: {item.Score:F2}");
    }
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 5. CTE
// ---------------------------------------------------------------------------
static async Task DemoCte(EventsContext context)
{
    Console.WriteLine("--- 5. CTE (Common Table Expression) ---");
    Console.WriteLine("Wraps a subquery as a named CTE for logical organization.");
    Console.WriteLine();

    // Generates: WITH "high_score_events" AS (SELECT ... WHERE ...) SELECT ... FROM "high_score_events"
    var recentHighScorers = await context.Events
        .Final()
        .Where(e => e.Score > 75)
        .AsCte("high_score_events")
        .OrderByDescending(e => e.Score)
        .Take(5)
        .ToListAsync();

    Console.WriteLine("Top 5 high-score events (via CTE):");
    foreach (var item in recentHighScorers)
    {
        Console.WriteLine($"  Id={item.Id}, Type={item.EventType}, Score={item.Score:F2}, Category={item.Category}");
    }
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 6. Set Operations
// ---------------------------------------------------------------------------
static async Task DemoSetOperations(EventsContext context)
{
    Console.WriteLine("--- 6. Set Operations ---");
    Console.WriteLine("Combines queries with UNION ALL and UNION DISTINCT.");
    Console.WriteLine();

    var webEvents = context.Events.Final()
        .Where(e => e.Category == "web")
        .Select(e => new { e.Id, e.Category, e.EventType });

    var mobileEvents = context.Events.Final()
        .Where(e => e.Category == "mobile")
        .Select(e => new { e.Id, e.Category, e.EventType });

    // UnionAll: keeps all rows including duplicates
    // Generates: SELECT ... UNION ALL SELECT ...
    var unionAllCount = await webEvents.UnionAll(mobileEvents).CountAsync();
    Console.WriteLine($"Web + Mobile events (UNION ALL): {unionAllCount}");

    // UnionDistinct: removes duplicate rows
    // Generates: SELECT ... UNION DISTINCT SELECT ...
    var unionDistinctCount = await webEvents.UnionDistinct(mobileEvents).CountAsync();
    Console.WriteLine($"Web + Mobile events (UNION DISTINCT): {unionDistinctCount}");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 7. GROUP BY Modifiers
// ---------------------------------------------------------------------------
static async Task DemoGroupByModifiers(EventsContext context)
{
    Console.WriteLine("--- 7. GROUP BY Modifiers ---");
    Console.WriteLine("WithRollup, WithCube, and WithTotals add subtotals to GROUP BY results.");
    Console.WriteLine();

    // WithTotals: adds a grand total row
    // Generates: SELECT ... GROUP BY Category WITH TOTALS
    var withTotals = await context.Events
        .Final()
        .GroupBy(e => e.Category)
        .Select(g => new { Category = g.Key, Count = g.Count() })
        .WithTotals()
        .ToListAsync();

    Console.WriteLine("Counts by category WITH TOTALS:");
    foreach (var item in withTotals)
    {
        var label = string.IsNullOrEmpty(item.Category) ? "(TOTAL)" : item.Category;
        Console.WriteLine($"  {label}: {item.Count}");
    }
    Console.WriteLine();

    // WithRollup: hierarchical subtotals
    // Generates: SELECT ... GROUP BY Category, EventType WITH ROLLUP
    var withRollup = await context.Events
        .Final()
        .GroupBy(e => new { e.Category, e.EventType })
        .Select(g => new
        {
            Category = g.Key.Category,
            EventType = g.Key.EventType,
            Count = g.Count()
        })
        .WithRollup()
        .ToListAsync();

    Console.WriteLine("Counts by Category, EventType WITH ROLLUP (showing subtotals):");
    foreach (var item in withRollup.Take(15))
    {
        var cat = string.IsNullOrEmpty(item.Category) ? "(all)" : item.Category;
        var evt = string.IsNullOrEmpty(item.EventType) ? "(subtotal)" : item.EventType;
        Console.WriteLine($"  {cat} / {evt}: {item.Count}");
    }
    if (withRollup.Count > 15)
    {
        Console.WriteLine($"  ... and {withRollup.Count - 15} more rows");
    }
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 8. Text Search
// ---------------------------------------------------------------------------
static async Task DemoTextSearch(EventsContext context)
{
    Console.WriteLine("--- 8. Text Search ---");
    Console.WriteLine("Token-based and fuzzy text search using ClickHouse functions.");
    Console.WriteLine();

    // HasToken: exact whole-word matching
    // Generates: WHERE hasToken(Message, 'error')
    var errorEvents = await context.Events
        .Final()
        .Where(e => Microsoft.EntityFrameworkCore.EF.Functions.HasToken(e.Message, "Error"))
        .CountAsync();
    Console.WriteLine($"Events with token 'Error': {errorEvents}");

    // HasTokenCaseInsensitive: case-insensitive token search
    // Generates: WHERE hasTokenCaseInsensitive(Message, 'error')
    var errorEventsCi = await context.Events
        .Final()
        .Where(e => Microsoft.EntityFrameworkCore.EF.Functions.HasTokenCaseInsensitive(e.Message, "error"))
        .CountAsync();
    Console.WriteLine($"Events with token 'error' (case-insensitive): {errorEventsCi}");

    // MultiSearchAny: matches any of several terms
    // Generates: WHERE multiSearchAnyCaseInsensitive(Message, ['error', 'crash', 'timeout'])
    var problemEvents = await context.Events
        .Final()
        .Where(e => Microsoft.EntityFrameworkCore.EF.Functions.MultiSearchAnyCaseInsensitive(
            e.Message, new[] { "error", "crash", "timeout" }))
        .CountAsync();
    Console.WriteLine($"Events containing 'error', 'crash', or 'timeout': {problemEvents}");

    // HasToken combined with other filters
    var checkoutEvents = await context.Events
        .Final()
        .Where(e => Microsoft.EntityFrameworkCore.EF.Functions.HasToken(e.Message, "checkout")
                    && e.Category == "web")
        .ToListAsync();
    Console.WriteLine($"Web events with token 'checkout': {checkoutEvents.Count}");
    foreach (var item in checkoutEvents.Take(3))
    {
        Console.WriteLine($"  Id={item.Id}: {item.Message[..Math.Min(60, item.Message.Length)]}...");
    }
    Console.WriteLine();
}

// ===========================================================================
// Entity and DbContext
// ===========================================================================

public class Event
{
    public ulong Id { get; set; }
    public string EventType { get; set; } = "";
    public string Category { get; set; } = "";
    public ulong UserId { get; set; }
    public double Score { get; set; }
    public string Message { get; set; } = "";
    public byte Active { get; set; }
    public DateTime CreatedAt { get; set; }
    public ulong Version { get; set; }
}

public class EventsContext(string connectionString) : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasNoKey();
            entity.UseReplacingMergeTree(x => x.Version, x => new { x.Id })
                .HasSampleBy("Id");
        });
    }
}
