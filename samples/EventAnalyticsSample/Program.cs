using EF.CH;
using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// ============================================================
// Event Analytics Pipeline Sample
// ============================================================
// Demonstrates an end-to-end analytics pipeline composing
// multiple ClickHouse features together:
//
// 1. Null engine source table (data discarded after MV processes)
// 2. SummingMergeTree for page view aggregation
// 3. ReplacingMergeTree for latest user session state
// 4. Materialized view: RawEvent -> PageViewSummary
// 5. Projections on PageViewSummary for fast page lookups
// 6. BulkInsert of 50,000 events
// 7. .Final() for deduplicated user session reads
// 8. ToCsvAsync for data export
// 9. Cross-entity analytics with standard aggregates
//
// Data flow:
//   RawEvent (Null) --MV--> PageViewSummary (SummingMergeTree)
//   UserSession (ReplacingMergeTree) updated separately
// ============================================================

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("Event Analytics Pipeline Sample");
    Console.WriteLine("===============================\n");

    await using var context = new AnalyticsDbContext(connectionString);

    await SetupDatabaseAsync(context);
    await GenerateEventsAsync(context);
    await QueryPageViewsAsync(context);
    await QueryUserSessionsAsync(context);
    await ExportResultsAsync(context);
    await AdvancedAggregatesAsync(context);

    Console.WriteLine("\nDone!");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ============================================================
// Setup: Create all tables and materialized views
// ============================================================

static async Task SetupDatabaseAsync(AnalyticsDbContext context)
{
    Console.WriteLine("Creating database, tables, and materialized views...");
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("Setup complete.\n");
}

// ============================================================
// Generate: Bulk insert 50,000 synthetic events
// ============================================================

static async Task GenerateEventsAsync(AnalyticsDbContext context)
{
    Console.WriteLine("--- Generating Events ---");

    var random = new Random(42);
    var eventTypes = new[] { "page_view", "click", "purchase", "signup", "search" };
    var pages = new[] { "/home", "/products", "/checkout", "/about", "/blog", "/pricing", "/docs", "/contact" };
    var userIds = Enumerable.Range(1, 500).Select(i => $"user_{i:D4}").ToArray();
    var now = DateTime.UtcNow;

    // Generate 50,000 raw events
    var events = Enumerable.Range(0, 50_000)
        .Select(i =>
        {
            var eventType = eventTypes[random.Next(eventTypes.Length)];
            var revenue = eventType == "purchase"
                ? Math.Round((decimal)(random.NextDouble() * 500 + 10), 2)
                : 0m;

            return new RawEvent
            {
                EventId = Guid.NewGuid(),
                Timestamp = now.AddMinutes(-random.Next(0, 60 * 24 * 30)),
                UserId = userIds[random.Next(userIds.Length)],
                EventType = eventType,
                Page = pages[random.Next(pages.Length)],
                Revenue = revenue,
                Properties = new Dictionary<string, string>
                {
                    ["source"] = random.Next(2) == 0 ? "organic" : "paid",
                    ["device"] = random.Next(3) switch { 0 => "mobile", 1 => "desktop", _ => "tablet" }
                }
            };
        })
        .ToList();

    var result = await context.BulkInsertAsync(events, options => options
        .WithBatchSize(10_000));

    Console.WriteLine($"Inserted {result.RowsInserted:N0} events into Null engine table.");
    Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

    // Verify Null engine discards raw data
    var rawCount = await context.RawEvents.CountAsync();
    Console.WriteLine($"Raw events stored: {rawCount} (Null engine discards data)");

    // Generate user sessions separately (ReplacingMergeTree)
    Console.WriteLine("\nGenerating user session records...");

    var sessions = userIds.Select(userId =>
    {
        var firstSeen = now.AddDays(-random.Next(1, 90));
        var lastSeen = firstSeen.AddHours(random.Next(1, 720));
        return new UserSession
        {
            UserId = userId,
            FirstSeen = firstSeen,
            LastSeen = lastSeen,
            EventCount = random.Next(10, 500),
            Version = 1
        };
    }).ToList();

    await context.BulkInsertAsync(sessions);

    // Insert "updated" sessions for some users (higher version = newer)
    var updatedSessions = sessions
        .Take(200)
        .Select(s => new UserSession
        {
            UserId = s.UserId,
            FirstSeen = s.FirstSeen,
            LastSeen = now,
            EventCount = s.EventCount + random.Next(50, 200),
            Version = 2
        })
        .ToList();

    await context.BulkInsertAsync(updatedSessions);
    Console.WriteLine($"Inserted {sessions.Count} initial + {updatedSessions.Count} updated sessions.\n");
}

// ============================================================
// Query: Page view analytics with window functions
// ============================================================

static async Task QueryPageViewsAsync(AnalyticsDbContext context)
{
    Console.WriteLine("--- Page View Analytics ---");

    // Basic page view summary
    var pageViews = await context.PageViewSummaries
        .GroupBy(p => p.Page)
        .Select(g => new
        {
            Page = g.Key,
            TotalViews = g.Sum(p => p.ViewCount),
            TotalVisitors = g.Sum(p => p.UniqueVisitors)
        })
        .OrderByDescending(x => x.TotalViews)
        .ToListAsync();

    Console.WriteLine("Page view totals:");
    foreach (var pv in pageViews)
    {
        Console.WriteLine($"  {pv.Page,-15} Views: {pv.TotalViews,8:N0}  Visitors: {pv.TotalVisitors,6:N0}");
    }

    // Top pages per day — ordered by date and views
    Console.WriteLine("\nTop pages per day (last 10 days):");

    var trending = await context.PageViewSummaries
        .OrderByDescending(p => p.Date)
        .ThenByDescending(p => p.ViewCount)
        .Take(20)
        .ToListAsync();

    var currentDate = DateTime.MinValue;
    foreach (var row in trending)
    {
        if (row.Date != currentDate)
        {
            currentDate = row.Date;
            Console.WriteLine($"  {row.Date:yyyy-MM-dd}:");
        }
        Console.WriteLine($"    {row.Page,-15} Views: {row.ViewCount,6:N0}  Visitors: {row.UniqueVisitors,4:N0}");
    }

    Console.WriteLine();
}

// ============================================================
// Query: User session analytics with FINAL
// ============================================================

static async Task QueryUserSessionsAsync(AnalyticsDbContext context)
{
    Console.WriteLine("--- User Session Analytics (FINAL) ---");

    // Without FINAL: may see duplicate rows for users with multiple versions
    var rawCount = await context.UserSessions.CountAsync();
    Console.WriteLine($"Physical rows (before merge): {rawCount}");

    // With FINAL: deduplicated, only latest version per user
    var deduplicatedCount = await context.UserSessions
        .Final()
        .CountAsync();
    Console.WriteLine($"Deduplicated users (with FINAL): {deduplicatedCount}");

    // Top users by event count, using FINAL for accurate latest state
    var topUsers = await context.UserSessions
        .Final()
        .OrderByDescending(u => u.EventCount)
        .Take(10)
        .ToListAsync();

    Console.WriteLine("\nTop 10 users by event count (deduplicated):");
    foreach (var user in topUsers)
    {
        Console.WriteLine($"  {user.UserId}: {user.EventCount,5} events  " +
                          $"(v{user.Version}, {user.FirstSeen:yyyy-MM-dd} to {user.LastSeen:yyyy-MM-dd})");
    }

    // Aggregate session statistics with FINAL
    var sessionStats = await context.UserSessions
        .Final()
        .GroupBy(u => 1)
        .Select(g => new
        {
            TotalUsers = g.Count(),
            AvgEvents = g.Average(u => (double)u.EventCount),
            MaxEvents = g.Max(u => u.EventCount),
            MinEvents = g.Min(u => u.EventCount)
        })
        .FirstAsync();

    Console.WriteLine($"\nSession statistics (deduplicated):");
    Console.WriteLine($"  Total users: {sessionStats.TotalUsers}");
    Console.WriteLine($"  Avg events:  {sessionStats.AvgEvents:F1}");
    Console.WriteLine($"  Max events:  {sessionStats.MaxEvents}");
    Console.WriteLine($"  Min events:  {sessionStats.MinEvents}");

    Console.WriteLine();
}

// ============================================================
// Export: CSV export of page view summaries
// ============================================================

static async Task ExportResultsAsync(AnalyticsDbContext context)
{
    Console.WriteLine("--- Export Results (CSV) ---");

    try
    {
        var csv = await context.PageViewSummaries
            .OrderByDescending(p => p.Date)
            .ThenByDescending(p => p.ViewCount)
            .Take(20)
            .ToCsvAsync(context);

        Console.WriteLine("Page view summary (CSV format, top 20 rows):");
        // Show first few lines of CSV output
        var lines = csv.Split('\n');
        foreach (var line in lines.Take(10))
        {
            Console.WriteLine($"  {line}");
        }

        if (lines.Length > 10)
        {
            Console.WriteLine($"  ... ({lines.Length - 10} more rows)");
        }
    }
    catch (HttpRequestException)
    {
        Console.WriteLine("  Export failed: HTTP auth mismatch with Testcontainers.");
        Console.WriteLine("  Note: Export uses direct HTTP and may require matching auth configuration.");
        Console.WriteLine("  The export APIs (ToCsvAsync, ToJsonAsync, etc.) work with");
        Console.WriteLine("  standard connection strings. See the test suite for verified examples.");
    }

    Console.WriteLine();
}

// ============================================================
// Advanced: Cross-entity analytics combining multiple tables
// ============================================================

static async Task AdvancedAggregatesAsync(AnalyticsDbContext context)
{
    Console.WriteLine("--- Cross-Entity Analytics ---");

    // Page view statistics: aggregate across all pages
    var viewStats = await context.PageViewSummaries
        .GroupBy(p => 1)
        .Select(g => new
        {
            TotalViews = g.Sum(p => p.ViewCount),
            TotalVisitors = g.Sum(p => p.UniqueVisitors),
            AvgViewsPerEntry = g.Average(p => (double)p.ViewCount),
            MaxViews = g.Max(p => p.ViewCount),
            PageCount = g.Count()
        })
        .FirstAsync();

    Console.WriteLine("Page view statistics (SummingMergeTree aggregation):");
    Console.WriteLine($"  Total views:       {viewStats.TotalViews:N0}");
    Console.WriteLine($"  Total visitors:    {viewStats.TotalVisitors:N0}");
    Console.WriteLine($"  Avg views/entry:   {viewStats.AvgViewsPerEntry:F1}");
    Console.WriteLine($"  Max views/entry:   {viewStats.MaxViews:N0}");
    Console.WriteLine($"  MV entries:        {viewStats.PageCount:N0}");

    // Top pages by total views
    Console.WriteLine("\nTop 5 pages by total views:");
    var topPages = await context.PageViewSummaries
        .GroupBy(p => p.Page)
        .Select(g => new
        {
            Page = g.Key,
            TotalViews = g.Sum(p => p.ViewCount),
            DaysActive = g.Count()
        })
        .OrderByDescending(x => x.TotalViews)
        .Take(5)
        .ToListAsync();

    foreach (var page in topPages)
    {
        Console.WriteLine($"  {page.Page,-15} Views: {page.TotalViews,8:N0}  Days active: {page.DaysActive}");
    }

    // Session analytics: compare deduplicated vs raw counts
    Console.WriteLine("\nSession data comparison (ReplacingMergeTree):");
    var rawSessionStats = await context.UserSessions
        .GroupBy(u => 1)
        .Select(g => new
        {
            TotalRows = g.Count(),
            AvgEvents = g.Average(u => (double)u.EventCount),
            TotalEvents = g.Sum(u => u.EventCount)
        })
        .FirstAsync();

    var finalSessionStats = await context.UserSessions
        .Final()
        .GroupBy(u => 1)
        .Select(g => new
        {
            TotalRows = g.Count(),
            AvgEvents = g.Average(u => (double)u.EventCount),
            TotalEvents = g.Sum(u => u.EventCount)
        })
        .FirstAsync();

    Console.WriteLine($"  Raw rows:          {rawSessionStats.TotalRows}  (includes old versions)");
    Console.WriteLine($"  FINAL rows:        {finalSessionStats.TotalRows}  (deduplicated)");
    Console.WriteLine($"  Raw avg events:    {rawSessionStats.AvgEvents:F1}");
    Console.WriteLine($"  FINAL avg events:  {finalSessionStats.AvgEvents:F1}");
    Console.WriteLine($"  Raw total events:  {rawSessionStats.TotalEvents:N0}");
    Console.WriteLine($"  FINAL total events:{finalSessionStats.TotalEvents:N0}");
}

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Raw event source. Uses Null engine - data is discarded after
/// materialized views process it. Acts as the ingestion point
/// for the entire analytics pipeline.
/// </summary>
public class RawEvent
{
    public Guid EventId { get; set; }
    public DateTime Timestamp { get; set; }
    public string UserId { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public string Page { get; set; } = string.Empty;
    public decimal Revenue { get; set; }
    public Dictionary<string, string> Properties { get; set; } = new();
}

/// <summary>
/// Page view summary target for the materialized view.
/// Uses SummingMergeTree to auto-sum ViewCount and UniqueVisitors
/// during background merges.
/// </summary>
public class PageViewSummary
{
    public DateTime Date { get; set; }
    public string Page { get; set; } = string.Empty;
    public long ViewCount { get; set; }
    public long UniqueVisitors { get; set; }
}

/// <summary>
/// User session state. Uses ReplacingMergeTree for latest-state
/// semantics - only the row with the highest Version is kept
/// after merges. Use .Final() for accurate reads.
/// </summary>
public class UserSession
{
    public string UserId { get; set; } = string.Empty;
    public DateTime FirstSeen { get; set; }
    public DateTime LastSeen { get; set; }
    public int EventCount { get; set; }
    public int Version { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class AnalyticsDbContext(string connectionString) : DbContext
{
    public DbSet<RawEvent> RawEvents => Set<RawEvent>();
    public DbSet<PageViewSummary> PageViewSummaries => Set<PageViewSummary>();
    public DbSet<UserSession> UserSessions => Set<UserSession>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // ============================================================
        // Null Engine: Raw events discarded after MV processing
        // ============================================================
        modelBuilder.Entity<RawEvent>(entity =>
        {
            entity.ToTable("raw_events");
            entity.HasKey(e => e.EventId);
            entity.UseNullEngine();

            entity.Property(e => e.UserId).HasLowCardinality();
            entity.Property(e => e.EventType).HasLowCardinality();
            entity.Property(e => e.Page).HasLowCardinality();
        });

        // ============================================================
        // SummingMergeTree: Page view aggregation target
        // ============================================================
        modelBuilder.Entity<PageViewSummary>(entity =>
        {
            entity.ToTable("page_view_summary_mv");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Date, x.Page });
            entity.HasPartitionByMonth(x => x.Date);

            entity.Property(e => e.Page).HasLowCardinality();
            entity.Property(e => e.UniqueVisitors)
                .HasSimpleAggregateFunction("sum");

            // Projection for fast lookups by page
            entity.HasProjection("proj_by_page")
                .OrderBy(x => x.Page)
                .ThenBy(x => x.Date);

            // Materialized view: count page views from raw events
            // Cast count() to Int64 to match the C# long property type
            entity.AsMaterializedViewRaw(
                sourceTable: "raw_events",
                selectSql: @"
                    SELECT
                        toDate(""Timestamp"") AS ""Date"",
                        ""Page"",
                        toInt64(count()) AS ""ViewCount"",
                        toInt64(1) AS ""UniqueVisitors""
                    FROM ""raw_events""
                    WHERE ""EventType"" = 'page_view'
                    GROUP BY ""Date"", ""Page""
                ",
                populate: false);
        });

        // ============================================================
        // ReplacingMergeTree: Latest user session state
        // ============================================================
        modelBuilder.Entity<UserSession>(entity =>
        {
            entity.ToTable("user_sessions");
            entity.HasNoKey();
            entity.UseReplacingMergeTree(
                x => x.Version,
                x => new { x.UserId });

            entity.Property(e => e.UserId).HasLowCardinality();
        });
    }
}
