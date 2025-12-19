using EF.CH;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Event Aggregation Sample
// ============================================================
// Demonstrates the Null Engine + Materialized Views + TTL pattern:
// - Raw events are inserted into a Null table (discarded after MVs process them)
// - Hourly aggregates are stored with 1-year TTL
// - Daily aggregates are stored with 5-year TTL
//
// This pattern is ideal for high-volume event ingestion where:
// - Raw events are too large to store
// - Pre-aggregated data is sufficient for analysis
// ============================================================

Console.WriteLine("Event Aggregation Sample");
Console.WriteLine("========================\n");

await using var context = new EventDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert raw events (will be discarded after MV processing)
Console.WriteLine("Inserting 10,000 raw events...\n");

var random = new Random(42);
var categories = new[] { "sales", "returns", "support", "marketing" };
var now = DateTime.UtcNow;

var events = Enumerable.Range(0, 10000)
    .Select(i => new RawEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = now.AddMinutes(-random.Next(0, 60 * 24 * 7)), // Last 7 days
        Category = categories[random.Next(categories.Length)],
        Amount = Math.Round((decimal)(random.NextDouble() * 1000), 2)
    })
    .ToList();

context.RawEvents.AddRange(events);
await context.SaveChangesAsync();

Console.WriteLine($"Inserted {events.Count} events into Null table.");
Console.WriteLine("Raw events were discarded - Null engine stores nothing.\n");

// Verify raw events are empty (Null engine behavior)
var rawCount = await context.RawEvents.CountAsync();
Console.WriteLine($"Raw events table count: {rawCount} (always 0 with Null engine)\n");

// Query hourly summaries (populated by materialized view)
Console.WriteLine("--- Hourly Summaries (last 24 hours) ---");
var hourlySummaries = await context.HourlySummaries
    .Where(s => s.Hour > now.AddHours(-24))
    .OrderByDescending(s => s.Hour)
    .ThenBy(s => s.Category)
    .ToListAsync();

if (hourlySummaries.Count == 0)
{
    Console.WriteLine("  (No data - materialized view may need OPTIMIZE or data may not have triggered)");
}

foreach (var summary in hourlySummaries.Take(20))
{
    Console.WriteLine($"  {summary.Hour:yyyy-MM-dd HH:00} | {summary.Category,-12}: " +
                      $"{summary.EventCount,5:N0} events, ${summary.TotalAmount,10:N2}");
}

if (hourlySummaries.Count > 20)
{
    Console.WriteLine($"  ... and {hourlySummaries.Count - 20} more rows");
}

// Query daily summaries
Console.WriteLine("\n--- Daily Summaries (all) ---");
var dailySummaries = await context.DailySummaries
    .OrderByDescending(s => s.Date)
    .ThenBy(s => s.Category)
    .ToListAsync();

if (dailySummaries.Count == 0)
{
    Console.WriteLine("  (No data - materialized view may need OPTIMIZE or data may not have triggered)");
}

foreach (var summary in dailySummaries)
{
    Console.WriteLine($"  {summary.Date:yyyy-MM-dd}       | {summary.Category,-12}: " +
                      $"{summary.EventCount,5:N0} events, ${summary.TotalAmount,10:N2}");
}

// Show storage benefit
Console.WriteLine("\n--- Storage Analysis ---");
Console.WriteLine($"Raw events inserted: 10,000");
Console.WriteLine($"Raw events stored:   0 (Null engine)");
Console.WriteLine($"Hourly aggregates:   {hourlySummaries.Count} rows");
Console.WriteLine($"Daily aggregates:    {dailySummaries.Count} rows");
Console.WriteLine($"Storage reduction:   {(hourlySummaries.Count + dailySummaries.Count > 0 ? $"~{10000 / Math.Max(1, hourlySummaries.Count + dailySummaries.Count)}x" : "N/A")}");

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Raw events - inserted into Null table, discarded immediately.
/// Only serves to trigger materialized views.
/// </summary>
public class RawEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

/// <summary>
/// Hourly aggregates - stored with 1-year TTL.
/// Automatically populated by materialized view on RawEvent insert.
/// </summary>
public class HourlySummary
{
    public DateTime Hour { get; set; }
    public string Category { get; set; } = string.Empty;
    public long EventCount { get; set; }
    public decimal TotalAmount { get; set; }
}

/// <summary>
/// Daily aggregates - stored with 5-year TTL.
/// Automatically populated by materialized view on RawEvent insert.
/// </summary>
public class DailySummary
{
    public DateOnly Date { get; set; }
    public string Category { get; set; } = string.Empty;
    public long EventCount { get; set; }
    public decimal TotalAmount { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class EventDbContext : DbContext
{
    public DbSet<RawEvent> RawEvents => Set<RawEvent>();
    public DbSet<HourlySummary> HourlySummaries => Set<HourlySummary>();
    public DbSet<DailySummary> DailySummaries => Set<DailySummary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=event_aggregation_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // ============================================================
        // Null Engine: Raw events discarded after triggering MVs
        // ============================================================
        modelBuilder.Entity<RawEvent>(entity =>
        {
            entity.ToTable("raw_events");
            entity.HasKey(e => e.Id);
            entity.UseNullEngine();  // Data discarded - no ORDER BY needed
        });

        // ============================================================
        // Hourly Aggregates with 1-year TTL
        // ============================================================
        modelBuilder.Entity<HourlySummary>(entity =>
        {
            entity.ToTable("hourly_summary_mv");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Hour, x.Category });
            entity.HasPartitionByMonth(x => x.Hour);
            entity.HasTtl(x => x.Hour, ClickHouseInterval.Years(1));  // TTL with ClickHouseInterval

            // Materialized view using raw SQL (LINQ GroupBy translation not supported)
            entity.AsMaterializedViewRaw(
                sourceTable: "raw_events",
                selectSql: @"
                    SELECT
                        toStartOfHour(""Timestamp"") AS ""Hour"",
                        ""Category"",
                        count() AS ""EventCount"",
                        sum(""Amount"") AS ""TotalAmount""
                    FROM ""raw_events""
                    GROUP BY ""Hour"", ""Category""
                ",
                populate: false);
        });

        // ============================================================
        // Daily Aggregates with 5-year TTL
        // ============================================================
        modelBuilder.Entity<DailySummary>(entity =>
        {
            entity.ToTable("daily_summary_mv");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => new { x.Date, x.Category });
            entity.HasPartitionByYear(x => x.Date);
            entity.HasTtl(x => x.Date, ClickHouseInterval.Years(5));  // Long-term retention

            entity.AsMaterializedViewRaw(
                sourceTable: "raw_events",
                selectSql: @"
                    SELECT
                        toDate(""Timestamp"") AS ""Date"",
                        ""Category"",
                        count() AS ""EventCount"",
                        sum(""Amount"") AS ""TotalAmount""
                    FROM ""raw_events""
                    GROUP BY ""Date"", ""Category""
                ",
                populate: false);
        });
    }
}
