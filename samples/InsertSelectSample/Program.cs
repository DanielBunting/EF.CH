using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// INSERT ... SELECT Sample
// ============================================================
// Demonstrates server-side INSERT ... SELECT operations:
// - Data archival with filtering
// - Cross-table copy with string filters
// - Captured variable parameters (DateTime, decimal, etc.)
// - Reverse fluent API (.InsertIntoAsync())
// - Performance comparison
// ============================================================

Console.WriteLine("INSERT ... SELECT Sample");
Console.WriteLine("========================\n");

await using var context = new SampleDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Clean up any existing data
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE ArchivedEvents");

// ============================================================
// Seed sample data
// ============================================================

Console.WriteLine("\n--- Seeding sample data ---");

var random = new Random(42);
var categories = new[] { "Electronics", "Clothing", "Books", "Home", "Sports" };
var baseDate = DateTime.UtcNow.Date;

var events = Enumerable.Range(0, 10_000)
    .Select(i => new Event
    {
        Id = Guid.NewGuid(),
        Timestamp = baseDate.AddDays(-random.Next(30)).AddHours(random.Next(24)),
        Category = categories[random.Next(categories.Length)],
        Amount = Math.Round((decimal)(random.NextDouble() * 100), 2)
    })
    .ToList();

await context.BulkInsertAsync(events);
Console.WriteLine($"Inserted {events.Count:N0} sample events");

// ============================================================
// 1. Data Archival - Copy filtered records to archive table
// ============================================================

Console.WriteLine("\n--- Data Archival (Copy Electronics) ---");

var electronicsCount = await context.Events.CountAsync(e => e.Category == "Electronics");
Console.WriteLine($"Found {electronicsCount:N0} Electronics events");

var archiveResult = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Category == "Electronics"),
    e => new ArchivedEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Category = e.Category,
        Amount = e.Amount
    });

Console.WriteLine($"Archived in {archiveResult.Elapsed.TotalMilliseconds:F2}ms");
Console.WriteLine($"Generated SQL:\n{archiveResult.Sql}\n");

var archivedCount = await context.ArchivedEvents.CountAsync();
Console.WriteLine($"Archive table now has {archivedCount:N0} records");

// ============================================================
// 2. Data Archival with DateTime - Captured variable parameter
// ============================================================

Console.WriteLine("\n--- Data Archival with DateTime Parameter ---");

// Clear archive for this demo
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE ArchivedEvents");

// Captured DateTime variable - properly resolved
var cutoffDate = DateTime.UtcNow.AddDays(-14);
var dateArchiveResult = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Timestamp < cutoffDate),
    e => new ArchivedEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Category = e.Category,
        Amount = e.Amount
    });

Console.WriteLine($"Archived events older than 14 days in {dateArchiveResult.Elapsed.TotalMilliseconds:F2}ms");
Console.WriteLine($"Generated SQL:\n{dateArchiveResult.Sql}\n");

var oldEventCount = await context.ArchivedEvents.CountAsync();
Console.WriteLine($"Archived {oldEventCount:N0} old events");

// ============================================================
// 3. Cross-table copy with multiple string filters
// ============================================================

Console.WriteLine("\n--- Cross-table Copy (Multiple Filters) ---");

// Clear archive for this demo
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE ArchivedEvents");

var copyResult = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Category == "Clothing" || e.Category == "Books"),
    e => new ArchivedEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Category = e.Category,
        Amount = e.Amount
    });

Console.WriteLine($"Copied Clothing and Books events in {copyResult.Elapsed.TotalMilliseconds:F2}ms");

var copiedCount = await context.ArchivedEvents.CountAsync();
Console.WriteLine($"Copied {copiedCount:N0} records to archive");

// ============================================================
// 4. Reverse Fluent API - .InsertIntoAsync()
// ============================================================

Console.WriteLine("\n--- Reverse Fluent API (.InsertIntoAsync()) ---");

// Clear archive for this demo
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE ArchivedEvents");

var fluentResult = await context.Events
    .Where(e => e.Category == "Sports")
    .InsertIntoAsync(context.ArchivedEvents,
        e => new ArchivedEvent
        {
            Id = e.Id,
            Timestamp = e.Timestamp,
            Category = e.Category,
            Amount = e.Amount
        });

Console.WriteLine($"Fluent insert completed in {fluentResult.Elapsed.TotalMilliseconds:F2}ms");
Console.WriteLine($"Generated SQL:\n{fluentResult.Sql}\n");

var sportsArchived = await context.ArchivedEvents.CountAsync();
Console.WriteLine($"Archived {sportsArchived:N0} Sports category events");

// ============================================================
// 5. Copy all records
// ============================================================

Console.WriteLine("\n--- Copy All Records ---");

// Clear archive for this demo
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE ArchivedEvents");

var allResult = await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events,
    e => new ArchivedEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Category = e.Category,
        Amount = e.Amount
    });

var allCount = await context.ArchivedEvents.CountAsync();
Console.WriteLine($"Copied all {allCount:N0} records in {allResult.Elapsed.TotalMilliseconds:F2}ms");

// ============================================================
// 6. Performance comparison vs client-side
// ============================================================

Console.WriteLine("\n--- Performance Comparison ---");

// Clear archive
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE ArchivedEvents");

// Server-side INSERT ... SELECT
var serverSideStart = DateTime.UtcNow;
await context.ArchivedEvents.ExecuteInsertFromQueryAsync(
    context.Events.Where(e => e.Category == "Home"),
    e => new ArchivedEvent
    {
        Id = e.Id,
        Timestamp = e.Timestamp,
        Category = e.Category,
        Amount = e.Amount
    });
var serverSideTime = DateTime.UtcNow - serverSideStart;

var serverCount = await context.ArchivedEvents.CountAsync();
Console.WriteLine($"Server-side INSERT...SELECT: {serverCount:N0} rows in {serverSideTime.TotalMilliseconds:F2}ms");
Console.WriteLine("  (Data never leaves the server!)");

Console.WriteLine("\nDone!");

// Stop the container if you started it for this sample
Console.WriteLine("\nTo clean up: docker stop clickhouse-sample && docker rm clickhouse-sample");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Source event entity.
/// </summary>
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

/// <summary>
/// Archived event entity - same schema as Event, different table.
/// </summary>
public class ArchivedEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class SampleDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();
    public DbSet<ArchivedEvent> ArchivedEvents => Set<ArchivedEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=insert_select_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByMonth(x => x.Timestamp);
        });

        modelBuilder.Entity<ArchivedEvent>(entity =>
        {
            entity.ToTable("ArchivedEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByMonth(x => x.Timestamp);
        });
    }
}
