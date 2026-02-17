using System.Diagnostics;
using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Bulk Insert Sample
// ============================================================
// Demonstrates high-performance bulk insert operations:
// - BulkInsertAsync() for batch insert of collections
// - BulkInsertStreamingAsync() for IAsyncEnumerable streams
// - Options: batch size, formats, parallelism, progress
// - Performance comparison vs AddRange/SaveChangesAsync
// ============================================================

Console.WriteLine("Bulk Insert Sample");
Console.WriteLine("==================\n");

await using var context = new EventDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Clean up any existing data
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Products");

// ============================================================
// 1. Basic Bulk Insert
// ============================================================

Console.WriteLine("\n--- Basic Bulk Insert ---");

var events = GenerateEvents(10_000);

var result = await context.BulkInsertAsync(events);

Console.WriteLine($"Inserted {result.RowsInserted:N0} rows");
Console.WriteLine($"Batches: {result.BatchesExecuted}");
Console.WriteLine($"Elapsed: {result.Elapsed.TotalMilliseconds:F0}ms");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

// ============================================================
// 2. Bulk Insert with Custom Batch Size
// ============================================================

Console.WriteLine("\n--- Bulk Insert with Batch Size ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

events = GenerateEvents(25_000);

result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(5_000));

Console.WriteLine($"Inserted {result.RowsInserted:N0} rows in {result.BatchesExecuted} batches");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

// ============================================================
// 3. Progress Callback
// ============================================================

Console.WriteLine("\n--- Progress Callback ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

events = GenerateEvents(30_000);

result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(10_000)
    .WithProgressCallback(rowsInserted =>
    {
        Console.WriteLine($"  Progress: {rowsInserted:N0} rows inserted");
    }));

Console.WriteLine($"Complete: {result.RowsInserted:N0} rows in {result.Elapsed.TotalMilliseconds:F0}ms");

// ============================================================
// 4. Streaming Insert with IAsyncEnumerable
// ============================================================

Console.WriteLine("\n--- Streaming Insert (IAsyncEnumerable) ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

result = await context.BulkInsertStreamingAsync(
    GenerateEventsAsync(50_000),
    options => options
        .WithBatchSize(10_000)
        .WithProgressCallback(rows => Console.WriteLine($"  Streamed: {rows:N0} rows")));

Console.WriteLine($"Complete: {result.RowsInserted:N0} rows in {result.Elapsed.TotalMilliseconds:F0}ms");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

// ============================================================
// 5. JSONEachRow Format
// ============================================================

Console.WriteLine("\n--- JSONEachRow Format ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

events = GenerateEvents(10_000);

result = await context.BulkInsertAsync(events, options => options
    .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow));

Console.WriteLine($"Inserted {result.RowsInserted:N0} rows using JSONEachRow format");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

// ============================================================
// 6. Async Insert Mode
// ============================================================

Console.WriteLine("\n--- Async Insert Mode ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

events = GenerateEvents(10_000);

result = await context.BulkInsertAsync(events, options => options
    .WithAsyncInsert(wait: true));

Console.WriteLine($"Inserted {result.RowsInserted:N0} rows with async_insert=1");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

// ============================================================
// 7. Parallel Insertion
// ============================================================

Console.WriteLine("\n--- Parallel Insertion ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

events = GenerateEvents(100_000);

result = await context.BulkInsertAsync(events, options => options
    .WithBatchSize(10_000)
    .WithParallelism(4)
    .WithProgressCallback(rows => Console.WriteLine($"  Progress: {rows:N0} rows")));

Console.WriteLine($"Complete: {result.RowsInserted:N0} rows in {result.Elapsed.TotalMilliseconds:F0}ms");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec (4 parallel connections)");

// ============================================================
// 8. Complex Types (Arrays and Maps)
// ============================================================

Console.WriteLine("\n--- Complex Types (Arrays, Maps) ---");

var products = GenerateProducts(5_000);

result = await context.BulkInsertAsync(products, options => options
    .WithBatchSize(1_000));

Console.WriteLine($"Inserted {result.RowsInserted:N0} products with arrays and maps");
Console.WriteLine($"Throughput: {result.RowsPerSecond:N0} rows/sec");

// ============================================================
// 9. Custom ClickHouse Settings
// ============================================================

Console.WriteLine("\n--- Custom ClickHouse Settings ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

events = GenerateEvents(10_000);

result = await context.BulkInsertAsync(events, options => options
    .WithMaxInsertThreads(4)
    .WithSetting("insert_quorum", 0)
    .WithTimeout(TimeSpan.FromMinutes(5)));

Console.WriteLine($"Inserted {result.RowsInserted:N0} rows with custom settings");

// ============================================================
// 10. Performance Comparison vs SaveChangesAsync
// ============================================================

Console.WriteLine("\n--- Performance Comparison ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

const int comparisonCount = 5_000;

// Standard EF Core approach
Console.WriteLine($"\nStandard EF Core (AddRange + SaveChangesAsync) - {comparisonCount:N0} rows:");
var standardEvents = GenerateEvents(comparisonCount);

var sw = Stopwatch.StartNew();
context.Events.AddRange(standardEvents);
await context.SaveChangesAsync();
sw.Stop();

var standardElapsed = sw.Elapsed;
var standardRowsPerSec = comparisonCount / standardElapsed.TotalSeconds;
Console.WriteLine($"  Elapsed: {standardElapsed.TotalMilliseconds:F0}ms");
Console.WriteLine($"  Throughput: {standardRowsPerSec:N0} rows/sec");

// Clear change tracker
context.ChangeTracker.Clear();
await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Events");

// Bulk insert approach
Console.WriteLine($"\nBulk Insert - {comparisonCount:N0} rows:");
var bulkEvents = GenerateEvents(comparisonCount);

result = await context.BulkInsertAsync(bulkEvents);

Console.WriteLine($"  Elapsed: {result.Elapsed.TotalMilliseconds:F0}ms");
Console.WriteLine($"  Throughput: {result.RowsPerSecond:N0} rows/sec");

var speedup = standardElapsed.TotalMilliseconds / result.Elapsed.TotalMilliseconds;
Console.WriteLine($"\nBulk insert is {speedup:F1}x faster!");

// ============================================================
// 11. Via DbSet
// ============================================================

Console.WriteLine("\n--- Via DbSet ---");

await context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE Products");

var moreProducts = GenerateProducts(2_000);

result = await context.Products.BulkInsertAsync(moreProducts);

Console.WriteLine($"Inserted {result.RowsInserted:N0} products via DbSet.BulkInsertAsync()");

Console.WriteLine("\nDone!");

// ============================================================
// Helper Methods
// ============================================================

static List<Event> GenerateEvents(int count)
{
    var random = new Random(42);
    var events = new List<Event>(count);

    for (var i = 0; i < count; i++)
    {
        events.Add(new Event
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow.AddMinutes(-random.Next(10000)),
            EventType = random.Next(5) switch
            {
                0 => "page_view",
                1 => "click",
                2 => "purchase",
                3 => "signup",
                _ => "logout"
            },
            Data = $"{{\"index\": {i}}}"
        });
    }

    return events;
}

static async IAsyncEnumerable<Event> GenerateEventsAsync(int count)
{
    var random = new Random(42);

    for (var i = 0; i < count; i++)
    {
        yield return new Event
        {
            Id = Guid.NewGuid(),
            Timestamp = DateTime.UtcNow.AddMinutes(-random.Next(10000)),
            EventType = random.Next(5) switch
            {
                0 => "page_view",
                1 => "click",
                2 => "purchase",
                3 => "signup",
                _ => "logout"
            },
            Data = $"{{\"index\": {i}}}"
        };

        // Simulate async data source
        if (i % 1000 == 0)
        {
            await Task.Yield();
        }
    }
}

static List<Product> GenerateProducts(int count)
{
    var random = new Random(42);
    var products = new List<Product>(count);
    var categories = new[] { "Electronics", "Clothing", "Books", "Home", "Sports" };

    for (var i = 0; i < count; i++)
    {
        products.Add(new Product
        {
            Id = Guid.NewGuid(),
            Name = $"Product {i}",
            Price = Math.Round((decimal)(random.NextDouble() * 1000), 2),
            Tags = Enumerable.Range(0, random.Next(1, 5))
                .Select(_ => categories[random.Next(categories.Length)])
                .Distinct()
                .ToArray(),
            Metadata = new Dictionary<string, string>
            {
                ["category"] = categories[random.Next(categories.Length)],
                ["sku"] = $"SKU-{i:D6}",
                ["warehouse"] = $"WH-{random.Next(1, 10)}"
            }
        });
    }

    return products;
}

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Simple event entity for bulk insert demonstration.
/// </summary>
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string? Data { get; set; }
}

/// <summary>
/// Product entity demonstrating complex types (Array, Map).
/// </summary>
public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string[] Tags { get; set; } = Array.Empty<string>();
    public Dictionary<string, string> Metadata { get; set; } = new();
}

// ============================================================
// DbContext Definition
// ============================================================

public class EventDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();
    public DbSet<Product> Products => Set<Product>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=bulk_insert_sample");
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

        modelBuilder.Entity<Product>(entity =>
        {
            entity.ToTable("Products");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);

            // Array type
            entity.Property(e => e.Tags)
                .HasColumnType("Array(String)");

            // Map type
            entity.Property(e => e.Metadata)
                .HasColumnType("Map(String, String)");
        });
    }
}
