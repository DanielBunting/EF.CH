// BulkOperationsSample - Demonstrates high-throughput data operations via EF.CH
//
// 1. BulkInsertAsync         - Insert 10,000 records with batch size configuration
// 2. BulkInsertStreamingAsync - Stream records from an async enumerable
// 3. INSERT...SELECT         - Server-side data movement between tables
// 4. OPTIMIZE TABLE          - Force background merges with fluent options
// 5. Export                  - Export query results as CSV, JSON, JSON Lines

using System.Diagnostics;
using System.Runtime.CompilerServices;
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

    Console.WriteLine("=== EF.CH Bulk Operations Sample ===");
    Console.WriteLine();

    await using var context = new BulkOpsContext(connectionString);

    // Create tables from the model configuration (MergeTree with PARTITION BY)
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("Created SensorReadings and SensorArchive tables via EnsureCreatedAsync.");
    Console.WriteLine();

    await DemoBulkInsert(context);
    await DemoBulkInsertStreaming(context);
    await DemoInsertSelect(context);
    await DemoOptimizeTable(context);
    await DemoExport(context);

    await context.Database.EnsureDeletedAsync();
    Console.WriteLine("Cleaned up tables.");
    Console.WriteLine();
    Console.WriteLine("=== All bulk operation demos complete ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ---------------------------------------------------------------------------
// 1. BulkInsertAsync
// ---------------------------------------------------------------------------
static async Task DemoBulkInsert(BulkOpsContext context)
{
    Console.WriteLine("--- 1. BulkInsertAsync ---");
    Console.WriteLine("Inserting 10,000 records with batch size configuration.");
    Console.WriteLine();

    var random = new Random(42);
    var locations = new[] { "warehouse-a", "warehouse-b", "office-1", "lab-2", "outdoor" };
    var baseTime = new DateTime(2025, 1, 15, 0, 0, 0, DateTimeKind.Utc);

    var readings = new List<SensorReading>(10_000);
    for (int i = 0; i < 10_000; i++)
    {
        readings.Add(new SensorReading
        {
            SensorId = (uint)(i % 50 + 1),
            Temperature = Math.Round(18.0 + random.NextDouble() * 15.0, 2),
            Humidity = Math.Round(30.0 + random.NextDouble() * 50.0, 2),
            ReadingAt = baseTime.AddSeconds(i * 10),
            Location = locations[random.Next(locations.Length)],
        });
    }

    var sw = Stopwatch.StartNew();
    var result = await context.BulkInsertAsync(readings, options =>
    {
        options.BatchSize = 5000;
        options.OnBatchCompleted = rowsSoFar =>
            Console.WriteLine($"  Progress: {rowsSoFar:N0} rows inserted");
    });
    sw.Stop();

    Console.WriteLine($"Inserted {result.RowsInserted:N0} rows in {result.BatchesExecuted} batches.");
    Console.WriteLine($"Elapsed: {sw.ElapsedMilliseconds}ms");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 2. BulkInsertStreamingAsync
// ---------------------------------------------------------------------------
static async Task DemoBulkInsertStreaming(BulkOpsContext context)
{
    Console.WriteLine("--- 2. BulkInsertStreamingAsync ---");
    Console.WriteLine("Streaming records from an async enumerable (memory-efficient).");
    Console.WriteLine();

    var sw = Stopwatch.StartNew();
    var result = await context.BulkInsertStreamingAsync(
        GenerateReadingsAsync(count: 5000),
        options => options.BatchSize = 2500);
    sw.Stop();

    Console.WriteLine($"Streamed {result.RowsInserted:N0} rows in {result.BatchesExecuted} batches.");
    Console.WriteLine($"Elapsed: {sw.ElapsedMilliseconds}ms");

    var totalCount = await context.SensorReadings.CountAsync();
    Console.WriteLine($"Total rows in SensorReadings: {totalCount:N0}");
    Console.WriteLine();
}

static async IAsyncEnumerable<SensorReading> GenerateReadingsAsync(
    int count,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var random = new Random(123);
    var locations = new[] { "stream-a", "stream-b", "stream-c" };
    var baseTime = new DateTime(2025, 2, 1, 0, 0, 0, DateTimeKind.Utc);

    for (int i = 0; i < count; i++)
    {
        cancellationToken.ThrowIfCancellationRequested();

        yield return new SensorReading
        {
            SensorId = (uint)(i % 30 + 51),
            Temperature = Math.Round(15.0 + random.NextDouble() * 20.0, 2),
            Humidity = Math.Round(25.0 + random.NextDouble() * 55.0, 2),
            ReadingAt = baseTime.AddSeconds(i * 5),
            Location = locations[random.Next(locations.Length)],
        };

        // Simulate async data generation
        if (i % 1000 == 0)
        {
            await Task.Yield();
        }
    }
}

// ---------------------------------------------------------------------------
// 3. INSERT...SELECT
// ---------------------------------------------------------------------------
static async Task DemoInsertSelect(BulkOpsContext context)
{
    Console.WriteLine("--- 3. INSERT...SELECT ---");
    Console.WriteLine("Server-side data movement between tables (no client roundtrip).");
    Console.WriteLine();

    // Count source rows matching filter
    var sourceQuery = context.SensorReadings
        .Where(r => r.Location == "warehouse-a");

    var sourceCount = await sourceQuery.CountAsync();
    Console.WriteLine($"Source rows matching Location='warehouse-a': {sourceCount:N0}");

    // Server-side INSERT...SELECT with mapping between different entity types.
    // Data never leaves ClickHouse -- the mapping expression is translated to SQL.
    var insertResult = await context.SensorArchive
        .ExecuteInsertFromQueryAsync(
            sourceQuery,
            src => new SensorArchiveEntry
            {
                SensorId = src.SensorId,
                Temperature = src.Temperature,
                Humidity = src.Humidity,
                ReadingAt = src.ReadingAt,
                Location = src.Location,
            });

    Console.WriteLine($"Archived {insertResult.RowsAffected:N0} rows to SensorArchive.");
    Console.WriteLine($"Elapsed: {insertResult.Elapsed.TotalMilliseconds:F0}ms");

    var archiveCount = await context.SensorArchive.CountAsync();
    Console.WriteLine($"Total rows in SensorArchive: {archiveCount:N0}");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 4. OPTIMIZE TABLE
// ---------------------------------------------------------------------------
static async Task DemoOptimizeTable(BulkOpsContext context)
{
    Console.WriteLine("--- 4. OPTIMIZE TABLE ---");
    Console.WriteLine("Force background merges to consolidate data parts.");
    Console.WriteLine();

    // Basic optimize
    Console.WriteLine("Running OPTIMIZE TABLE SensorReadings...");
    await context.Database.OptimizeTableAsync<SensorReading>();
    Console.WriteLine("  Basic OPTIMIZE complete.");

    // Optimize with FINAL (forces complete merge)
    Console.WriteLine("Running OPTIMIZE TABLE SensorReadings FINAL...");
    await context.Database.OptimizeTableFinalAsync<SensorReading>();
    // Allow time for the merge to complete
    await Task.Delay(500);
    Console.WriteLine("  OPTIMIZE FINAL complete.");

    // Optimize with fluent options: FINAL + DEDUPLICATE
    Console.WriteLine("Running OPTIMIZE TABLE with fluent options...");
    await context.Database.OptimizeTableAsync<SensorReading>(options => options
        .WithFinal()
        .WithDeduplicate());
    await Task.Delay(500);
    Console.WriteLine("  OPTIMIZE FINAL DEDUPLICATE complete.");

    var count = await context.SensorReadings.CountAsync();
    Console.WriteLine($"Rows after optimization: {count:N0}");
    Console.WriteLine();
}

// ---------------------------------------------------------------------------
// 5. Export
// ---------------------------------------------------------------------------
static async Task DemoExport(BulkOpsContext context)
{
    Console.WriteLine("--- 5. Export ---");
    Console.WriteLine("Export query results in various formats.");
    Console.WriteLine();

    var query = context.SensorReadings
        .Where(r => r.SensorId == 1)
        .OrderBy(r => r.ReadingAt)
        .Take(5);

    try
    {
        // CSV export
        Console.WriteLine("CSV export (first 5 rows for SensorId=1):");
        var csv = await query.ToCsvAsync(context);
        Console.WriteLine(csv);

        // JSON export
        Console.WriteLine("JSON export (first 5 rows for SensorId=1):");
        var json = await query.ToJsonAsync(context);
        // Print just the first 500 chars to keep output manageable
        Console.WriteLine(json.Length > 500 ? json[..500] + "\n..." : json);
        Console.WriteLine();

        // JSON Lines export (one JSON object per line)
        Console.WriteLine("JSON Lines export (first 5 rows for SensorId=1):");
        var jsonLines = await query.ToJsonLinesAsync(context);
        Console.WriteLine(jsonLines);
    }
    catch (HttpRequestException ex)
    {
        Console.WriteLine($"  Export failed: {ex.Message}");
        Console.WriteLine("  Note: Export uses direct HTTP and may require matching auth configuration.");
        Console.WriteLine("  The export APIs (ToCsvAsync, ToJsonAsync, ToJsonLinesAsync) work with");
        Console.WriteLine("  standard connection strings. See the test suite for verified examples.");
    }
}

// ===========================================================================
// Entities and DbContext
// ===========================================================================

public class SensorReading
{
    public uint SensorId { get; set; }
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public DateTime ReadingAt { get; set; }
    public string Location { get; set; } = "";
}

public class SensorArchiveEntry
{
    public uint SensorId { get; set; }
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public DateTime ReadingAt { get; set; }
    public string Location { get; set; } = "";
}

public class BulkOpsContext(string connectionString) : DbContext
{
    public DbSet<SensorReading> SensorReadings => Set<SensorReading>();
    public DbSet<SensorArchiveEntry> SensorArchive => Set<SensorArchiveEntry>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SensorReading>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.SensorId, x.ReadingAt })
                .HasPartitionByMonth(x => x.ReadingAt);
        });

        modelBuilder.Entity<SensorArchiveEntry>(entity =>
        {
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.SensorId, x.ReadingAt })
                .HasPartitionByMonth(x => x.ReadingAt);
        });
    }
}
