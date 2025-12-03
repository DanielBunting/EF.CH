using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Partitioning Sample
// ============================================================
// Demonstrates different partitioning strategies:
// - Monthly partitioning for access logs
// - Daily partitioning for high-volume metrics
// - TTL for automatic data expiration
// ============================================================

Console.WriteLine("Partitioning Sample");
Console.WriteLine("===================\n");

await using var context = new LoggingDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert access logs across multiple months
Console.WriteLine("Inserting access logs...\n");

var accessLogs = new[]
{
    // December logs
    new AccessLog { Id = Guid.NewGuid(), Timestamp = new DateTime(2024, 12, 1, 10, 30, 0), Endpoint = "/api/users", StatusCode = 200, ResponseTimeMs = 45 },
    new AccessLog { Id = Guid.NewGuid(), Timestamp = new DateTime(2024, 12, 15, 14, 22, 0), Endpoint = "/api/orders", StatusCode = 201, ResponseTimeMs = 120 },
    new AccessLog { Id = Guid.NewGuid(), Timestamp = new DateTime(2024, 12, 20, 9, 15, 0), Endpoint = "/api/users", StatusCode = 404, ResponseTimeMs = 12 },

    // January logs
    new AccessLog { Id = Guid.NewGuid(), Timestamp = new DateTime(2025, 1, 5, 11, 45, 0), Endpoint = "/api/products", StatusCode = 200, ResponseTimeMs = 89 },
    new AccessLog { Id = Guid.NewGuid(), Timestamp = new DateTime(2025, 1, 10, 16, 30, 0), Endpoint = "/api/orders", StatusCode = 500, ResponseTimeMs = 250 },

    // February logs
    new AccessLog { Id = Guid.NewGuid(), Timestamp = new DateTime(2025, 2, 1, 8, 0, 0), Endpoint = "/api/users", StatusCode = 200, ResponseTimeMs = 35 },
};

context.AccessLogs.AddRange(accessLogs);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {accessLogs.Length} access logs.\n");

// Insert metrics across multiple days
Console.WriteLine("Inserting metrics...\n");

var metrics = new List<Metric>();
var random = new Random(42);

// Generate metrics for the past 5 days
for (var day = 0; day < 5; day++)
{
    var date = DateTime.UtcNow.Date.AddDays(-day);
    for (var i = 0; i < 10; i++)
    {
        metrics.Add(new Metric
        {
            Id = Guid.NewGuid(),
            Timestamp = date.AddHours(random.Next(24)),
            Name = random.Next(2) == 0 ? "cpu_usage" : "memory_usage",
            Value = random.NextDouble() * 100,
            Host = $"server-{random.Next(1, 4)}"
        });
    }
}

context.Metrics.AddRange(metrics);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {metrics.Count} metrics across 5 days.\n");

// Query with partition pruning
Console.WriteLine("--- Query: December 2024 logs only (partition pruning) ---");
var decemberLogs = await context.AccessLogs
    .Where(l => l.Timestamp >= new DateTime(2024, 12, 1))
    .Where(l => l.Timestamp < new DateTime(2025, 1, 1))
    .OrderBy(l => l.Timestamp)
    .ToListAsync();

foreach (var log in decemberLogs)
{
    Console.WriteLine($"  {log.Timestamp:yyyy-MM-dd HH:mm} | {log.Endpoint} | {log.StatusCode}");
}

Console.WriteLine($"\nFound {decemberLogs.Count} December logs.\n");

// Query metrics with partition pruning
Console.WriteLine("--- Query: Today's metrics only (partition pruning) ---");
var todaysMetrics = await context.Metrics
    .Where(m => m.Timestamp >= DateTime.UtcNow.Date)
    .Where(m => m.Timestamp < DateTime.UtcNow.Date.AddDays(1))
    .OrderBy(m => m.Timestamp)
    .ToListAsync();

foreach (var metric in todaysMetrics.Take(5))
{
    Console.WriteLine($"  {metric.Timestamp:yyyy-MM-dd HH:mm} | {metric.Name} | {metric.Value:F2}% | {metric.Host}");
}
if (todaysMetrics.Count > 5)
{
    Console.WriteLine($"  ... and {todaysMetrics.Count - 5} more");
}

Console.WriteLine($"\nFound {todaysMetrics.Count} today's metrics.\n");

// Aggregation with partition pruning
Console.WriteLine("--- Aggregation: Average response time by endpoint (January) ---");
var avgByEndpoint = await context.AccessLogs
    .Where(l => l.Timestamp >= new DateTime(2025, 1, 1))
    .Where(l => l.Timestamp < new DateTime(2025, 2, 1))
    .GroupBy(l => l.Endpoint)
    .Select(g => new { Endpoint = g.Key, AvgResponseTime = g.Average(l => l.ResponseTimeMs) })
    .ToListAsync();

foreach (var stat in avgByEndpoint)
{
    Console.WriteLine($"  {stat.Endpoint}: {stat.AvgResponseTime:F1}ms avg");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Access log with monthly partitioning.
/// Good for data retained for months/years.
/// </summary>
public class AccessLog
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public int ResponseTimeMs { get; set; }
}

/// <summary>
/// Metric with daily partitioning and TTL.
/// Good for high-volume, short-retention data.
/// </summary>
public class Metric
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
    public string Host { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class LoggingDbContext : DbContext
{
    public DbSet<AccessLog> AccessLogs => Set<AccessLog>();
    public DbSet<Metric> Metrics => Set<Metric>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=partitioning_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Access logs: Monthly partitioning for longer retention
        modelBuilder.Entity<AccessLog>(entity =>
        {
            entity.ToTable("AccessLogs");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });

            // Monthly partitions - good for data kept for months/years
            // Each month's data is stored separately
            entity.HasPartitionByMonth(x => x.Timestamp);

            // TTL: Auto-expire after 1 year
            entity.HasTtl("Timestamp + INTERVAL 1 YEAR");
        });

        // Metrics: Daily partitioning for high-volume, short retention
        modelBuilder.Entity<Metric>(entity =>
        {
            entity.ToTable("Metrics");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });

            // Daily partitions - good for high-volume data with short retention
            // Allows dropping entire days instantly
            entity.HasPartitionByDay(x => x.Timestamp);

            // TTL: Auto-expire after 30 days
            entity.HasTtl("Timestamp + INTERVAL 30 DAY");
        });
    }
}
