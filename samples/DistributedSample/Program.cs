using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// EF.CH Distributed Table Sample with Sharding
// ============================================================
// This sample demonstrates:
// 1. Connecting to a 3-node ClickHouse cluster with sharding
// 2. Using MergeTree for local tables and Distributed for unified view
// 3. Verifying data distribution across shards
//
// Prerequisites:
//   docker compose up -d
//   Wait for cluster to be healthy (about 30 seconds)
//
// Architecture (Sharding - data split across nodes):
// ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
// │ clickhouse1 │    │ clickhouse2 │    │ clickhouse3 │
// │  Shard 1    │    │  Shard 2    │    │  Shard 3    │
// │  Events     │    │  Events     │    │  Events     │
// │  (subset)   │    │  (subset)   │    │  (subset)   │
// └─────────────┘    └─────────────┘    └─────────────┘
//        │                 │                   │
//        └────────────────────────────────────┘
//              Distributed Table (Unified View)
//
// Compare to ClusterSample (Replication):
// - ClusterSample: ALL data on ALL nodes (redundancy)
// - This sample:   Data SPLIT across nodes (scalability)
// ============================================================

Console.WriteLine("EF.CH Distributed Table Sample with Sharding");
Console.WriteLine("=============================================\n");

// Connection strings for all 3 nodes (shards)
var nodes = new[]
{
    ("Shard 1", "Host=localhost;Port=8123;Database=distributed_demo"),
    ("Shard 2", "Host=localhost;Port=8124;Database=distributed_demo"),
    ("Shard 3", "Host=localhost;Port=8125;Database=distributed_demo")
};

// First connect to default database to create our database
Console.WriteLine("Step 1: Creating database on all nodes...");
await using (var context = new DistributedDbContext("Host=localhost;Port=8123;Database=default"))
{
    // Create database on all nodes using ON CLUSTER
    await context.Database.OpenConnectionAsync();
    try
    {
        await using var cmd = context.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = "CREATE DATABASE IF NOT EXISTS distributed_demo ON CLUSTER shard_cluster";
        await cmd.ExecuteNonQueryAsync();
        Console.WriteLine("  Database created on all nodes.\n");
    }
    finally
    {
        await context.Database.CloseConnectionAsync();
    }
}

// Create tables using EF Core migrations
Console.WriteLine("Step 2: Creating local and distributed tables...");
await using (var context = new DistributedDbContext(nodes[0].Item2))
{
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("  Tables created.\n");
}

// Insert events through the distributed table
Console.WriteLine("Step 3: Inserting 100 events through distributed table...");
await using (var context = new DistributedDbContext(nodes[0].Item2))
{
    var random = new Random(42); // Fixed seed for reproducibility
    var eventTypes = new[] { "click", "view", "purchase", "signup", "logout" };

    var events = Enumerable.Range(1, 100).Select(i => new Event
    {
        Id = Guid.NewGuid(),
        EventTime = DateTime.UtcNow.AddMinutes(-random.Next(0, 1440)), // Random time in last 24 hours
        UserId = random.Next(1, 1001), // Random user ID 1-1000
        EventType = eventTypes[random.Next(eventTypes.Length)],
        Properties = $"{{\"index\": {i}}}"
    }).ToList();

    context.Events.AddRange(events);
    await context.SaveChangesAsync();
    Console.WriteLine($"  Inserted {events.Count} events.\n");
}

// Wait for data to settle
Console.WriteLine("Step 4: Waiting for data distribution (2 seconds)...");
await Task.Delay(2000);
Console.WriteLine("  Distribution complete.\n");

// Show data distribution across shards
Console.WriteLine("Step 5: Checking data distribution across shards...\n");
var totalFromShards = 0;
foreach (var (name, connectionString) in nodes)
{
    await using var context = new DistributedDbContext(connectionString);

    // Query the LOCAL table directly to see what's on this shard
    await context.Database.OpenConnectionAsync();
    try
    {
        await using var cmd = context.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = "SELECT count() FROM events_local";
        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        totalFromShards += count;

        // Get host name
        cmd.CommandText = "SELECT hostName()";
        var host = (await cmd.ExecuteScalarAsync())?.ToString();

        Console.WriteLine($"  {name} (host: {host}):");
        Console.WriteLine($"    - Local events: {count}");
    }
    finally
    {
        await context.Database.CloseConnectionAsync();
    }
}
Console.WriteLine($"\n  Total events across all shards: {totalFromShards}");
Console.WriteLine();

// Query through the distributed table (unified view)
Console.WriteLine("Step 6: Querying through distributed table (unified view)...\n");
await using (var context = new DistributedDbContext(nodes[0].Item2))
{
    // Count all events through distributed table
    var totalEvents = await context.Events.CountAsync();
    Console.WriteLine($"  Total events (distributed query): {totalEvents}");

    // Aggregate by event type
    var eventCounts = await context.Events
        .GroupBy(e => e.EventType)
        .Select(g => new { EventType = g.Key, Count = g.Count() })
        .OrderByDescending(x => x.Count)
        .ToListAsync();

    Console.WriteLine("\n  Events by type:");
    foreach (var ec in eventCounts)
    {
        Console.WriteLine($"    - {ec.EventType}: {ec.Count}");
    }

    // Find top users by event count
    var topUsers = await context.Events
        .GroupBy(e => e.UserId)
        .Select(g => new { UserId = g.Key, Count = g.Count() })
        .OrderByDescending(x => x.Count)
        .Take(5)
        .ToListAsync();

    Console.WriteLine("\n  Top 5 users by event count:");
    foreach (var user in topUsers)
    {
        Console.WriteLine($"    - User {user.UserId}: {user.Count} events");
    }
}
Console.WriteLine();

// Show cluster status
Console.WriteLine("Step 7: Cluster status...\n");
await using (var context = new DistributedDbContext(nodes[0].Item2))
{
    await context.Database.OpenConnectionAsync();
    try
    {
        await using var cmd = context.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = @"
            SELECT
                cluster,
                shard_num,
                host_name,
                is_local
            FROM system.clusters
            WHERE cluster = 'shard_cluster'
            ORDER BY shard_num";

        Console.WriteLine("  Cluster 'shard_cluster' members:");
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var shardNum = Convert.ToInt32(reader.GetValue(1));
            var hostName = reader.GetString(2);
            var isLocal = Convert.ToInt32(reader.GetValue(3)) == 1;
            var localMarker = isLocal ? " (current)" : "";
            Console.WriteLine($"    - Shard {shardNum}: {hostName}{localMarker}");
        }
    }
    finally
    {
        await context.Database.CloseConnectionAsync();
    }
}

Console.WriteLine("\nDone! Data is distributed across 3 shards.");
Console.WriteLine("\nKey differences from ClusterSample (Replication):");
Console.WriteLine("  - Replication: ALL data on ALL nodes (for redundancy/HA)");
Console.WriteLine("  - Sharding: Data SPLIT across nodes (for horizontal scaling)");
Console.WriteLine("\nTo clean up: docker compose down -v");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// The local event table - actual data storage on each shard.
/// Each shard stores a subset of events based on the sharding key.
/// </summary>
public class EventLocal
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public long UserId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Properties { get; set; } = string.Empty;
}

/// <summary>
/// The distributed event table - unified view across all shards.
/// Queries against this table are fanned out to all shards.
/// Inserts are routed to shards based on the sharding key (cityHash64(UserId)).
/// </summary>
public class Event
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public long UserId { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Properties { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class DistributedDbContext : DbContext
{
    private readonly string _connectionString;

    public DistributedDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// The distributed events table - use this for all application queries.
    /// </summary>
    public DbSet<Event> Events => Set<Event>();

    /// <summary>
    /// The local events table - used for direct shard queries (advanced).
    /// </summary>
    public DbSet<EventLocal> EventsLocal => Set<EventLocal>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse(_connectionString, o => o
            .UseCluster("shard_cluster"));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // ============================================================
        // Local Table Configuration (MergeTree)
        // ============================================================
        // This is the actual data storage table that exists on each shard.
        // Data is partitioned by month and ordered by EventTime + Id.
        modelBuilder.Entity<EventLocal>(entity =>
        {
            entity.ToTable("events_local");
            entity.HasKey(e => e.Id);

            entity.UseMergeTree(x => new { x.EventTime, x.Id });
            entity.HasPartitionByMonth(x => x.EventTime);
        });

        // ============================================================
        // Distributed Table Configuration
        // ============================================================
        // This provides a unified view across all shards.
        // - Queries are fanned out to all shards and results merged
        // - Inserts are routed based on the sharding key
        // - cityHash64(UserId) ensures events for the same user go to the same shard
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);

            // ============================================================
            // Fluent Chain API for Distributed Engine
            // ============================================================
            // UseDistributed(cluster, table) - creates a distributed table
            // WithShardingKey() - determines how data is routed to shards
            //
            // cityHash64(UserId) ensures:
            // 1. Same user's events always go to the same shard (locality)
            // 2. Even distribution across shards (good hash function)
            entity.UseDistributed("shard_cluster", "events_local")
                  .WithShardingKey("cityHash64(UserId)");
        });
    }
}
