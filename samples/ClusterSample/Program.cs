// -----------------------------------------------------------------
// ClusterSample - Multi-Node ClickHouse Cluster with EF.CH
// -----------------------------------------------------------------
// Demonstrates:
//   1. 3-node cluster setup via docker-compose
//   2. Replicated engine (UseReplicatedMergeTree) with cluster + replication
//   3. ON CLUSTER DDL (UseCluster for distributed table creation)
//   4. Connection routing (read/write endpoint splitting)
//   5. Table groups (AddTableGroup for logical grouping)
// -----------------------------------------------------------------
// Prerequisites:
//   docker compose up -d   (from this sample directory)
// -----------------------------------------------------------------

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

var connectionString = "Host=localhost;Port=8123;Database=default";

Console.WriteLine("=== Cluster Sample ===\n");
Console.WriteLine("This sample requires docker compose up -d to be running.");
Console.WriteLine("See docker-compose.yml for the 3-node cluster configuration.\n");

try
{
    await DemoClusterSetup(connectionString);
    await DemoReplicatedEngine(connectionString);
    await DemoOnClusterDdl(connectionString);
    DemoConnectionRouting();
    DemoTableGroups();
}
catch (Exception ex)
{
    Console.WriteLine($"\nError: {ex.Message}");
    Console.WriteLine("Make sure docker compose services are running:");
    Console.WriteLine("  cd samples/ClusterSample && docker compose up -d");
}

// -----------------------------------------------------------------
// Demo 1: Cluster Setup Verification
// -----------------------------------------------------------------
static async Task DemoClusterSetup(string connectionString)
{
    Console.WriteLine("=== 1. Cluster Setup Verification ===\n");

    await using var context = new ClusterDemoContext(connectionString);

    // Query system.clusters to verify the cluster is configured
    var clusterInfo = await context.Database
        .SqlQueryRaw<ClusterNode>("""
            SELECT
                cluster AS Cluster,
                shard_num AS ShardNum,
                replica_num AS ReplicaNum,
                host_name AS HostName
            FROM system.clusters
            WHERE cluster = 'sample_cluster'
            ORDER BY shard_num, replica_num
            """)
        .ToListAsync();

    Console.WriteLine($"Cluster 'sample_cluster' has {clusterInfo.Count} nodes:");
    foreach (var node in clusterInfo)
    {
        Console.WriteLine($"  Shard {node.ShardNum}, Replica {node.ReplicaNum}: {node.HostName}");
    }
    Console.WriteLine();
}

// -----------------------------------------------------------------
// Demo 2: Replicated Engine Configuration
// -----------------------------------------------------------------
static async Task DemoReplicatedEngine(string connectionString)
{
    Console.WriteLine("=== 2. Replicated Engine ===\n");

    await using var context = new ClusterDemoContext(connectionString);

    Console.WriteLine("UseReplicatedMergeTree creates tables that replicate across cluster nodes.");
    Console.WriteLine("Configuration in OnModelCreating:\n");
    Console.WriteLine("""
      entity.UseReplicatedMergeTree<Event>(x => new { x.EventDate, x.EventId })
          .WithCluster("sample_cluster")
          .WithReplication(
              "/clickhouse/{database}/{table}",
              "{replica}");
    """);

    Console.WriteLine("\nThis generates DDL like:");
    Console.WriteLine("""
      CREATE TABLE events ON CLUSTER sample_cluster (
          ...
      ) ENGINE = ReplicatedMergeTree(
          '/clickhouse/{database}/{table}',
          '{replica}'
      ) ORDER BY (EventDate, EventId)
    """);

    // Create the table on all cluster nodes via ON CLUSTER
    await context.Database.ExecuteSqlRawAsync("""
        CREATE TABLE IF NOT EXISTS events ON CLUSTER sample_cluster (
            EventId UInt64,
            EventDate DateTime64(3),
            EventType String,
            UserId UInt64,
            Payload String
        ) ENGINE = ReplicatedMergeTree(
            '/clickhouse/{database}/events',
            '{replica}'
        ) ORDER BY (EventDate, EventId)
        """);

    Console.WriteLine("\nTable 'events' created on all 3 cluster nodes.");

    // Insert data on node 1 using BulkInsertAsync
    await context.BulkInsertAsync(new List<Event>
    {
        new() { EventId = 1, EventDate = new DateTime(2024, 1, 15, 10, 0, 0), EventType = "page_view", UserId = 100, Payload = """{"page": "/home"}""" },
        new() { EventId = 2, EventDate = new DateTime(2024, 1, 15, 10, 5, 0), EventType = "click", UserId = 100, Payload = """{"button": "signup"}""" },
        new() { EventId = 3, EventDate = new DateTime(2024, 1, 15, 10, 10, 0), EventType = "page_view", UserId = 200, Payload = """{"page": "/pricing"}""" },
    });

    Console.WriteLine("Inserted 3 events on node 1.");

    // Give replication a moment to propagate
    await Task.Delay(1000);

    // Query the data
    var events = await context.Events.OrderBy(e => e.EventId).ToListAsync();
    Console.WriteLine($"Queried {events.Count} events from node 1:");
    foreach (var evt in events)
    {
        Console.WriteLine($"  [{evt.EventId}] {evt.EventType} by user {evt.UserId} at {evt.EventDate:HH:mm:ss}");
    }
}

// -----------------------------------------------------------------
// Demo 3: ON CLUSTER DDL
// -----------------------------------------------------------------
static async Task DemoOnClusterDdl(string connectionString)
{
    Console.WriteLine("\n=== 3. ON CLUSTER DDL ===\n");

    await using var context = new ClusterDemoContext(connectionString);

    Console.WriteLine("When UseCluster is configured, DDL statements include ON CLUSTER.");
    Console.WriteLine("This causes CREATE TABLE, ALTER TABLE, and DROP TABLE to execute");
    Console.WriteLine("on all nodes in the cluster simultaneously.\n");

    Console.WriteLine("Entity-level configuration:");
    Console.WriteLine("""
      entity.UseCluster("sample_cluster");
    """);

    Console.WriteLine("\nContext-level default (applies to all tables):");
    Console.WriteLine("""
      options.UseClickHouse(connectionString, o => o
          .UseCluster("sample_cluster"));
    """);

    // Verify the table exists on another node
    var options2 = new DbContextOptionsBuilder<ClusterDemoContext>()
        .UseClickHouse("Host=localhost;Port=8124;Database=default")
        .Options;

    await using var node2Context = new ClusterDemoContext(options2);

    var eventsOnNode2 = await node2Context.Events.CountAsync();
    Console.WriteLine($"\nEvents visible on node 2 (port 8124): {eventsOnNode2}");
    Console.WriteLine("All nodes see the same data via replication.");
}

// -----------------------------------------------------------------
// Demo 4: Connection Routing
// -----------------------------------------------------------------
static void DemoConnectionRouting()
{
    Console.WriteLine("\n=== 4. Connection Routing ===\n");

    Console.WriteLine("UseConnectionRouting splits reads and writes to different endpoints.");
    Console.WriteLine("SELECT queries go to read endpoints; INSERT/UPDATE/DELETE go to write endpoints.\n");

    Console.WriteLine("Configuration:");
    Console.WriteLine("""
      options.UseClickHouse("Host=localhost;Port=8123", o => o
          .UseConnectionRouting()
          .AddConnection("Primary", conn => conn
              .Database("production")
              .WriteEndpoint("dc1-clickhouse:8123")
              .ReadEndpoints("dc2-clickhouse:8123", "dc1-clickhouse:8123")));
    """);

    Console.WriteLine("\nThis sample uses a 3-shard cluster without replication.");
    Console.WriteLine("In a production replication setup, you would configure:");
    Console.WriteLine("  - Write endpoint: the primary/leader node");
    Console.WriteLine("  - Read endpoints: replica nodes (for read scaling)");
}

// -----------------------------------------------------------------
// Demo 5: Table Groups
// -----------------------------------------------------------------
static void DemoTableGroups()
{
    Console.WriteLine("\n=== 5. Table Groups ===\n");

    Console.WriteLine("Table groups logically organize tables with shared cluster and replication settings.\n");

    Console.WriteLine("Configuration:");
    Console.WriteLine("""
      options.UseClickHouse(connectionString, o => o
          .AddTableGroup("Core", group => group
              .UseCluster("sample_cluster")
              .Replicated())
          .AddTableGroup("LocalCache", group => group
              .NoCluster()
              .NotReplicated()));
    """);

    Console.WriteLine("\nThen assign tables to groups:");
    Console.WriteLine("""
      // In OnModelCreating:
      entity.UseReplicatedMergeTree<Event>(x => x.EventId)
          .WithTableGroup("Core");

      // Local-only tables skip ON CLUSTER:
      entity.IsLocalOnly();
    """);

    Console.WriteLine("\nTable group benefits:");
    Console.WriteLine("  - Consistent cluster/replication settings across related tables");
    Console.WriteLine("  - Local-only tables for caching or staging (skip ON CLUSTER)");
    Console.WriteLine("  - Mix replicated and non-replicated tables in one context");
}

// -----------------------------------------------------------------
// Entities
// -----------------------------------------------------------------

public class Event
{
    public ulong EventId { get; set; }
    public DateTime EventDate { get; set; }
    public string EventType { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public string Payload { get; set; } = string.Empty;
}

public class ClusterNode
{
    public string Cluster { get; set; } = string.Empty;
    public uint ShardNum { get; set; }
    public uint ReplicaNum { get; set; }
    public string HostName { get; set; } = string.Empty;
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class ClusterDemoContext : DbContext
{
    private readonly string? _connectionString;

    public ClusterDemoContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    public ClusterDemoContext(DbContextOptions<ClusterDemoContext> options)
        : base(options) { }

    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (_connectionString == null)
            return; // Already configured via DbContextOptions constructor

        optionsBuilder.UseClickHouse(_connectionString, o => o
            // Set the default cluster for all DDL operations
            .UseCluster("sample_cluster")
            // Enable read/write routing
            .UseConnectionRouting()
            // Define table groups for logical organization
            .AddTableGroup("Core", group => group
                .UseCluster("sample_cluster")
                .Replicated())
            .AddTableGroup("LocalCache", group => group
                .NoCluster()
                .NotReplicated()));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("events");
            // In a real application you would use:
            // entity.UseReplicatedMergeTree<Event>(x => new { x.EventDate, x.EventId })
            //     .WithCluster("sample_cluster")
            //     .WithReplication("/clickhouse/{database}/{table}", "{replica}");
            //
            // For this sample, the table is created via raw SQL to work with
            // the docker-compose cluster without ZooKeeper/Keeper.
        });
    }
}
