// KeeperMapSample - Atomic key-value storage with the KeeperMap engine
//
// Demonstrates:
// - Configuring UseKeeperMapEngine with a Keeper root path and PRIMARY KEY column
// - Bootstrapping a single-node ClickHouse with embedded Keeper for the engine
// - Upsert semantics: inserting the same key multiple times only ever leaves ONE row
// - Multi-key workflow: three distinct keys produce exactly three rows
// - A "feature flag" style use case where values are overwritten in place

using System.Text;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// KeeperMap requires ClickHouse Keeper (or ZooKeeper) to be reachable AND a
// <keeper_map_path_prefix> namespace to be configured. For a standalone demo we
// start the ClickHouse container with an embedded Keeper on the same node.
const string keeperMapConfig = """
    <clickhouse>
        <keeper_server>
            <tcp_port>9181</tcp_port>
            <server_id>1</server_id>
            <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
            <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
            <coordination_settings>
                <operation_timeout_ms>10000</operation_timeout_ms>
                <session_timeout_ms>30000</session_timeout_ms>
            </coordination_settings>
            <raft_configuration>
                <server>
                    <id>1</id>
                    <hostname>localhost</hostname>
                    <port>9234</port>
                </server>
            </raft_configuration>
        </keeper_server>
        <zookeeper>
            <node><host>localhost</host><port>9181</port></node>
        </zookeeper>
        <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
    </clickhouse>
    """;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:25.6")
    .WithResourceMapping(
        Encoding.UTF8.GetBytes(keeperMapConfig),
        "/etc/clickhouse-server/config.d/keeper_map.xml")
    .Build();

Console.WriteLine("Starting ClickHouse container (with embedded Keeper)...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("=== EF.CH KeeperMap Sample ===\n");

    await using var context = new FeatureFlagContext(connectionString);

    // Step 1: Create the table. The CREATE TABLE DDL includes:
    //   ENGINE = KeeperMap('/keeper_map_tables/feature_flags')
    //   PRIMARY KEY ("Name")
    Console.WriteLine("[1] Creating KeeperMap-backed table 'FeatureFlags'...");
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("    Table created.\n");

    // Step 2: Insert the same key three times. Because the KeeperMap engine
    // treats an INSERT with an existing PRIMARY KEY as an atomic upsert, only
    // the final value persists.
    Console.WriteLine("[2] Inserting key 'beta-search' three times with different values...");

    await context.Flags.UpsertRangeAsync([new FeatureFlag { Name = "beta-search", Enabled = false, RolloutPct = 0 }]);
    Console.WriteLine("    UPSERT  beta-search  Enabled=false  RolloutPct=0");

    await context.Flags.UpsertRangeAsync([new FeatureFlag { Name = "beta-search", Enabled = true, RolloutPct = 10 }]);
    Console.WriteLine("    UPSERT  beta-search  Enabled=true   RolloutPct=10");

    await context.Flags.UpsertRangeAsync([new FeatureFlag { Name = "beta-search", Enabled = true, RolloutPct = 100 }]);
    Console.WriteLine("    UPSERT  beta-search  Enabled=true   RolloutPct=100\n");

    var rowCount = await context.Flags.LongCountAsync();
    var betaSearch = await context.Flags.SingleAsync(f => f.Name == "beta-search");

    Console.WriteLine($"    Total rows in table : {rowCount}   <-- three inserts, one row");
    Console.WriteLine($"    Final value         : Enabled={betaSearch.Enabled}  RolloutPct={betaSearch.RolloutPct}\n");

    // Step 3: Multi-key workflow. Three distinct keys produce three rows, but
    // each individual key still obeys the one-row-per-key invariant.
    Console.WriteLine("[3] Seeding three more flags via EF Core tracking...");
    context.Flags.AddRange(
        new FeatureFlag { Name = "dark-mode",      Enabled = true,  RolloutPct = 100 },
        new FeatureFlag { Name = "new-onboarding", Enabled = false, RolloutPct = 0   },
        new FeatureFlag { Name = "ai-suggest",     Enabled = true,  RolloutPct = 25  });
    await context.SaveChangesAsync();

    var allFlags = await context.Flags.OrderBy(f => f.Name).ToListAsync();
    Console.WriteLine($"    Total rows in table : {allFlags.Count}\n");

    Console.WriteLine("    Current flag state:");
    foreach (var f in allFlags)
    {
        Console.WriteLine($"      {f.Name,-16} Enabled={f.Enabled,-5}  RolloutPct={f.RolloutPct}");
    }
    Console.WriteLine();

    // Step 4: Overwrite an existing flag. This is the canonical "update" path
    // for KeeperMap: there is no ALTER TABLE UPDATE dance, just INSERT.
    Console.WriteLine("[4] Rolling 'ai-suggest' from 25% -> 75% via UpsertRangeAsync on the same key...");
    await context.Flags.UpsertRangeAsync(
        [new FeatureFlag { Name = "ai-suggest", Enabled = true, RolloutPct = 75 }]);

    var aiSuggest = await context.Flags.AsNoTracking().SingleAsync(f => f.Name == "ai-suggest");
    var postRollCount = await context.Flags.LongCountAsync();
    Console.WriteLine($"    ai-suggest now Enabled={aiSuggest.Enabled}  RolloutPct={aiSuggest.RolloutPct}");
    Console.WriteLine($"    Total rows still    : {postRollCount}   <-- upsert, not append\n");

    await context.Database.EnsureDeletedAsync();

    Console.WriteLine("=== Done ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// ===========================================================================
// Entity and DbContext definitions
// ===========================================================================

public class FeatureFlag
{
    public string Name { get; set; } = string.Empty;
    public bool Enabled { get; set; }
    public int RolloutPct { get; set; }
}

public class FeatureFlagContext(string connectionString) : DbContext
{
    public DbSet<FeatureFlag> Flags => Set<FeatureFlag>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FeatureFlag>(entity =>
        {
            entity.ToTable("FeatureFlags");
            entity.HasKey(e => e.Name);

            // Backed by ClickHouse Keeper under /keeper_map_tables/feature_flags.
            // The PRIMARY KEY column is "Name" -- inserting an existing Name
            // atomically replaces the row.
            entity.UseKeeperMapEngine(
                rootPath: "/keeper_map_tables/feature_flags",
                primaryKey: x => x.Name);
        });
    }
}
