// IdentifierDefaultsSample - Server-generated identifier columns
//
// Demonstrates five fluent helpers that wire ClickHouse identifier generators
// as DEFAULT column expressions:
//
//   HasSerialIDDefault("counter") -> generateSerialID('counter')   UInt64, Keeper-backed
//   HasUuidV4Default()            -> generateUUIDv4()              random UUID
//   HasUuidV7Default()            -> generateUUIDv7()              time-sortable UUID
//   HasUlidDefault()              -> generateULID()                26-char sortable string
//   HasSnowflakeIDDefault()       -> generateSnowflakeID()         Int64, no Keeper needed
//
// Each helper also marks the property ValueGeneratedOnAdd so EF omits the
// column from INSERT and the server populates it.

using System.Text;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using EfClass = Microsoft.EntityFrameworkCore.EF;

// generateSerialID needs ClickHouse Keeper. We bring up a single-node container
// with embedded Keeper so the sample is self-contained.
const string keeperConfig = """
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
    </clickhouse>
    """;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:25.6")
    .WithResourceMapping(
        Encoding.UTF8.GetBytes(keeperConfig),
        "/etc/clickhouse-server/config.d/keeper.xml")
    .Build();

Console.WriteLine("Starting ClickHouse container (with embedded Keeper)...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    await using var context = new OrderContext(container.GetConnectionString());

    Console.WriteLine("=== EF.CH Identifier Defaults Sample ===\n");

    Console.WriteLine("[1] Creating table with five server-generated ID columns...");
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("    Table created.\n");

    // Inserting rows without setting any ID columns: EF sends only Amount,
    // ClickHouse fills the rest from the DEFAULT expressions.
    Console.WriteLine("[2] Inserting three orders with only Amount set...");
    context.Orders.AddRange(
        new Order { Amount = 19.99m },
        new Order { Amount = 49.50m },
        new Order { Amount = 7.25m });
    await context.SaveChangesAsync();
    Console.WriteLine("    Inserted.\n");

    Console.WriteLine("[3] Reading back the server-populated IDs:");
    var orders = await context.Orders
        .OrderBy(o => o.SerialId)
        .AsNoTracking()
        .ToListAsync();

    foreach (var o in orders)
    {
        Console.WriteLine($"    Amount={o.Amount,6:C}");
        Console.WriteLine($"      SerialId    (UInt64): {o.SerialId}");
        Console.WriteLine($"      UuidV4      (UUID)  : {o.UuidV4}");
        Console.WriteLine($"      UuidV7      (UUID)  : {o.UuidV7}");
        Console.WriteLine($"      Ulid        (String): {o.Ulid}");
        Console.WriteLine($"      SnowflakeId (Int64) : {o.SnowflakeId}");
        Console.WriteLine();
    }

    // SerialId values are a monotonically increasing sequence. A second batch
    // keeps counting from where the first one stopped, because the counter lives
    // in Keeper, not in the table.
    //
    // Clear the tracker first: the first batch's rows came back with SerialId=0,
    // and the new (unsaved) entities also have SerialId=0 (the CLR default for
    // ulong). Without Clear() EF's identity map rejects the second set as a
    // duplicate key.
    context.ChangeTracker.Clear();

    Console.WriteLine("[4] Inserting two more orders — SerialId continues from where it left off...");
    context.Orders.AddRange(
        new Order { Amount = 100.00m },
        new Order { Amount = 200.00m });
    await context.SaveChangesAsync();

    var serials = await context.Orders
        .OrderBy(o => o.SerialId)
        .Select(o => o.SerialId)
        .ToListAsync();

    Console.WriteLine($"    SerialId sequence: [{string.Join(", ", serials)}]\n");

    // The scalar translator also works in SELECT — useful for generating IDs
    // inline in projections or INSERT ... SELECT.
    Console.WriteLine("[5] Calling generateSerialID from a LINQ projection...");
    var adHoc = await context.Orders
        .Select(o => new
        {
            o.Amount,
            AdHocSerial = EfClass.Functions.GenerateSerialID("adhoc_counter")
        })
        .Take(3)
        .ToListAsync();

    foreach (var row in adHoc)
    {
        Console.WriteLine($"    Amount={row.Amount,6:C}  AdHocSerial={row.AdHocSerial}");
    }
    Console.WriteLine();

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
// Entity and DbContext
// ===========================================================================

public class Order
{
    public ulong SerialId { get; set; }
    public Guid UuidV4 { get; set; }
    public Guid UuidV7 { get; set; }
    // Nullable on purpose: EF treats null as the sentinel and omits the column
    // from INSERT so ClickHouse can apply the DEFAULT. An empty string would be
    // sent as an explicit value and would overwrite the default.
    public string? Ulid { get; set; }
    public long SnowflakeId { get; set; }
    public decimal Amount { get; set; }
}

public class OrderContext(string connectionString) : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.SerialId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => x.SerialId);

            entity.Property(e => e.SerialId).HasSerialIDDefault("orders_counter");
            entity.Property(e => e.UuidV4).HasUuidV4Default();
            entity.Property(e => e.UuidV7).HasUuidV7Default();
            entity.Property(e => e.Ulid).HasUlidDefault();
            entity.Property(e => e.SnowflakeId).HasSnowflakeIDDefault();
        });
    }
}
