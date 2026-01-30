using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// EF.CH Cluster Sample with Replication
// ============================================================
// This sample demonstrates:
// 1. Connecting to a 3-node ClickHouse cluster
// 2. Using ReplicatedMergeTree with the fluent API
// 3. Verifying data replication across nodes
//
// Prerequisites:
//   docker compose up -d
//   Wait for cluster to be healthy (about 30 seconds)
//
// Architecture:
// ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
// │ clickhouse1 │◄──►│ clickhouse2 │◄──►│ clickhouse3 │
// │  :8123      │    │  :8124      │    │  :8125      │
// │  replica 1  │    │  replica 2  │    │  replica 3  │
// └─────────────┘    └─────────────┘    └─────────────┘
//        │                 │                   │
//        └────────────────────────────────────┘
//                  Data replicated via Keeper
// ============================================================

Console.WriteLine("EF.CH Cluster Sample with Replication");
Console.WriteLine("======================================\n");

// Connection strings for all 3 nodes
var nodes = new[]
{
    ("Node 1", "Host=localhost;Port=8123;Database=cluster_demo"),
    ("Node 2", "Host=localhost;Port=8124;Database=cluster_demo"),
    ("Node 3", "Host=localhost;Port=8125;Database=cluster_demo")
};

// Use Node 1 for writes
Console.WriteLine("Step 1: Creating database and tables on Node 1...");
await using (var context = new ClusterDbContext(nodes[0].Item2))
{
    await context.Database.EnsureCreatedAsync();
    Console.WriteLine("  Database and tables created.\n");
}

// Insert data through Node 1
Console.WriteLine("Step 2: Inserting sample orders through Node 1...");
await using (var context = new ClusterDbContext(nodes[0].Item2))
{
    var orders = new[]
    {
        new Order
        {
            Id = Guid.NewGuid(),
            OrderDate = DateTime.UtcNow.AddDays(-2),
            CustomerId = "CUST-001",
            ProductName = "Widget Pro",
            Quantity = 5,
            TotalAmount = 249.95m,
            Version = 1
        },
        new Order
        {
            Id = Guid.NewGuid(),
            OrderDate = DateTime.UtcNow.AddDays(-1),
            CustomerId = "CUST-002",
            ProductName = "Gadget Max",
            Quantity = 2,
            TotalAmount = 599.98m,
            Version = 1
        },
        new Order
        {
            Id = Guid.NewGuid(),
            OrderDate = DateTime.UtcNow,
            CustomerId = "CUST-001",
            ProductName = "Gizmo Basic",
            Quantity = 10,
            TotalAmount = 99.90m,
            Version = 1
        }
    };

    context.Orders.AddRange(orders);
    await context.SaveChangesAsync();
    Console.WriteLine($"  Inserted {orders.Length} orders.\n");
}

// Wait for replication
Console.WriteLine("Step 3: Waiting for replication (2 seconds)...");
await Task.Delay(2000);
Console.WriteLine("  Replication sync complete.\n");

// Verify data on all nodes
Console.WriteLine("Step 4: Verifying data replication across all nodes...\n");
foreach (var (name, connectionString) in nodes)
{
    await using var context = new ClusterDbContext(connectionString);

    var orderCount = await context.Orders.CountAsync();
    var totalRevenue = await context.Orders.SumAsync(o => o.TotalAmount);

    // Get host name using ADO.NET directly (EF Core's SqlQueryRaw adds wrapping that ClickHouse doesn't like)
    string? replicaInfo = null;
    await context.Database.OpenConnectionAsync();
    try
    {
        await using var cmd = context.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = "SELECT hostName()";
        replicaInfo = (await cmd.ExecuteScalarAsync())?.ToString();
    }
    finally
    {
        await context.Database.CloseConnectionAsync();
    }

    Console.WriteLine($"  {name} (host: {replicaInfo}):");
    Console.WriteLine($"    - Order count: {orderCount}");
    Console.WriteLine($"    - Total revenue: ${totalRevenue:F2}");
    Console.WriteLine();
}

// Query from Node 2 (demonstrating read from any replica)
Console.WriteLine("Step 5: Querying recent orders from Node 2...\n");
await using (var context = new ClusterDbContext(nodes[1].Item2))
{
    var recentOrders = await context.Orders
        .Where(o => o.OrderDate >= DateTime.UtcNow.AddDays(-7))
        .OrderByDescending(o => o.OrderDate)
        .ToListAsync();

    Console.WriteLine("  Recent orders:");
    foreach (var order in recentOrders)
    {
        Console.WriteLine($"    [{order.OrderDate:yyyy-MM-dd}] {order.ProductName} x{order.Quantity} = ${order.TotalAmount:F2}");
    }
    Console.WriteLine();
}

// Show cluster status
Console.WriteLine("Step 6: Cluster status...\n");
await using (var context = new ClusterDbContext(nodes[0].Item2))
{
    await context.Database.OpenConnectionAsync();
    try
    {
        await using var cmd = context.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = @"
            SELECT
                cluster,
                host_name,
                host_address,
                is_local
            FROM system.clusters
            WHERE cluster = 'sample_cluster'
            ORDER BY host_name";

        Console.WriteLine("  Cluster 'sample_cluster' members:");
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var hostName = reader.GetString(1);
            var hostAddress = reader.GetString(2);
            var isLocal = reader.GetByte(3) == 1;
            var localMarker = isLocal ? " (current)" : "";
            Console.WriteLine($"    - {hostName} ({hostAddress}){localMarker}");
        }
    }
    finally
    {
        await context.Database.CloseConnectionAsync();
    }
}

Console.WriteLine("\nDone! Data is replicated across all 3 nodes.");
Console.WriteLine("\nTo clean up: docker compose down -v");

// ============================================================
// Entity Definitions
// ============================================================

public class Order
{
    public Guid Id { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal TotalAmount { get; set; }
    public long Version { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class ClusterDbContext : DbContext
{
    private readonly string _connectionString;

    public ClusterDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse(_connectionString, o => o
            .UseCluster("sample_cluster"));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);

            // ============================================================
            // Fluent Chain API for Replicated Engines
            // ============================================================
            // UseReplicatedReplacingMergeTree() returns a ReplicatedEngineBuilder
            // that allows fluent chaining of cluster and replication settings
            entity.UseReplicatedReplacingMergeTree(x => x.Version, x => new { x.OrderDate, x.Id })
                  .WithCluster("sample_cluster")
                  .WithReplication("/clickhouse/tables/{database}/{table}");

            // Partition by month for efficient queries
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
