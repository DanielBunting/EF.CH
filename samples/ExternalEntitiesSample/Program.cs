// -----------------------------------------------------------------
// ExternalEntitiesSample - External Data Sources with EF.CH
// -----------------------------------------------------------------
// Demonstrates:
//   1. PostgreSQL external entity (ExternalPostgresEntity)
//   2. Redis external entity (ExternalRedisEntity)
//   3. JOIN between external entities and native ClickHouse tables
// -----------------------------------------------------------------
// Prerequisites:
//   docker compose up -d   (from this sample directory)
// -----------------------------------------------------------------

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// Environment variables for connection configuration.
// In production, these would come from secrets management.
// For this sample, they match the docker-compose.yml values.
Environment.SetEnvironmentVariable("PG_HOSTPORT", "localhost:5432");
Environment.SetEnvironmentVariable("PG_DATABASE", "sampledb");
Environment.SetEnvironmentVariable("PG_USER", "postgres");
Environment.SetEnvironmentVariable("PG_PASSWORD", "postgres");
Environment.SetEnvironmentVariable("REDIS_HOST", "localhost:6379");

var clickHouseConnectionString = "Host=localhost;Port=8123;Database=default";

Console.WriteLine("=== External Entities Sample ===\n");
Console.WriteLine("This sample requires docker compose up -d to be running.");
Console.WriteLine("See docker-compose.yml for ClickHouse, PostgreSQL, and Redis services.\n");

try
{
    await DemoPostgresExternalEntity(clickHouseConnectionString);
    await DemoRedisExternalEntity(clickHouseConnectionString);
    await DemoJoinWithNativeTable(clickHouseConnectionString);
}
catch (Exception ex)
{
    Console.WriteLine($"\nError: {ex.Message}");
    Console.WriteLine("Make sure docker compose services are running:");
    Console.WriteLine("  cd samples/ExternalEntitiesSample && docker compose up -d");
}

// -----------------------------------------------------------------
// Demo 1: PostgreSQL External Entity
// -----------------------------------------------------------------
static async Task DemoPostgresExternalEntity(string connectionString)
{
    Console.WriteLine("=== 1. PostgreSQL External Entity ===\n");

    await using var context = new ExternalEntitiesContext(connectionString);

    Console.WriteLine("Querying PostgreSQL 'customers' table directly from ClickHouse...");
    Console.WriteLine("No data import needed -- ClickHouse queries Postgres via postgresql() table function.\n");

    // The query executes against PostgreSQL through ClickHouse's postgresql() table function.
    // EF.CH rewrites the SQL to use the table function instead of a local table.
    var customers = await context.Customers.ToListAsync();

    Console.WriteLine($"Found {customers.Count} customers in PostgreSQL:");
    foreach (var customer in customers)
    {
        Console.WriteLine($"  [{customer.Id}] {customer.Name} ({customer.Email}) - {customer.Country}");
    }

    // Filtering works as expected -- the predicate is pushed to the query
    Console.WriteLine("\nFiltering customers from Germany:");
    var germanCustomers = await context.Customers
        .Where(c => c.Country == "Germany")
        .ToListAsync();

    foreach (var customer in germanCustomers)
    {
        Console.WriteLine($"  [{customer.Id}] {customer.Name} ({customer.Email})");
    }
}

// -----------------------------------------------------------------
// Demo 2: Redis External Entity
// -----------------------------------------------------------------
static async Task DemoRedisExternalEntity(string connectionString)
{
    Console.WriteLine("\n=== 2. Redis External Entity ===\n");

    await using var context = new ExternalEntitiesContext(connectionString);

    Console.WriteLine("Redis external entities use the redis() table function.");
    Console.WriteLine("Each entity maps to Redis key-value pairs.\n");

    Console.WriteLine("Configuration in OnModelCreating:");
    Console.WriteLine("""
      modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
          .KeyColumn(x => x.SessionId)
          .Connection(c => c
              .HostPort(env: "REDIS_HOST")
              .DbIndex(0))
          .ReadOnly());
    """);

    Console.WriteLine("\nNote: Redis external entity queries require data to be pre-populated");
    Console.WriteLine("in Redis using the expected key format. The redis() table function");
    Console.WriteLine("reads keys matching the configured structure.");
}

// -----------------------------------------------------------------
// Demo 3: JOIN External Entity with Native ClickHouse Table
// -----------------------------------------------------------------
static async Task DemoJoinWithNativeTable(string connectionString)
{
    Console.WriteLine("\n=== 3. JOIN External Entity with Native Table ===\n");

    await using var context = new ExternalEntitiesContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    // Seed the native ClickHouse orders table
    await context.BulkInsertAsync(new List<Order>
    {
        new() { Id = 1, CustomerId = 1, Amount = 150.00m, OrderDate = new DateTime(2024, 1, 15) },
        new() { Id = 2, CustomerId = 1, Amount = 200.00m, OrderDate = new DateTime(2024, 2, 20) },
        new() { Id = 3, CustomerId = 2, Amount = 350.00m, OrderDate = new DateTime(2024, 1, 10) },
        new() { Id = 4, CustomerId = 3, Amount = 75.50m, OrderDate = new DateTime(2024, 3, 5) },
        new() { Id = 5, CustomerId = 4, Amount = 420.00m, OrderDate = new DateTime(2024, 2, 28) },
    });

    Console.WriteLine("Native ClickHouse 'orders' table seeded with 5 orders.");
    Console.WriteLine("Joining with PostgreSQL 'customers' external entity...\n");

    // Join native ClickHouse table with external PostgreSQL entity.
    // ClickHouse handles the cross-engine join transparently.
    var enrichedOrders = await context.Orders
        .Join(
            context.Customers,
            o => o.CustomerId,
            c => c.Id,
            (o, c) => new
            {
                OrderId = o.Id,
                CustomerName = c.Name,
                CustomerCountry = c.Country,
                o.Amount,
                o.OrderDate
            })
        .OrderBy(x => x.OrderId)
        .ToListAsync();

    Console.WriteLine("Enriched orders (ClickHouse orders + PostgreSQL customers):");
    foreach (var order in enrichedOrders)
    {
        Console.WriteLine($"  Order #{order.OrderId}: {order.CustomerName} ({order.CustomerCountry}) " +
                          $"- ${order.Amount:F2} on {order.OrderDate:yyyy-MM-dd}");
    }

    // Aggregation across the join
    Console.WriteLine("\nRevenue by country (aggregated across engines):");
    var revenueByCountry = await context.Orders
        .Join(
            context.Customers,
            o => o.CustomerId,
            c => c.Id,
            (o, c) => new { c.Country, o.Amount })
        .GroupBy(x => x.Country)
        .Select(g => new
        {
            Country = g.Key,
            TotalRevenue = g.Sum(x => x.Amount),
            OrderCount = g.Count()
        })
        .OrderByDescending(x => x.TotalRevenue)
        .ToListAsync();

    foreach (var row in revenueByCountry)
    {
        Console.WriteLine($"  {row.Country}: ${row.TotalRevenue:F2} ({row.OrderCount} orders)");
    }
}

// -----------------------------------------------------------------
// Entities
// -----------------------------------------------------------------

// External entity: lives in PostgreSQL, queried via postgresql() table function
public class Customer
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

// External entity: lives in Redis, queried via redis() table function
public class SessionCache
{
    public string SessionId { get; set; } = string.Empty;
    public int UserId { get; set; }
    public string UserAgent { get; set; } = string.Empty;
}

// Native ClickHouse table
public class Order
{
    public ulong Id { get; set; }
    public int CustomerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class ExternalEntitiesContext(string connectionString) : DbContext
{
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<SessionCache> Sessions => Set<SessionCache>();
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // External PostgreSQL entity.
        // No ClickHouse table is created -- queries go directly to PostgreSQL.
        // Connection credentials are resolved from environment variables at runtime.
        modelBuilder.ExternalPostgresEntity<Customer>(ext => ext
            .FromTable("customers", schema: "public")
            .Connection(c => c
                .HostPort(env: "PG_HOSTPORT")
                .Database(env: "PG_DATABASE")
                .Credentials("PG_USER", "PG_PASSWORD"))
            .ReadOnly());

        // External Redis entity.
        // Queries Redis via the redis() table function.
        // KeyColumn identifies which property is the Redis key.
        modelBuilder.ExternalRedisEntity<SessionCache>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c
                .HostPort(env: "REDIS_HOST")
                .DbIndex(0))
            .ReadOnly());

        // Native ClickHouse table for orders.
        // This is a regular MergeTree table stored in ClickHouse.
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
        });
    }
}
