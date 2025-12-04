using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Enum Type Sample
// ============================================================
// Demonstrates using C# enums with ClickHouse Enum8/Enum16.
// EF.CH automatically selects the right enum size based on
// your enum values.
// ============================================================

Console.WriteLine("Enum Type Sample");
Console.WriteLine("================\n");

await using var context = new OrderDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert orders with enum properties
Console.WriteLine("Inserting orders with various statuses and priorities...\n");

var orders = new[]
{
    new Order
    {
        Id = Guid.NewGuid(),
        OrderNumber = "ORD-001",
        OrderDate = DateTime.UtcNow.AddDays(-5),
        Status = OrderStatus.Delivered,
        Priority = Priority.Normal,
        Total = 149.99m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderNumber = "ORD-002",
        OrderDate = DateTime.UtcNow.AddDays(-3),
        Status = OrderStatus.Shipped,
        Priority = Priority.High,
        Total = 299.99m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderNumber = "ORD-003",
        OrderDate = DateTime.UtcNow.AddDays(-2),
        Status = OrderStatus.Processing,
        Priority = Priority.Normal,
        Total = 79.99m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderNumber = "ORD-004",
        OrderDate = DateTime.UtcNow.AddDays(-1),
        Status = OrderStatus.Pending,
        Priority = Priority.Critical,
        Total = 999.99m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderNumber = "ORD-005",
        OrderDate = DateTime.UtcNow,
        Status = OrderStatus.Cancelled,
        Priority = Priority.Low,
        Total = 49.99m
    },
    new Order
    {
        Id = Guid.NewGuid(),
        OrderNumber = "ORD-006",
        OrderDate = DateTime.UtcNow,
        Status = OrderStatus.Pending,
        Priority = Priority.High,
        Total = 199.99m
    }
};

context.Orders.AddRange(orders);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {orders.Length} orders.\n");

// Query: Filter by single enum value
Console.WriteLine("--- Pending orders ---");
var pending = await context.Orders
    .Where(o => o.Status == OrderStatus.Pending)
    .Select(o => new { o.OrderNumber, o.Priority, o.Total })
    .ToListAsync();

foreach (var order in pending)
    Console.WriteLine($"  {order.OrderNumber}: {order.Priority} priority, ${order.Total}");

// Query: Filter by multiple enum values
Console.WriteLine("\n--- Active orders (not delivered or cancelled) ---");
var active = await context.Orders
    .Where(o => o.Status != OrderStatus.Delivered && o.Status != OrderStatus.Cancelled)
    .Select(o => new { o.OrderNumber, o.Status })
    .ToListAsync();

foreach (var order in active)
    Console.WriteLine($"  {order.OrderNumber}: {order.Status}");

// Query: Enum comparison (enum values are comparable)
Console.WriteLine("\n--- High priority or above ---");
var urgent = await context.Orders
    .Where(o => o.Priority >= Priority.High)
    .Select(o => new { o.OrderNumber, o.Priority, o.Status })
    .ToListAsync();

foreach (var order in urgent)
    Console.WriteLine($"  {order.OrderNumber}: {order.Priority} - {order.Status}");

// Query: Group by enum
Console.WriteLine("\n--- Order count by status ---");
var statusCounts = await context.Orders
    .GroupBy(o => o.Status)
    .Select(g => new { Status = g.Key, Count = g.Count() })
    .ToListAsync();

foreach (var group in statusCounts.OrderBy(g => g.Status))
    Console.WriteLine($"  {group.Status}: {group.Count} order(s)");

// Query: Group by enum with aggregation
Console.WriteLine("\n--- Revenue by priority ---");
var priorityRevenue = await context.Orders
    .Where(o => o.Status != OrderStatus.Cancelled)
    .GroupBy(o => o.Priority)
    .Select(g => new
    {
        Priority = g.Key,
        OrderCount = g.Count(),
        TotalRevenue = g.Sum(o => o.Total)
    })
    .ToListAsync();

foreach (var group in priorityRevenue.OrderByDescending(g => g.Priority))
    Console.WriteLine($"  {group.Priority}: {group.OrderCount} order(s), ${group.TotalRevenue}");

// Query: Combine enum filters
Console.WriteLine("\n--- Critical pending orders (need immediate attention) ---");
var criticalPending = await context.Orders
    .Where(o => o.Status == OrderStatus.Pending)
    .Where(o => o.Priority >= Priority.High)
    .OrderByDescending(o => o.Priority)
    .ThenByDescending(o => o.Total)
    .ToListAsync();

if (criticalPending.Any())
{
    foreach (var order in criticalPending)
        Console.WriteLine($"  {order.OrderNumber}: {order.Priority}, ${order.Total}");
}
else
{
    Console.WriteLine("  (none)");
}

Console.WriteLine("\nDone!");

// ============================================================
// Enum Definitions
// ============================================================

/// <summary>
/// Order processing status.
/// Maps to Enum8 in ClickHouse (values fit in Int8).
/// </summary>
public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}

/// <summary>
/// Order priority level.
/// Maps to Enum8 in ClickHouse (values fit in Int8).
/// </summary>
public enum Priority
{
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3
}

// ============================================================
// Entity Definition
// ============================================================
public class Order
{
    public Guid Id { get; set; }
    public string OrderNumber { get; set; } = string.Empty;
    public DateTime OrderDate { get; set; }
    public OrderStatus Status { get; set; }  // Enum8
    public Priority Priority { get; set; }   // Enum8
    public decimal Total { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================
public class OrderDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=enum_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
            // Enum properties work automatically
        });
    }
}
