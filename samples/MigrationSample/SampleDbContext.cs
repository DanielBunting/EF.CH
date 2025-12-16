using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

namespace MigrationSample;

public class SampleDbContext : DbContext
{
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<EventLog> EventLogs => Set<EventLog>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse("Host=localhost;Database=migration_sample;User=default");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(entity =>
        {
            entity.ToTable("Products");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);  // Expression-based syntax
        });

        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });  // Multiple columns with anonymous type
            entity.HasPartitionByMonth(x => x.OrderDate);  // Type-safe partition by month
        });

        // Keyless entity - append-only event log with no primary key
        // ORDER BY is on timestamp columns, not a unique identifier
        modelBuilder.Entity<EventLog>(entity =>
        {
            entity.ToTable("EventLogs");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.Timestamp, x.EventType });
            entity.HasPartitionByDay(x => x.Timestamp);  // Type-safe partition by day
        });
    }
}

public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class Order
{
    public Guid Id { get; set; }
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public DateTime OrderDate { get; set; }
    public string CustomerName { get; set; } = string.Empty;
}

/// <summary>
/// Keyless entity for append-only event logging.
/// No primary key - uses MergeTree ORDER BY for sorting.
/// </summary>
public class EventLog
{
    public DateTime Timestamp { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string? UserId { get; set; }
    public string? Metadata { get; set; }
}
