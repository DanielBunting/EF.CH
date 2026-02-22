using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

namespace MigrationSample;

// ---------------------------------------------------------------------------
// Entities
// ---------------------------------------------------------------------------

public class Product
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int StockQuantity { get; set; }
    public DateTime CreatedAt { get; set; }
}

// ---------------------------------------------------------------------------
// DbContext
// ---------------------------------------------------------------------------

public class SampleDbContext : DbContext
{
    public DbSet<Product> Products => Set<Product>();

    private readonly string _connectionString;

    public SampleDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Parameterless constructor required by EF Core design-time tools (Add-Migration, etc.).
    /// When used by the design-time factory, the connection string comes from
    /// IDesignTimeDbContextFactory or falls back to a default.
    /// </summary>
    public SampleDbContext()
    {
        _connectionString = "Host=localhost;Port=8123;Database=default";
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
            optionsBuilder.UseClickHouse(_connectionString);
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasKey(e => e.Id);

            // MergeTree engine ordered by Category then CreatedAt for efficient
            // category-based range queries
            entity.UseMergeTree(x => new { x.Category, x.CreatedAt });

            // Monthly partitioning for time-based data management and TTL
            entity.HasPartitionByMonth<Product, DateTime>(x => x.CreatedAt);
        });
    }
}
