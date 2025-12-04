using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

namespace KeylessSample;

/// <summary>
/// Demonstrates keyless entities by default for ClickHouse.
/// All entities are keyless unless explicitly configured with HasKey().
/// </summary>
public class KeylessDbContext : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();
    public DbSet<ApiRequest> ApiRequests => Set<ApiRequest>();
    public DbSet<User> Users => Set<User>();  // This one has a key

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(
            "Host=localhost;Database=keyless_sample",
            o => o.UseKeylessEntitiesByDefault());  // Enable keyless by default
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // PageView - keyless (uses default from convention)
        // Just specify ORDER BY columns
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.ToTable("PageViews");
            entity.UseMergeTree("Timestamp", "PageUrl");
            entity.HasPartitionBy("toYYYYMMDD(\"Timestamp\")");
        });

        // ApiRequest - also keyless
        modelBuilder.Entity<ApiRequest>(entity =>
        {
            entity.ToTable("ApiRequests");
            entity.UseMergeTree("Timestamp", "Endpoint");
            entity.HasPartitionBy("toYYYYMM(\"Timestamp\")");
            entity.HasTtl("\"Timestamp\" + INTERVAL 90 DAY");
        });

        // User - explicit HasKey() overrides the keyless default
        // This entity can be tracked and updated
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("Users");
            entity.HasKey(e => e.Id);  // Override keyless default
            entity.UseReplacingMergeTree("UpdatedAt", "Id");
        });
    }
}

/// <summary>
/// Keyless append-only page view tracking.
/// </summary>
public class PageView
{
    public DateTime Timestamp { get; set; }
    public string PageUrl { get; set; } = string.Empty;
    public string? UserId { get; set; }
    public string? Referrer { get; set; }
    public string? UserAgent { get; set; }
    public int DurationMs { get; set; }
}

/// <summary>
/// Keyless API request logging with TTL.
/// </summary>
public class ApiRequest
{
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public string Method { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public int ResponseTimeMs { get; set; }
    public string? RequestBody { get; set; }
}

/// <summary>
/// Entity with explicit key - supports change tracking.
/// </summary>
public class User
{
    public Guid Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
}

class Program
{
    static void Main()
    {
        Console.WriteLine("Keyless Entities Sample for ClickHouse");
        Console.WriteLine("======================================");
        Console.WriteLine();
        Console.WriteLine("This sample demonstrates UseKeylessEntitiesByDefault()");
        Console.WriteLine();
        Console.WriteLine("Run 'dotnet ef migrations add InitialCreate' to see the generated SQL.");
    }
}
