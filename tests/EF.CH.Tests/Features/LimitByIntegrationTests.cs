using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Integration tests for LIMIT BY feature using real ClickHouse database.
/// Tests verify that LIMIT BY correctly returns top N rows per group.
/// </summary>
public class LimitByIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact]
    public async Task LimitBy_SingleColumn_ReturnsTopNPerGroup()
    {
        await using var context = CreateContext();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert test data: 3 categories, multiple items each with different scores
        var now = DateTime.UtcNow;
        context.Events.AddRange(
            // Category A: scores 100, 90, 80, 70, 60
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 100, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 90, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "EU", Score = 80, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "EU", Score = 70, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 60, CreatedAt = now },
            // Category B: scores 95, 85, 75
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 95, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "EU", Score = 85, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 75, CreatedAt = now },
            // Category C: scores 50, 40
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "C", Region = "EU", Score = 50, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "C", Region = "EU", Score = 40, CreatedAt = now }
        );
        await context.SaveChangesAsync();

        // Get top 2 per category
        // Note: We can't chain OrderBy after LimitBy due to EF Core's navigation expander limitations.
        // The results come back already ordered by the original OrderByDescending(Score)
        var results = await context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(2, e => e.Category)
            .ToListAsync();

        // Expect: A(100,90), B(95,85), C(50,40) = 6 total
        Assert.Equal(6, results.Count);

        // Verify Category A has top 2
        var categoryA = results.Where(e => e.Category == "A").ToList();
        Assert.Equal(2, categoryA.Count);
        Assert.Contains(categoryA, e => e.Score == 100);
        Assert.Contains(categoryA, e => e.Score == 90);

        // Verify Category B has top 2
        var categoryB = results.Where(e => e.Category == "B").ToList();
        Assert.Equal(2, categoryB.Count);
        Assert.Contains(categoryB, e => e.Score == 95);
        Assert.Contains(categoryB, e => e.Score == 85);

        // Verify Category C has top 2 (only 2 exist)
        var categoryC = results.Where(e => e.Category == "C").ToList();
        Assert.Equal(2, categoryC.Count);
    }

    [Fact]
    public async Task LimitBy_CompoundKey_ReturnsTopNPerCompoundGroup()
    {
        await using var context = CreateContext();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert test data
        var now = DateTime.UtcNow;
        context.Events.AddRange(
            // A-US: 100, 90, 80
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 100, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 90, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 80, CreatedAt = now },
            // A-EU: 95, 85
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "EU", Score = 95, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "EU", Score = 85, CreatedAt = now },
            // B-US: 70, 60
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 70, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 60, CreatedAt = now }
        );
        await context.SaveChangesAsync();

        // Get top 2 per category-region combination
        // Note: Additional ordering after LimitBy is not supported due to EF Core's NavigationExpandingExpressionVisitor.
        // Sort in-memory after fetching.
        var results = (await context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(2, e => new { e.Category, e.Region })
            .ToListAsync())
            .OrderBy(e => e.Category)
            .ThenBy(e => e.Region)
            .ThenByDescending(e => e.Score)
            .ToList();

        // Expect: A-EU(95,85), A-US(100,90), B-US(70,60) = 6 total
        Assert.Equal(6, results.Count);

        // Verify A-US has top 2
        var aUs = results.Where(e => e.Category == "A" && e.Region == "US").ToList();
        Assert.Equal(2, aUs.Count);
        Assert.Contains(aUs, e => e.Score == 100);
        Assert.Contains(aUs, e => e.Score == 90);

        // Verify A-EU has top 2
        var aEu = results.Where(e => e.Category == "A" && e.Region == "EU").ToList();
        Assert.Equal(2, aEu.Count);
        Assert.Contains(aEu, e => e.Score == 95);
        Assert.Contains(aEu, e => e.Score == 85);

        // Verify B-US has top 2
        var bUs = results.Where(e => e.Category == "B" && e.Region == "US").ToList();
        Assert.Equal(2, bUs.Count);
    }

    [Fact]
    public async Task LimitBy_WithOffset_SkipsRowsPerGroup()
    {
        await using var context = CreateContext();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert test data
        var now = DateTime.UtcNow;
        context.Events.AddRange(
            // Category A: scores 100, 90, 80, 70, 60
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 100, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 90, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 80, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 70, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 60, CreatedAt = now },
            // Category B: scores 95, 85, 75
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 95, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 85, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 75, CreatedAt = now }
        );
        await context.SaveChangesAsync();

        // Skip 1, take 2 per category (get 2nd and 3rd highest)
        // Note: Additional ordering after LimitBy is not supported due to EF Core's NavigationExpandingExpressionVisitor.
        // Sort in-memory after fetching.
        var results = (await context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(1, 2, e => e.Category)
            .ToListAsync())
            .OrderBy(e => e.Category)
            .ThenByDescending(e => e.Score)
            .ToList();

        // Expect: A(90,80), B(85,75) = 4 total (skipped 100 and 95)
        Assert.Equal(4, results.Count);

        // Verify Category A has 2nd and 3rd highest
        var categoryA = results.Where(e => e.Category == "A").ToList();
        Assert.Equal(2, categoryA.Count);
        Assert.Contains(categoryA, e => e.Score == 90);
        Assert.Contains(categoryA, e => e.Score == 80);
        Assert.DoesNotContain(categoryA, e => e.Score == 100); // Skipped

        // Verify Category B has 2nd and 3rd highest
        var categoryB = results.Where(e => e.Category == "B").ToList();
        Assert.Equal(2, categoryB.Count);
        Assert.Contains(categoryB, e => e.Score == 85);
        Assert.Contains(categoryB, e => e.Score == 75);
        Assert.DoesNotContain(categoryB, e => e.Score == 95); // Skipped
    }

    [Fact(Skip = "EF Core's NavigationExpandingExpressionVisitor doesn't recognize custom LimitBy method, " +
                   "so Take() after LimitBy() fails. Use LINQ-to-objects Take after ToList().")]
    public async Task LimitBy_WithGlobalTake_LimitsTotalResults()
    {
        await using var context = CreateContext();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert many categories
        var now = DateTime.UtcNow;
        for (int cat = 0; cat < 10; cat++)
        {
            for (int score = 100; score >= 80; score -= 10)
            {
                context.Events.Add(new LimitByIntegrationEvent
                {
                    Id = Guid.NewGuid(),
                    Category = $"Cat{cat}",
                    Region = "US",
                    Score = score,
                    CreatedAt = now
                });
            }
        }
        await context.SaveChangesAsync();

        // Get top 2 per category, but limit total to 6
        var results = await context.Events
            .OrderByDescending(e => e.Score)
            .LimitBy(2, e => e.Category)
            .Take(6)
            .ToListAsync();

        // Should have at most 6 results (global limit)
        Assert.True(results.Count <= 6, $"Expected at most 6 results, got {results.Count}");
    }

    [Fact]
    public async Task LimitBy_WithFilter_AppliesFilterBeforeLimitBy()
    {
        await using var context = CreateContext();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var now = DateTime.UtcNow;
        context.Events.AddRange(
            // High scores (> 50)
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 100, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 90, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 80, CreatedAt = now },
            // Low scores (<= 50) - should be filtered out
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "A", Region = "US", Score = 40, CreatedAt = now },
            new LimitByIntegrationEvent { Id = Guid.NewGuid(), Category = "B", Region = "US", Score = 30, CreatedAt = now }
        );
        await context.SaveChangesAsync();

        // Filter to high scores, then top 2 per category
        var results = await context.Events
            .Where(e => e.Score > 50)
            .OrderByDescending(e => e.Score)
            .LimitBy(2, e => e.Category)
            .ToListAsync();

        // All results should have Score > 50
        Assert.All(results, e => Assert.True(e.Score > 50));

        // Should have 3 results: A(100, 90), B(80)
        Assert.Equal(3, results.Count);
    }

    private LimitByIntegrationDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LimitByIntegrationDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new LimitByIntegrationDbContext(options);
    }
}

public class LimitByIntegrationDbContext : DbContext
{
    public LimitByIntegrationDbContext(DbContextOptions<LimitByIntegrationDbContext> options) : base(options)
    {
    }

    public DbSet<LimitByIntegrationEvent> Events => Set<LimitByIntegrationEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LimitByIntegrationEvent>(entity =>
        {
            entity.ToTable("LimitByEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
        });
    }
}

public class LimitByIntegrationEvent
{
    public Guid Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public int Score { get; set; }
    public DateTime CreatedAt { get; set; }
}
