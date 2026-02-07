using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class CteTests : IAsyncLifetime
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
    public async Task AsCte_BasicFilter()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        // Filter + AsCte should wrap the source query as a CTE
        var result = await context.CteEvents
            .Where(e => e.Category == "electronics")
            .AsCte("filtered")
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.All(result, e => Assert.Equal("electronics", e.Category));
    }

    [Fact]
    public async Task AsCte_WithOrderBy()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.CteEvents
            .Where(e => e.Amount > 50)
            .AsCte("expensive")
            .OrderBy(e => e.Name)
            .ToListAsync();

        Assert.True(result.Count > 0);
        // Verify ordering
        for (var i = 1; i < result.Count; i++)
        {
            Assert.True(string.Compare(result[i - 1].Name, result[i].Name, StringComparison.Ordinal) <= 0);
        }
    }

    [Fact]
    public async Task AsCte_IntegrationTest()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        // Use CTE with aggregation-like operations (Take/Count)
        var result = await context.CteEvents
            .Where(e => e.EventDate > DateTime.UtcNow.AddDays(-30))
            .AsCte("recent")
            .OrderByDescending(e => e.Amount)
            .Take(5)
            .ToListAsync();

        Assert.True(result.Count <= 5);
        Assert.True(result.Count > 0);
    }

    [Fact]
    public void AsCte_EmptyName_Throws()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentException>(() =>
            context.CteEvents.AsCte(""));
    }

    [Fact]
    public void AsCte_NullName_Throws()
    {
        using var context = CreateContext();

        Assert.ThrowsAny<ArgumentException>(() =>
            context.CteEvents.AsCte(null!));
    }

    private async Task SetupTable(CteTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""CteEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Category"" String,
                ""Amount"" Decimal64(2),
                ""EventDate"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");
    }

    private async Task SeedData(CteTestContext context)
    {
        var now = DateTime.UtcNow;
        context.CteEvents.AddRange(
            new CteEvent { Id = Guid.NewGuid(), Name = "Laptop Sale", Category = "electronics", Amount = 999.99m, EventDate = now.AddDays(-1) },
            new CteEvent { Id = Guid.NewGuid(), Name = "Phone Sale", Category = "electronics", Amount = 599.99m, EventDate = now.AddDays(-2) },
            new CteEvent { Id = Guid.NewGuid(), Name = "Cable Sale", Category = "electronics", Amount = 9.99m, EventDate = now.AddDays(-3) },
            new CteEvent { Id = Guid.NewGuid(), Name = "Book Sale", Category = "books", Amount = 29.99m, EventDate = now.AddDays(-5) },
            new CteEvent { Id = Guid.NewGuid(), Name = "Magazine Sale", Category = "books", Amount = 5.99m, EventDate = now.AddDays(-10) }
        );
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
    }

    private CteTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<CteTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new CteTestContext(options);
    }
}

public class CteEvent
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime EventDate { get; set; }
}

public class CteTestContext : DbContext
{
    public CteTestContext(DbContextOptions<CteTestContext> options)
        : base(options) { }

    public DbSet<CteEvent> CteEvents => Set<CteEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CteEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("CteEvents");
            entity.UseMergeTree(x => x.Id);
        });
    }
}
