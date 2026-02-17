using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Core;

public class UpdateTests : IAsyncLifetime
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
    public async Task ExecuteUpdateAsync_UpdatesSingleColumn()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""UpdateProducts"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Price"" Decimal64(2),
                ""Category"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        var id = Guid.NewGuid();
        context.Products.Add(new UpdateProduct
        {
            Id = id,
            Name = "Widget",
            Price = 10.00m,
            Category = "electronics",
            UpdatedAt = DateTime.UtcNow
        });
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        await context.Products
            .Where(p => p.Id == id)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, "Updated Widget"));

        // Allow mutation to process
        await Task.Delay(500);

        context.ChangeTracker.Clear();
        var product = await context.Products.FirstAsync(p => p.Id == id);
        Assert.Equal("Updated Widget", product.Name);
        Assert.Equal(10.00m, product.Price); // Other columns unchanged
    }

    [Fact]
    public async Task ExecuteUpdateAsync_UpdatesMultipleColumns()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""UpdateProducts"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Price"" Decimal64(2),
                ""Category"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        var id = Guid.NewGuid();
        context.Products.Add(new UpdateProduct
        {
            Id = id,
            Name = "Widget",
            Price = 10.00m,
            Category = "electronics",
            UpdatedAt = DateTime.UtcNow
        });
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var newTime = DateTime.UtcNow;
        await context.Products
            .Where(p => p.Id == id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(p => p.Name, "Premium Widget")
                .SetProperty(p => p.Category, "premium"));

        await Task.Delay(500);

        context.ChangeTracker.Clear();
        var product = await context.Products.FirstAsync(p => p.Id == id);
        Assert.Equal("Premium Widget", product.Name);
        Assert.Equal("premium", product.Category);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_WithExpression()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""UpdateProducts"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Price"" Decimal64(2),
                ""Category"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        context.Products.AddRange(
            new UpdateProduct { Id = id1, Name = "Widget A", Price = 100.00m, Category = "electronics", UpdatedAt = DateTime.UtcNow },
            new UpdateProduct { Id = id2, Name = "Widget B", Price = 200.00m, Category = "electronics", UpdatedAt = DateTime.UtcNow }
        );
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        // Use expression-based update: Price * 1.1
        await context.Products
            .Where(p => p.Category == "electronics")
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Price, p => p.Price * 1.1m));

        await Task.Delay(500);

        context.ChangeTracker.Clear();
        var products = await context.Products.OrderBy(p => p.Name).ToListAsync();
        Assert.Equal(110.00m, products[0].Price);
        Assert.Equal(220.00m, products[1].Price);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_NoMatchingRows()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""UpdateProducts"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Price"" Decimal64(2),
                ""Category"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        // Should not throw even with no matching rows
        await context.Products
            .Where(p => p.Category == "nonexistent")
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, "Updated"));
    }

    [Fact]
    public async Task SaveChanges_ModifiedEntity_StillThrowsNotSupported()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""UpdateProducts"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Price"" Decimal64(2),
                ""Category"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        var entity = new UpdateProduct
        {
            Id = Guid.NewGuid(),
            Name = "Original",
            Price = 10.00m,
            Category = "test",
            UpdatedAt = DateTime.UtcNow
        };
        context.Products.Add(entity);
        await context.SaveChangesAsync();

        // Try to update via change tracker (should still throw)
        context.ChangeTracker.Clear();
        var toUpdate = await context.Products.FirstAsync(p => p.Id == entity.Id);
        toUpdate.Name = "Changed";

        var ex = await Assert.ThrowsAsync<ClickHouseUnsupportedOperationException>(
            () => context.SaveChangesAsync());

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.Update, ex.Category);
    }

    private UpdateTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<UpdateTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new UpdateTestContext(options);
    }
}

public class UpdateProduct
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Category { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }
}

public class UpdateTestContext : DbContext
{
    public UpdateTestContext(DbContextOptions<UpdateTestContext> options)
        : base(options) { }

    public DbSet<UpdateProduct> Products => Set<UpdateProduct>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UpdateProduct>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("UpdateProducts");
            entity.UseMergeTree(x => x.Id);
        });
    }
}
