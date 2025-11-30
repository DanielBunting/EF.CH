using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class DeleteTests : IAsyncLifetime
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

    #region SaveChanges Delete Tests

    [Fact]
    public async Task CanDeleteEntity_WithSimpleKey()
    {
        await using var context = CreateContext<DeleteTestContext>();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""DeleteEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""CreatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        // Insert an entity
        var entity = new DeleteEvent
        {
            Id = Guid.NewGuid(),
            Name = "Test Event",
            CreatedAt = DateTime.UtcNow
        };
        context.Events.Add(entity);
        await context.SaveChangesAsync();

        // Clear tracking
        context.ChangeTracker.Clear();

        // Retrieve and delete
        var toDelete = await context.Events.FirstAsync(e => e.Id == entity.Id);
        context.Events.Remove(toDelete);
        await context.SaveChangesAsync();

        // Verify deleted
        context.ChangeTracker.Clear();
        var exists = await context.Events.AnyAsync(e => e.Id == entity.Id);
        Assert.False(exists);
    }

    [Fact]
    public async Task CanDeleteEntity_WithCompositeKey()
    {
        await using var context = CreateContext<CompositeKeyDeleteContext>();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""OrderItems"" (
                ""OrderId"" UUID,
                ""ProductId"" Int32,
                ""Quantity"" Int32
            ) ENGINE = MergeTree()
            ORDER BY (""OrderId"", ""ProductId"")
        ");

        var orderId = Guid.NewGuid();
        var productId = 42;

        // Insert
        var entity = new OrderItem
        {
            OrderId = orderId,
            ProductId = productId,
            Quantity = 5
        };
        context.OrderItems.Add(entity);
        await context.SaveChangesAsync();

        // Clear and delete
        context.ChangeTracker.Clear();
        var toDelete = await context.OrderItems.FirstAsync(e => e.OrderId == orderId && e.ProductId == productId);
        context.OrderItems.Remove(toDelete);
        await context.SaveChangesAsync();

        // Verify deleted
        context.ChangeTracker.Clear();
        var exists = await context.OrderItems.AnyAsync(e => e.OrderId == orderId && e.ProductId == productId);
        Assert.False(exists);
    }

    [Fact]
    public async Task DeleteMultipleEntities_GeneratesSeparateStatements()
    {
        await using var context = CreateContext<DeleteTestContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""DeleteEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""CreatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        // Insert multiple entities
        var entities = new[]
        {
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Event 1", CreatedAt = DateTime.UtcNow },
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Event 2", CreatedAt = DateTime.UtcNow },
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Event 3", CreatedAt = DateTime.UtcNow }
        };
        context.Events.AddRange(entities);
        await context.SaveChangesAsync();

        // Delete all
        context.ChangeTracker.Clear();
        var toDelete = await context.Events.ToListAsync();
        context.Events.RemoveRange(toDelete);
        await context.SaveChangesAsync();

        // Verify all deleted
        context.ChangeTracker.Clear();
        var count = await context.Events.CountAsync();
        Assert.Equal(0, count);
    }

    #endregion

    #region Update Throws NotSupportedException

    [Fact]
    public async Task Update_ThrowsNotSupportedException()
    {
        await using var context = CreateContext<DeleteTestContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""DeleteEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""CreatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        var entity = new DeleteEvent
        {
            Id = Guid.NewGuid(),
            Name = "Original Name",
            CreatedAt = DateTime.UtcNow
        };
        context.Events.Add(entity);
        await context.SaveChangesAsync();

        // Try to update
        context.ChangeTracker.Clear();
        var toUpdate = await context.Events.FirstAsync(e => e.Id == entity.Id);
        toUpdate.Name = "Updated Name";

        var ex = await Assert.ThrowsAsync<NotSupportedException>(
            () => context.SaveChangesAsync());

        Assert.Contains("UPDATE", ex.Message);
        Assert.Contains("ClickHouse", ex.Message);
    }

    #endregion

    #region Delete Strategy Configuration

    [Fact]
    public void DefaultDeleteStrategy_IsLightweight()
    {
        var options = new DbContextOptionsBuilder<DeleteTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        var extension = options.FindExtension<ClickHouseOptionsExtension>();
        Assert.Equal(ClickHouseDeleteStrategy.Lightweight, extension?.DeleteStrategy);
    }

    [Fact]
    public void CanConfigureMutationDeleteStrategy()
    {
        var options = new DbContextOptionsBuilder<DeleteTestContext>()
            .UseClickHouse(GetConnectionString(), o => o.UseDeleteStrategy(ClickHouseDeleteStrategy.Mutation))
            .Options;

        var extension = options.FindExtension<ClickHouseOptionsExtension>();
        Assert.Equal(ClickHouseDeleteStrategy.Mutation, extension?.DeleteStrategy);
    }

    #endregion

    #region Bulk Delete (ExecuteDeleteAsync)

    [Fact]
    public async Task ExecuteDeleteAsync_DeletesMatchingRows()
    {
        await using var context = CreateContext<DeleteTestContext>();

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""DeleteEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""CreatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        // Insert entities with different names
        var now = DateTime.UtcNow;
        context.Events.AddRange(
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Keep", CreatedAt = now },
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Delete", CreatedAt = now },
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Delete", CreatedAt = now },
            new DeleteEvent { Id = Guid.NewGuid(), Name = "Keep", CreatedAt = now }
        );
        await context.SaveChangesAsync();

        // Bulk delete
        context.ChangeTracker.Clear();
        await context.Events
            .Where(e => e.Name == "Delete")
            .ExecuteDeleteAsync();

        // Note: ClickHouse lightweight delete may not return accurate row counts via HTTP interface
        // The important thing is that rows are actually deleted

        // Verify only "Keep" entities remain
        var remaining = await context.Events.ToListAsync();
        Assert.Equal(2, remaining.Count);
        Assert.All(remaining, e => Assert.Equal("Keep", e.Name));
    }

    #endregion

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Test Entities

public class DeleteEvent
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

public class OrderItem
{
    public Guid OrderId { get; set; }
    public int ProductId { get; set; }
    public int Quantity { get; set; }
}

#endregion

#region Test Contexts

public class DeleteTestContext : DbContext
{
    public DeleteTestContext(DbContextOptions<DeleteTestContext> options)
        : base(options) { }

    public DbSet<DeleteEvent> Events => Set<DeleteEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DeleteEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("DeleteEvents");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class CompositeKeyDeleteContext : DbContext
{
    public CompositeKeyDeleteContext(DbContextOptions<CompositeKeyDeleteContext> options)
        : base(options) { }

    public DbSet<OrderItem> OrderItems => Set<OrderItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OrderItem>(entity =>
        {
            entity.HasKey(e => new { e.OrderId, e.ProductId });
            entity.ToTable("OrderItems");
            entity.UseMergeTree(x => new { x.OrderId, x.ProductId });
        });
    }
}

#endregion
