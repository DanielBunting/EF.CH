using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

public class BulkInsertComplexTypesTests : IAsyncLifetime
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
    public async Task BulkInsert_WithArrayProperty_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var entities = new[]
        {
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity1",
                Tags = ["tag1", "tag2", "tag3"]
            },
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity2",
                Tags = ["single"]
            },
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity3",
                Tags = []
            }
        };

        var result = await context.BulkInsertAsync(entities);

        Assert.Equal(3, result.RowsInserted);

        var loaded = await context.ComplexEntities.OrderBy(e => e.Name).ToListAsync();
        Assert.Equal(3, loaded.Count);
        Assert.Equal(new[] { "tag1", "tag2", "tag3" }, loaded[0].Tags);
        Assert.Equal(new[] { "single" }, loaded[1].Tags);
        Assert.Empty(loaded[2].Tags);
    }

    [Fact]
    public async Task BulkInsert_WithMapProperty_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var entities = new[]
        {
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity1",
                Tags = [],
                Metadata = new Dictionary<string, int> { ["key1"] = 1, ["key2"] = 2 }
            },
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity2",
                Tags = [],
                Metadata = new Dictionary<string, int> { ["only"] = 42 }
            },
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity3",
                Tags = [],
                Metadata = new Dictionary<string, int>()
            }
        };

        var result = await context.BulkInsertAsync(entities);

        Assert.Equal(3, result.RowsInserted);

        var loaded = await context.ComplexEntities.OrderBy(e => e.Name).ToListAsync();
        Assert.Equal(3, loaded.Count);
        Assert.Equal(2, loaded[0].Metadata.Count);
        Assert.Equal(1, loaded[0].Metadata["key1"]);
        Assert.Equal(2, loaded[0].Metadata["key2"]);
    }

    [Fact]
    public async Task BulkInsert_WithDecimalProperty_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var entities = new[]
        {
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity1",
                Tags = [],
                Price = 123.45m
            },
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity2",
                Tags = [],
                Price = 0.01m
            },
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity3",
                Tags = [],
                Price = 999999.9999m
            }
        };

        var result = await context.BulkInsertAsync(entities);

        Assert.Equal(3, result.RowsInserted);

        var loaded = await context.ComplexEntities.OrderBy(e => e.Name).ToListAsync();
        Assert.Equal(123.45m, loaded[0].Price);
        Assert.Equal(0.01m, loaded[1].Price);
        Assert.Equal(999999.9999m, loaded[2].Price);
    }

    [Fact]
    public async Task BulkInsert_WithJsonEachRow_HandlesComplexTypes()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var entities = new[]
        {
            new ComplexEntity
            {
                Id = Guid.NewGuid(),
                Name = "Entity1",
                Tags = ["a", "b"],
                Metadata = new Dictionary<string, int> { ["x"] = 10 },
                Price = 99.99m
            }
        };

        var result = await context.BulkInsertAsync(entities, opts => opts
            .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow));

        Assert.Equal(1, result.RowsInserted);

        var loaded = await context.ComplexEntities.FirstAsync();
        Assert.Equal("Entity1", loaded.Name);
        Assert.Equal(new[] { "a", "b" }, loaded.Tags);
        Assert.Equal(10, loaded.Metadata["x"]);
        Assert.Equal(99.99m, loaded.Price);
    }

    private ComplexTypesDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ComplexTypesDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new ComplexTypesDbContext(options);
    }
}

public class ComplexTypesDbContext : DbContext
{
    public ComplexTypesDbContext(DbContextOptions<ComplexTypesDbContext> options) : base(options) { }

    public DbSet<ComplexEntity> ComplexEntities => Set<ComplexEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ComplexEntity>(entity =>
        {
            entity.ToTable("ComplexEntities");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class ComplexEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[] Tags { get; set; } = [];
    public Dictionary<string, int> Metadata { get; set; } = new();
    public decimal Price { get; set; }
}
