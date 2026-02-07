using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class SetOperationTests : IAsyncLifetime
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
    public async Task Concat_GeneratesUnionAll()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var books = context.SetItems.Where(i => i.Category == "books");

        // Concat = UNION ALL (keeps duplicates)
        var result = await electronics.Concat(books).OrderBy(i => i.Name).ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.Contains(result, i => i.Name == "Laptop");
        Assert.Contains(result, i => i.Name == "Phone");
        Assert.Contains(result, i => i.Name == "Novel");
        Assert.Contains(result, i => i.Name == "Textbook");
    }

    [Fact]
    public async Task Union_GeneratesUnionDistinct()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var expensive = context.SetItems.Where(i => i.Price > 50);

        // Union = UNION DISTINCT (removes duplicates)
        // Laptop (electronics, 999) appears in both queries but should be in result only once
        var result = await electronics.Union(expensive).OrderBy(i => i.Name).ToListAsync();
        Assert.Equal(3, result.Count); // Laptop, Phone, Textbook (Novel is $15, not expensive)
    }

    [Fact]
    public async Task Intersect_Works()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var expensive = context.SetItems.Where(i => i.Price > 50);

        // INTERSECT - items that are both electronics AND expensive
        var result = await electronics.Intersect(expensive).ToListAsync();
        Assert.Equal(2, result.Count); // Laptop and Phone
    }

    [Fact]
    public async Task Except_Works()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var allItems = context.SetItems;
        var electronics = context.SetItems.Where(i => i.Category == "electronics");

        // EXCEPT - all items except electronics
        var result = await allItems.Except(electronics).OrderBy(i => i.Name).ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.All(result, i => Assert.Equal("books", i.Category));
    }

    [Fact]
    public async Task MultipleUnionAll_ChainsCorrectly()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var books = context.SetItems.Where(i => i.Category == "books");
        var expensive = context.SetItems.Where(i => i.Price > 500);

        // UnionAll chains multiple UNION ALL operations
        var result = await electronics.UnionAll(books, expensive).ToListAsync();
        // electronics: 2, books: 2, expensive: 2 (Laptop + Phone) = 6 total (with duplicates)
        Assert.Equal(6, result.Count);
    }

    [Fact]
    public async Task UnionDistinct_RemovesDuplicatesAcrossMultiple()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var books = context.SetItems.Where(i => i.Category == "books");
        var expensive = context.SetItems.Where(i => i.Price > 500);

        var result = await electronics.UnionDistinct(books, expensive).ToListAsync();
        // All 4 unique items (Laptop appears in electronics and expensive, deduped)
        Assert.Equal(4, result.Count);
    }

    [Fact]
    public async Task SetOperationBuilder_ProducesCorrectQuery()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var books = context.SetItems.Where(i => i.Category == "books");

        var result = await electronics
            .AsSetOperation()
            .UnionAll(books)
            .Build()
            .OrderBy(i => i.Name)
            .Take(10)
            .ToListAsync();

        Assert.Equal(4, result.Count);
    }

    [Fact]
    public async Task SetOperationBuilder_ChainedOperations()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var electronics = context.SetItems.Where(i => i.Category == "electronics");
        var books = context.SetItems.Where(i => i.Category == "books");
        var expensive = context.SetItems.Where(i => i.Price > 500);

        // Chain: electronics UNION ALL books EXCEPT expensive
        var result = await electronics
            .AsSetOperation()
            .UnionAll(books)
            .Except(expensive)
            .Build()
            .OrderBy(i => i.Name)
            .ToListAsync();

        // (Laptop + Phone + Novel + Textbook) EXCEPT (Laptop + Phone) = Novel, Textbook
        Assert.Equal(2, result.Count);
        Assert.DoesNotContain(result, i => i.Name == "Laptop");
        Assert.DoesNotContain(result, i => i.Name == "Phone");
    }

    private async Task SetupTable(SetOperationTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""SetItems"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Category"" String,
                ""Price"" Decimal64(2)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");
    }

    private async Task SeedData(SetOperationTestContext context)
    {
        context.SetItems.AddRange(
            new SetItem { Id = Guid.NewGuid(), Name = "Laptop", Category = "electronics", Price = 999.99m },
            new SetItem { Id = Guid.NewGuid(), Name = "Phone", Category = "electronics", Price = 599.99m },
            new SetItem { Id = Guid.NewGuid(), Name = "Novel", Category = "books", Price = 15.99m },
            new SetItem { Id = Guid.NewGuid(), Name = "Textbook", Category = "books", Price = 89.99m }
        );
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
    }

    private SetOperationTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<SetOperationTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new SetOperationTestContext(options);
    }
}

public class SetItem
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class SetOperationTestContext : DbContext
{
    public SetOperationTestContext(DbContextOptions<SetOperationTestContext> options)
        : base(options) { }

    public DbSet<SetItem> SetItems => Set<SetItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SetItem>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("SetItems");
            entity.UseMergeTree(x => x.Id);
        });
    }
}
