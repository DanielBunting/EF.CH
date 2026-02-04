using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;
using EFCore = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Tests.Query;

/// <summary>
/// Tests for EFCore.Functions.RawSql extension method in projections.
/// </summary>
public class RawSqlProjectionTests : IAsyncLifetime
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

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    [Fact]
    public async Task RawSql_SimpleExpression_ReturnsCorrectValue()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test simple raw SQL in projection
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                DoubledValue = EFCore.Functions.RawSql<double>("Value * 2")
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(201.0, results[0].DoubledValue);
        Assert.Equal(400.0, results[1].DoubledValue);
    }

    [Fact]
    public async Task RawSql_ClickHouseFunction_WorksCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test ClickHouse-specific function in raw SQL
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                UpperName = EFCore.Functions.RawSql<string>("upper(Name)")
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("ALPHA", results[0].UpperName);
        Assert.Equal("BETA", results[1].UpperName);
    }

    [Fact]
    public async Task RawSql_JsonExtract_WorksCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test JSON extraction in raw SQL
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                Category = EFCore.Functions.RawSql<string>("JSONExtractString(Metadata, 'category')")
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("A", results[0].Category);
        Assert.Equal("B", results[1].Category);
    }

    [Fact]
    public async Task RawSql_WithParameters_SubstitutesCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test raw SQL with parameters
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                AdjustedValue = EFCore.Functions.RawSql<double>("Value * {0} + {1}", 2.0, 50.0)
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(251.0, results[0].AdjustedValue); // 100.5 * 2 + 50
        Assert.Equal(450.0, results[1].AdjustedValue); // 200.0 * 2 + 50
    }

    [Fact]
    public async Task RawSql_WithStringParameter_EscapesCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test raw SQL with string parameter
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                Concatenated = EFCore.Functions.RawSql<string>("concat(Name, {0})", "_suffix")
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alpha_suffix", results[0].Concatenated);
        Assert.Equal("Beta_suffix", results[1].Concatenated);
    }

    [Fact]
    public async Task RawSql_CombinedWithLinqProjection_WorksCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test combining raw SQL with LINQ projection
        var results = await context.RawSqlItems
            .Where(x => x.Value > 50)
            .Select(x => new
            {
                x.Id,
                x.Name,
                RawComputed = EFCore.Functions.RawSql<double>("round(Value, 0)"),
                LinqComputed = x.Value * 2
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(101.0, results[0].RawComputed); // round(100.5, 0)
        Assert.Equal(201.0, results[0].LinqComputed); // 100.5 * 2
        Assert.Equal(200.0, results[1].RawComputed);
        Assert.Equal(400.0, results[1].LinqComputed);
    }

    [Fact]
    public async Task RawSql_IntegerResult_WorksCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test raw SQL returning integer
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                NameLength = EFCore.Functions.RawSql<int>("length(Name)")
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal(5, results[0].NameLength); // "Alpha"
        Assert.Equal(4, results[1].NameLength); // "Beta"
    }

    [Fact]
    public async Task RawSql_BooleanResult_WorksCorrectly()
    {
        await using var context = CreateContext<RawSqlTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawSqlItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawSqlItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawSqlItems" ("Id", "Name", "Value", "Metadata")
            VALUES
                (1, 'Alpha', 100.5, '{"category": "A"}'),
                (2, 'Beta', 200.0, '{"category": "B"}')
            """);

        // Test raw SQL returning boolean
        var results = await context.RawSqlItems
            .Select(x => new
            {
                x.Id,
                IsHighValue = EFCore.Functions.RawSql<bool>("Value > 150")
            })
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.False(results[0].IsHighValue); // 100.5 > 150 = false
        Assert.True(results[1].IsHighValue);  // 200.0 > 150 = true
    }
}

#region Test Context and Entities

public class RawSqlTestContext : DbContext
{
    public RawSqlTestContext(DbContextOptions<RawSqlTestContext> options) : base(options) { }

    public DbSet<RawSqlItem> RawSqlItems => Set<RawSqlItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RawSqlItem>(entity =>
        {
            entity.ToTable("RawSqlItems");
            entity.HasNoKey();
            entity.UseMergeTree("Id");
        });
    }
}

public class RawSqlItem
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
    public string Metadata { get; set; } = string.Empty;
}

#endregion
