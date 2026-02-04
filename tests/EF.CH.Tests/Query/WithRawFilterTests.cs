using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Query;

/// <summary>
/// Tests for WithRawFilter extension method.
/// </summary>
public class WithRawFilterTests : IAsyncLifetime
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
    public async Task WithRawFilter_SimpleCondition_GeneratesCorrectSql()
    {
        await using var context = CreateContext<RawFilterTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawFilterItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawFilterItems" (
                "Id" Int32,
                "Name" String,
                "Score" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawFilterItems" ("Id", "Name", "Score", "Metadata")
            VALUES
                (1, 'Alpha', 85.5, '{"source": "api"}'),
                (2, 'Beta', 92.0, '{"source": "web"}'),
                (3, 'Gamma', 78.0, '{"source": "api"}')
            """);

        // Test raw filter with string parameter
        var results = await context.RawFilterItems
            .WithRawFilter("Score > {0}", 80.0)
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alpha", results[0].Name);
        Assert.Equal("Beta", results[1].Name);
    }

    [Fact]
    public async Task WithRawFilter_WithLinqWhere_CombinesConditions()
    {
        await using var context = CreateContext<RawFilterTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawFilterItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawFilterItems" (
                "Id" Int32,
                "Name" String,
                "Score" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawFilterItems" ("Id", "Name", "Score", "Metadata")
            VALUES
                (1, 'Alpha', 85.5, '{"source": "api"}'),
                (2, 'Beta', 92.0, '{"source": "web"}'),
                (3, 'Gamma', 78.0, '{"source": "api"}'),
                (4, 'Delta', 95.0, '{"source": "api"}')
            """);

        // Combine LINQ Where with raw filter
        var results = await context.RawFilterItems
            .Where(x => x.Score > 80)
            .WithRawFilter("Name != {0}", "Beta")
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alpha", results[0].Name);
        Assert.Equal("Delta", results[1].Name);
    }

    [Fact]
    public async Task WithRawFilter_MultipleParameters_SubstitutesCorrectly()
    {
        await using var context = CreateContext<RawFilterTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawFilterItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawFilterItems" (
                "Id" Int32,
                "Name" String,
                "Score" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawFilterItems" ("Id", "Name", "Score", "Metadata")
            VALUES
                (1, 'Alpha', 85.5, '{"source": "api"}'),
                (2, 'Beta', 92.0, '{"source": "web"}'),
                (3, 'Gamma', 78.0, '{"source": "api"}')
            """);

        // Test with multiple parameters
        var results = await context.RawFilterItems
            .WithRawFilter("Score >= {0} AND Score <= {1}", 80.0, 90.0)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Alpha", results[0].Name);
    }

    [Fact]
    public async Task WithRawFilter_ClickHouseSpecificFunction_Works()
    {
        await using var context = CreateContext<RawFilterTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawFilterItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawFilterItems" (
                "Id" Int32,
                "Name" String,
                "Score" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawFilterItems" ("Id", "Name", "Score", "Metadata")
            VALUES
                (1, 'Alpha', 85.5, '{"source": "api"}'),
                (2, 'Beta', 92.0, '{"source": "web"}'),
                (3, 'Gamma', 78.0, '{"source": "api"}')
            """);

        // Use ClickHouse-specific JSON function in raw filter
        var results = await context.RawFilterItems
            .WithRawFilter("JSONExtractString(Metadata, 'source') = {0}", "api")
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alpha", results[0].Name);
        Assert.Equal("Gamma", results[1].Name);
    }

    [Fact]
    public async Task WithRawFilter_MultipleFilters_CombinesWithAnd()
    {
        await using var context = CreateContext<RawFilterTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawFilterItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawFilterItems" (
                "Id" Int32,
                "Name" String,
                "Score" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawFilterItems" ("Id", "Name", "Score", "Metadata")
            VALUES
                (1, 'Alpha', 85.5, '{"source": "api"}'),
                (2, 'Beta', 92.0, '{"source": "web"}'),
                (3, 'Gamma', 78.0, '{"source": "api"}'),
                (4, 'Delta', 95.0, '{"source": "api"}')
            """);

        // Chain multiple WithRawFilter calls
        var results = await context.RawFilterItems
            .WithRawFilter("Score > {0}", 80.0)
            .WithRawFilter("JSONExtractString(Metadata, 'source') = {0}", "api")
            .OrderBy(x => x.Id)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alpha", results[0].Name);
        Assert.Equal("Delta", results[1].Name);
    }

    [Fact]
    public async Task WithRawFilter_NoParameters_WorksCorrectly()
    {
        await using var context = CreateContext<RawFilterTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"RawFilterItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "RawFilterItems" (
                "Id" Int32,
                "Name" String,
                "Score" Float64,
                "Metadata" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "RawFilterItems" ("Id", "Name", "Score", "Metadata")
            VALUES
                (1, 'Alpha', 85.5, '{"source": "api"}'),
                (2, 'Beta', 92.0, '{"source": "web"}')
            """);

        // Raw filter without parameters
        var results = await context.RawFilterItems
            .WithRawFilter("Score > 90")
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Beta", results[0].Name);
    }
}

#region Test Context and Entities

public class RawFilterTestContext : DbContext
{
    public RawFilterTestContext(DbContextOptions<RawFilterTestContext> options) : base(options) { }

    public DbSet<RawFilterItem> RawFilterItems => Set<RawFilterItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RawFilterItem>(entity =>
        {
            entity.ToTable("RawFilterItems");
            entity.HasNoKey();
            entity.UseMergeTree("Id");
        });
    }
}

public class RawFilterItem
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Score { get; set; }
    public string Metadata { get; set; } = string.Empty;
}

#endregion
