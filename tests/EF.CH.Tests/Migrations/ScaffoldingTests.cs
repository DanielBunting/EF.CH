using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using EF.CH.Scaffolding.Internal;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

public class ScaffoldingTests : IAsyncLifetime
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

    #region ClickHouseEngineParser Unit Tests

    [Theory]
    [InlineData("id, created_at", new[] { "id", "created_at" })]
    [InlineData("(id, created_at)", new[] { "id", "created_at" })]
    [InlineData("id", new[] { "id" })]
    [InlineData("", new string[0])]
    [InlineData(null, new string[0])]
    public void EngineParser_ParsesOrderBy(string? sortingKey, string[] expected)
    {
        var parser = new ClickHouseEngineParser();
        var metadata = parser.Parse("MergeTree", "MergeTree", sortingKey, null, null, null);
        Assert.Equal(expected, metadata.OrderBy);
    }

    [Fact]
    public void EngineParser_ParsesReplacingMergeTreeVersionColumn()
    {
        var parser = new ClickHouseEngineParser();
        var metadata = parser.Parse(
            "ReplacingMergeTree",
            "ReplacingMergeTree(version)",
            "id",
            null,
            null,
            null);

        Assert.Equal("ReplacingMergeTree", metadata.EngineName);
        Assert.Equal("version", metadata.VersionColumn);
        Assert.Equal(new[] { "id" }, metadata.OrderBy);
    }

    [Fact]
    public void EngineParser_ParsesCollapsingMergeTreeSignColumn()
    {
        var parser = new ClickHouseEngineParser();
        var metadata = parser.Parse(
            "CollapsingMergeTree",
            "CollapsingMergeTree(Sign)",
            "user_id, timestamp",
            null,
            null,
            null);

        Assert.Equal("CollapsingMergeTree", metadata.EngineName);
        Assert.Equal("Sign", metadata.SignColumn);
        Assert.Equal(new[] { "user_id", "timestamp" }, metadata.OrderBy);
    }

    [Fact]
    public void EngineParser_ParsesVersionedCollapsingMergeTree()
    {
        var parser = new ClickHouseEngineParser();
        var metadata = parser.Parse(
            "VersionedCollapsingMergeTree",
            "VersionedCollapsingMergeTree(Sign, Version)",
            "id",
            null,
            null,
            null);

        Assert.Equal("VersionedCollapsingMergeTree", metadata.EngineName);
        Assert.Equal("Sign", metadata.SignColumn);
        Assert.Equal("Version", metadata.VersionColumn);
    }

    [Fact]
    public void EngineParser_ParsesPartitionKey()
    {
        var parser = new ClickHouseEngineParser();
        var metadata = parser.Parse(
            "MergeTree",
            "MergeTree",
            "id",
            "toYYYYMM(created_at)",
            null,
            null);

        Assert.Equal("toYYYYMM(created_at)", metadata.PartitionBy);
    }

    [Fact]
    public void EngineParser_ParsesSampleBy()
    {
        var parser = new ClickHouseEngineParser();
        var metadata = parser.Parse(
            "MergeTree",
            "MergeTree",
            "id",
            null,
            null,
            "intHash32(user_id)");

        Assert.Equal("intHash32(user_id)", metadata.SampleBy);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanScaffoldSimpleMergeTreeTable()
    {
        // Create test table
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_simple (
                id UUID,
                name String,
                created_at DateTime64(3)
            )
            ENGINE = MergeTree()
            ORDER BY (created_at, id)
            """);

        // Scaffold
        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        // Verify
        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_simple");
        Assert.NotNull(table);
        Assert.Equal("scaffold_simple", table.Name);
        Assert.Equal(3, table.Columns.Count);

        // Check engine annotation
        Assert.Equal("MergeTree", table[ClickHouseAnnotationNames.Engine]);

        // Check ORDER BY annotation
        var orderBy = table[ClickHouseAnnotationNames.OrderBy] as string[];
        Assert.NotNull(orderBy);
        Assert.Equal(new[] { "created_at", "id" }, orderBy);

        // Check columns
        Assert.Contains(table.Columns, c => c.Name == "id" && c.StoreType == "UUID");
        Assert.Contains(table.Columns, c => c.Name == "name" && c.StoreType == "String");
        Assert.Contains(table.Columns, c => c.Name == "created_at" && c.StoreType == "DateTime64(3)");
    }

    [Fact]
    public async Task CanScaffoldTableWithNullableColumns()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_nullable (
                id UInt64,
                name Nullable(String),
                value Nullable(Int32)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_nullable");
        Assert.NotNull(table);

        var idCol = table.Columns.First(c => c.Name == "id");
        Assert.False(idCol.IsNullable);
        Assert.Equal("UInt64", idCol.StoreType);

        var nameCol = table.Columns.First(c => c.Name == "name");
        Assert.True(nameCol.IsNullable);
        Assert.Equal("Nullable(String)", nameCol.StoreType);

        var valueCol = table.Columns.First(c => c.Name == "value");
        Assert.True(valueCol.IsNullable);
        Assert.Equal("Nullable(Int32)", valueCol.StoreType);
    }

    [Fact]
    public async Task CanScaffoldTableWithArrayColumn()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_array (
                id UInt64,
                tags Array(String),
                values Array(Int32)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_array");
        Assert.NotNull(table);

        var tagsCol = table.Columns.First(c => c.Name == "tags");
        Assert.Equal("Array(String)", tagsCol.StoreType);

        var valuesCol = table.Columns.First(c => c.Name == "values");
        Assert.Equal("Array(Int32)", valuesCol.StoreType);
    }

    [Fact]
    public async Task CanScaffoldTableWithMapColumn()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_map (
                id UInt64,
                metadata Map(String, String),
                counts Map(String, Int32)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_map");
        Assert.NotNull(table);

        var metadataCol = table.Columns.First(c => c.Name == "metadata");
        Assert.Contains("Map(String", metadataCol.StoreType);

        var countsCol = table.Columns.First(c => c.Name == "counts");
        Assert.Contains("Map(String", countsCol.StoreType);
    }

    [Fact]
    public async Task CanScaffoldTableWithIPColumns()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_ip (
                id UInt64,
                ipv4_addr IPv4,
                ipv6_addr IPv6
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_ip");
        Assert.NotNull(table);

        var ipv4Col = table.Columns.First(c => c.Name == "ipv4_addr");
        Assert.Equal("IPv4", ipv4Col.StoreType);

        var ipv6Col = table.Columns.First(c => c.Name == "ipv6_addr");
        Assert.Equal("IPv6", ipv6Col.StoreType);
    }

    [Fact]
    public async Task CanScaffoldReplacingMergeTreeWithVersionColumn()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_replacing (
                id UInt64,
                name String,
                version UInt32
            )
            ENGINE = ReplacingMergeTree(version)
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_replacing");
        Assert.NotNull(table);

        Assert.Equal("ReplacingMergeTree", table[ClickHouseAnnotationNames.Engine]);
        Assert.Equal("version", table[ClickHouseAnnotationNames.VersionColumn]);
    }

    [Fact]
    public async Task CanScaffoldCollapsingMergeTree()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_collapsing (
                user_id UInt64,
                page_views UInt32,
                sign Int8
            )
            ENGINE = CollapsingMergeTree(sign)
            ORDER BY user_id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_collapsing");
        Assert.NotNull(table);

        Assert.Equal("CollapsingMergeTree", table[ClickHouseAnnotationNames.Engine]);
        Assert.Equal("sign", table[ClickHouseAnnotationNames.SignColumn]);
    }

    [Fact]
    public async Task CanScaffoldTableWithPartitionBy()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_partition (
                id UInt64,
                created_at DateTime
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)
            ORDER BY (created_at, id)
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_partition");
        Assert.NotNull(table);

        var partitionBy = table[ClickHouseAnnotationNames.PartitionBy] as string;
        Assert.NotNull(partitionBy);
        Assert.Contains("toYYYYMM", partitionBy);
    }

    [Fact]
    public async Task CanScaffoldWithTableFilter()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_filter_a (id UInt64) ENGINE = MergeTree() ORDER BY id
            """);
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_filter_b (id UInt64) ENGINE = MergeTree() ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var options = new DatabaseModelFactoryOptions(
            tables: new[] { "scaffold_filter_a" },
            schemas: null);

        var model = factory.Create(GetConnectionString(), options);

        // Should only include scaffold_filter_a
        Assert.Contains(model.Tables, t => t.Name == "scaffold_filter_a");
        Assert.DoesNotContain(model.Tables, t => t.Name == "scaffold_filter_b");
    }

    [Fact]
    public async Task CanScaffoldTableWithTupleColumn()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_tuple (
                id UInt64,
                point Tuple(Float64, Float64)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_tuple");
        Assert.NotNull(table);

        var pointCol = table.Columns.First(c => c.Name == "point");
        Assert.Contains("Tuple", pointCol.StoreType);
    }

    #endregion

    #region Helpers

    private ScaffoldingTestContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<ScaffoldingTestContext>()
            .UseClickHouse(connectionString)
            .Options;

        return new ScaffoldingTestContext(options);
    }

    private ClickHouseDatabaseModelFactory CreateDatabaseModelFactory()
    {
        var services = new ServiceCollection();
        services.AddEntityFrameworkClickHouse();

        var serviceProvider = services.BuildServiceProvider();
        var typeMappingSource = serviceProvider.GetRequiredService<IRelationalTypeMappingSource>();

        return new ClickHouseDatabaseModelFactory(
            NullLogger<ClickHouseDatabaseModelFactory>.Instance,
            typeMappingSource);
    }

    #endregion
}

public class ScaffoldingTestContext : DbContext
{
    public ScaffoldingTestContext(DbContextOptions<ScaffoldingTestContext> options)
        : base(options) { }
}
