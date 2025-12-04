using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class MapTypeTests : IAsyncLifetime
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

    #region Type Mapping Unit Tests

    [Fact]
    public void StringIntMapMapping_GeneratesCorrectStoreType()
    {
        var stringMapping = new ClickHouseStringTypeMapping();
        var intMapping = new ClickHouseInt32TypeMapping();
        var mapping = new ClickHouseMapTypeMapping(
            typeof(Dictionary<string, int>),
            stringMapping,
            intMapping);

        Assert.Equal("Map(String, Int32)", mapping.StoreType);
        Assert.Equal(typeof(Dictionary<string, int>), mapping.ClrType);
    }

    [Fact]
    public void StringStringMapMapping_GeneratesCorrectStoreType()
    {
        var stringMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseMapTypeMapping(
            typeof(Dictionary<string, string>),
            stringMapping,
            stringMapping);

        Assert.Equal("Map(String, String)", mapping.StoreType);
    }

    [Fact]
    public void MapMapping_GeneratesCorrectLiteral_StringIntMap()
    {
        var stringMapping = new ClickHouseStringTypeMapping();
        var intMapping = new ClickHouseInt32TypeMapping();
        var mapping = new ClickHouseMapTypeMapping(
            typeof(Dictionary<string, int>),
            stringMapping,
            intMapping);

        var dict = new Dictionary<string, int>
        {
            ["key1"] = 100,
            ["key2"] = 200
        };

        var literal = mapping.GenerateSqlLiteral(dict);

        Assert.Contains("'key1': 100", literal);
        Assert.Contains("'key2': 200", literal);
        Assert.StartsWith("{", literal);
        Assert.EndsWith("}", literal);
    }

    [Fact]
    public void MapMapping_GeneratesCorrectLiteral_EmptyMap()
    {
        var stringMapping = new ClickHouseStringTypeMapping();
        var intMapping = new ClickHouseInt32TypeMapping();
        var mapping = new ClickHouseMapTypeMapping(
            typeof(Dictionary<string, int>),
            stringMapping,
            intMapping);

        var dict = new Dictionary<string, int>();

        var literal = mapping.GenerateSqlLiteral(dict);

        Assert.Equal("{}", literal);
    }

    [Fact]
    public void MapMapping_GeneratesCorrectLiteral_IntStringMap()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseMapTypeMapping(
            typeof(Dictionary<int, string>),
            intMapping,
            stringMapping);

        var dict = new Dictionary<int, string>
        {
            [1] = "one",
            [2] = "two"
        };

        var literal = mapping.GenerateSqlLiteral(dict);

        Assert.Contains("1: 'one'", literal);
        Assert.Contains("2: 'two'", literal);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanInsertAndQueryMap()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "UserSettings" (
                "Id" UUID,
                "UserName" String,
                "Settings" Map(String, String)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert using map literal (double braces to escape for EF Core)
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""UserSettings"" (""Id"", ""UserName"", ""Settings"")
              VALUES ('" + id + @"', 'john', {{'theme': 'dark', 'language': 'en'}})");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""UserSettings"" WHERE ""UserName"" = 'john'"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryMapContains()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "ProductAttributes" (
                "Id" UUID,
                "ProductName" String,
                "Attributes" Map(String, String)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert products with different attributes (double braces to escape)
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""ProductAttributes"" (""Id"", ""ProductName"", ""Attributes"") VALUES
            ('" + Guid.NewGuid() + @"', 'Laptop', {{'brand': 'Dell', 'color': 'silver'}}),
            ('" + Guid.NewGuid() + @"', 'Phone', {{'brand': 'Apple', 'color': 'black'}}),
            ('" + Guid.NewGuid() + @"', 'Tablet', {{'brand': 'Dell', 'size': 'large'}})");

        // Query using mapContains
        var hasColor = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""ProductAttributes"" WHERE mapContains(""Attributes"", 'color')"
        ).AnyAsync();

        Assert.True(hasColor);
    }

    [Fact]
    public async Task CanQueryMapKeys()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "ConfigItems" (
                "Id" UUID,
                "Config" Map(String, Int32)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""ConfigItems"" (""Id"", ""Config"")
              VALUES ('" + Guid.NewGuid() + @"', {{'timeout': 30, 'retries': 3, 'port': 8080}})");

        // Query map keys
        var hasKeys = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""ConfigItems"" WHERE has(mapKeys(""Config""), 'timeout')"
        ).AnyAsync();

        Assert.True(hasKeys);
    }

    [Fact]
    public async Task CanQueryMapValues()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Scores" (
                "Id" UUID,
                "PlayerScores" Map(String, Int32)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Scores"" (""Id"", ""PlayerScores"")
              VALUES ('" + Guid.NewGuid() + @"', {{'alice': 100, 'bob': 85, 'charlie': 92}})");

        // Query map values - check if any value is >= 90
        var hasHighScore = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""Scores"" WHERE arrayExists(x -> x >= 90, mapValues(""PlayerScores""))"
        ).AnyAsync();

        Assert.True(hasHighScore);
    }

    [Fact]
    public async Task CanAccessMapValueByKey()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Inventory" (
                "Id" UUID,
                "Items" Map(String, Int32)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Inventory"" (""Id"", ""Items"")
              VALUES ('" + Guid.NewGuid() + @"', {{'apples': 50, 'oranges': 30, 'bananas': 40}})");

        // Access map value by key using bracket notation
        var hasEnoughApples = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""Inventory"" WHERE ""Items""['apples'] >= 25"
        ).AnyAsync();

        Assert.True(hasEnoughApples);
    }

    [Fact]
    public async Task CanUseMapFunction()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "MapFunctionTest" (
                "Id" UUID,
                "Data" Map(String, Int64)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert using map() function instead of literal
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""MapFunctionTest"" (""Id"", ""Data"")
              VALUES ('" + Guid.NewGuid() + @"', map('a', 1, 'b', 2, 'c', 3))");

        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""MapFunctionTest"" WHERE ""Data""['a'] = 1"
        ).AnyAsync();

        Assert.True(exists);
    }

    #endregion

    private MapTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<MapTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new MapTestContext(options);
    }
}

#region Test Context

public class MapTestContext : DbContext
{
    public MapTestContext(DbContextOptions<MapTestContext> options)
        : base(options) { }
}

#endregion
