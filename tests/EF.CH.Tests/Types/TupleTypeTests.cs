using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Types;

public class TupleTypeTests : IAsyncLifetime
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
    public void ValueTuple2Mapping_GeneratesCorrectStoreType()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping(
            typeof((int, string)),
            new RelationalTypeMapping[] { intMapping, stringMapping });

        Assert.Equal("Tuple(Int32, String)", mapping.StoreType);
        Assert.Equal(typeof((int, string)), mapping.ClrType);
    }

    [Fact]
    public void ValueTuple3Mapping_GeneratesCorrectStoreType()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var boolMapping = new ClickHouseBoolTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping(
            typeof((int, string, bool)),
            new RelationalTypeMapping[] { intMapping, stringMapping, boolMapping });

        Assert.Equal("Tuple(Int32, String, Bool)", mapping.StoreType);
    }

    [Fact]
    public void NamedTupleMapping_GeneratesCorrectStoreType()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping(
            typeof((int, string)),
            new RelationalTypeMapping[] { intMapping, stringMapping },
            new[] { "id", "name" });

        Assert.Equal("Tuple(id Int32, name String)", mapping.StoreType);
    }

    [Fact]
    public void TupleMapping_GeneratesCorrectLiteral_IntString()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping(
            typeof((int, string)),
            new RelationalTypeMapping[] { intMapping, stringMapping });

        var tuple = (42, "hello");
        var literal = mapping.GenerateSqlLiteral(tuple);

        Assert.Equal("(42, 'hello')", literal);
    }

    [Fact]
    public void TupleMapping_GeneratesCorrectLiteral_ThreeElements()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var boolMapping = new ClickHouseBoolTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping(
            typeof((int, string, bool)),
            new RelationalTypeMapping[] { intMapping, stringMapping, boolMapping });

        var tuple = (100, "test", true);
        var literal = mapping.GenerateSqlLiteral(tuple);

        Assert.Equal("(100, 'test', true)", literal);
    }

    [Fact]
    public void ReferenceTupleMapping_GeneratesCorrectLiteral()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var stringMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseTupleTypeMapping(
            typeof(Tuple<int, string>),
            new RelationalTypeMapping[] { intMapping, stringMapping });

        var tuple = Tuple.Create(42, "hello");
        var literal = mapping.GenerateSqlLiteral(tuple);

        Assert.Equal("(42, 'hello')", literal);
    }

    [Fact]
    public void TupleMapping_WithNestedArray_GeneratesCorrectStoreType()
    {
        var intMapping = new ClickHouseInt32TypeMapping();
        var arrayMapping = new ClickHouseArrayTypeMapping(typeof(string[]), new ClickHouseStringTypeMapping());
        var mapping = new ClickHouseTupleTypeMapping(
            typeof((int, string[])),
            new RelationalTypeMapping[] { intMapping, arrayMapping });

        Assert.Equal("Tuple(Int32, Array(String))", mapping.StoreType);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanInsertAndQueryTuple()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Coordinates" (
                "Id" UUID,
                "Point" Tuple(Float64, Float64)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert using tuple literal
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Coordinates"" (""Id"", ""Point"")
              VALUES ('" + id + @"', (40.7128, -74.0060))");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""Coordinates"" WHERE ""Point"".1 > 40"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryTupleElements()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "NamedPoints" (
                "Id" UUID,
                "Location" Tuple(x Float64, y Float64, z Float64)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""NamedPoints"" (""Id"", ""Location"")
              VALUES ('" + Guid.NewGuid() + @"', (1.0, 2.0, 3.0))");

        // Query using named tuple element access
        var hasPoint = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""NamedPoints"" WHERE ""Location"".x = 1.0"
        ).AnyAsync();

        Assert.True(hasPoint);
    }

    [Fact]
    public async Task CanQueryTupleWithMixedTypes()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "PersonInfo" (
                "Id" UUID,
                "Info" Tuple(String, Int32, Bool)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""PersonInfo"" (""Id"", ""Info"")
              VALUES ('" + Guid.NewGuid() + @"', ('Alice', 30, true))");

        // Query tuple element by position
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""PersonInfo"" WHERE ""Info"".1 = 'Alice'"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanUseTupleFunction()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "TupleTest" (
                "Id" UUID,
                "Data" Tuple(Int32, String)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert using tuple() function instead of literal
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""TupleTest"" (""Id"", ""Data"")
              VALUES ('" + Guid.NewGuid() + @"', tuple(42, 'test'))");

        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""TupleTest"" WHERE ""Data"".1 = 42"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryArrayOfTuples()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "KeyValuePairs" (
                "Id" UUID,
                "Pairs" Array(Tuple(String, Int32))
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""KeyValuePairs"" (""Id"", ""Pairs"")
              VALUES ('" + Guid.NewGuid() + @"', [('a', 1), ('b', 2), ('c', 3)])");

        // Query using array function on tuples
        var hasElements = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""KeyValuePairs"" WHERE length(""Pairs"") = 3"
        ).AnyAsync();

        Assert.True(hasElements);
    }

    #endregion

    private TupleTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TupleTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new TupleTestContext(options);
    }
}

#region Test Context

public class TupleTestContext : DbContext
{
    public TupleTestContext(DbContextOptions<TupleTestContext> options)
        : base(options) { }
}

#endregion
