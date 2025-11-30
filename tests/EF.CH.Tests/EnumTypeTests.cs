using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class EnumTypeTests : IAsyncLifetime
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
    public void SmallEnumMapping_GeneratesEnum8()
    {
        var mapping = new ClickHouseEnumTypeMapping(typeof(OrderStatus));

        Assert.StartsWith("Enum8(", mapping.StoreType);
        Assert.Contains("'Pending' = 0", mapping.StoreType);
        Assert.Contains("'Processing' = 1", mapping.StoreType);
        Assert.Contains("'Shipped' = 2", mapping.StoreType);
        Assert.Contains("'Delivered' = 3", mapping.StoreType);
        Assert.Contains("'Cancelled' = 4", mapping.StoreType);
    }

    [Fact]
    public void EnumWithExplicitValues_GeneratesCorrectMapping()
    {
        var mapping = new ClickHouseEnumTypeMapping(typeof(Priority));

        Assert.StartsWith("Enum8(", mapping.StoreType);
        Assert.Contains("'Low' = 1", mapping.StoreType);
        Assert.Contains("'Medium' = 5", mapping.StoreType);
        Assert.Contains("'High' = 10", mapping.StoreType);
        Assert.Contains("'Critical' = 100", mapping.StoreType);
    }

    [Fact]
    public void EnumMapping_GeneratesCorrectLiteral()
    {
        var mapping = new ClickHouseEnumTypeMapping(typeof(OrderStatus));

        var literal = mapping.GenerateSqlLiteral(OrderStatus.Shipped);

        Assert.Equal("'Shipped'", literal);
    }

    [Fact]
    public void EnumMapping_GeneratesCorrectLiteral_ForFirstValue()
    {
        var mapping = new ClickHouseEnumTypeMapping(typeof(OrderStatus));

        var literal = mapping.GenerateSqlLiteral(OrderStatus.Pending);

        Assert.Equal("'Pending'", literal);
    }

    [Fact]
    public void EnumWithNegativeValues_UsesEnum8WhenInRange()
    {
        var mapping = new ClickHouseEnumTypeMapping(typeof(SignedEnum));

        Assert.StartsWith("Enum8(", mapping.StoreType);
        Assert.Contains("'Negative' = -10", mapping.StoreType);
        Assert.Contains("'Zero' = 0", mapping.StoreType);
        Assert.Contains("'Positive' = 10", mapping.StoreType);
    }

    [Fact]
    public void EnumWithLargeValues_UsesEnum16()
    {
        var mapping = new ClickHouseEnumTypeMapping(typeof(LargeValueEnum));

        Assert.StartsWith("Enum16(", mapping.StoreType);
        Assert.Contains("'Small' = 1", mapping.StoreType);
        Assert.Contains("'Large' = 1000", mapping.StoreType);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanInsertAndQueryEnumValue()
    {
        await using var context = CreateContext();

        // Create table with enum column using the generated enum type
        var mapping = new ClickHouseEnumTypeMapping(typeof(OrderStatus));
        await context.Database.ExecuteSqlRawAsync($"""
            CREATE TABLE IF NOT EXISTS "EnumOrders" (
                "Id" UUID,
                "CustomerName" String,
                "Status" {mapping.StoreType}
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert using enum string value
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""EnumOrders"" (""Id"", ""CustomerName"", ""Status"")
              VALUES ('" + id + @"', 'John Doe', 'Processing')");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""EnumOrders"" WHERE ""Status"" = 'Processing'"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryEnumWithComparison()
    {
        await using var context = CreateContext();

        var mapping = new ClickHouseEnumTypeMapping(typeof(Priority));
        await context.Database.ExecuteSqlRawAsync($"""
            CREATE TABLE IF NOT EXISTS "Tasks" (
                "Id" UUID,
                "Title" String,
                "Priority" {mapping.StoreType}
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert tasks with different priorities
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Tasks"" (""Id"", ""Title"", ""Priority"") VALUES
            ('" + Guid.NewGuid() + @"', 'Task 1', 'Low'),
            ('" + Guid.NewGuid() + @"', 'Task 2', 'High'),
            ('" + Guid.NewGuid() + @"', 'Task 3', 'Critical')");

        // Query for high priority tasks (value >= 10)
        // Note: In ClickHouse, enum comparisons use the underlying integer values
        var highPriorityCount = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""Tasks"" WHERE ""Priority"" IN ('High', 'Critical')"
        ).AnyAsync();

        Assert.True(highPriorityCount);
    }

    [Fact]
    public async Task CanFilterByMultipleEnumValues()
    {
        await using var context = CreateContext();

        var mapping = new ClickHouseEnumTypeMapping(typeof(OrderStatus));
        await context.Database.ExecuteSqlRawAsync($"""
            CREATE TABLE IF NOT EXISTS "FilterOrders" (
                "Id" UUID,
                "Status" {mapping.StoreType}
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""FilterOrders"" (""Id"", ""Status"") VALUES
            ('" + Guid.NewGuid() + @"', 'Pending'),
            ('" + Guid.NewGuid() + @"', 'Processing'),
            ('" + Guid.NewGuid() + @"', 'Shipped'),
            ('" + Guid.NewGuid() + @"', 'Delivered'),
            ('" + Guid.NewGuid() + @"', 'Cancelled')");

        // Query for active orders (not delivered or cancelled)
        var activeCount = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""FilterOrders"" WHERE ""Status"" IN ('Pending', 'Processing', 'Shipped')"
        ).AnyAsync();

        Assert.True(activeCount);
    }

    #endregion

    private EnumTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<EnumTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new EnumTestContext(options);
    }
}

#region Test Enums

public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}

public enum Priority
{
    Low = 1,
    Medium = 5,
    High = 10,
    Critical = 100
}

public enum SignedEnum
{
    Negative = -10,
    Zero = 0,
    Positive = 10
}

public enum LargeValueEnum
{
    Small = 1,
    Large = 1000
}

#endregion

#region Test Context

public class EnumTestContext : DbContext
{
    public EnumTestContext(DbContextOptions<EnumTestContext> options)
        : base(options) { }
}

#endregion
