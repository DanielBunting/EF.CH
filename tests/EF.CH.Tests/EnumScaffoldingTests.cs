using EF.CH.Design.Internal;
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

namespace EF.CH.Tests;

public class EnumScaffoldingTests : IAsyncLifetime
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

    #region Enum Code Generator Unit Tests

    [Fact]
    public void EnumCodeGenerator_GeneratesValidEnum8Code()
    {
        var generator = new ClickHouseEnumCodeGenerator();
        var code = generator.GenerateEnumCode(
            "OrderStatus",
            "Enum8('Pending' = 0, 'Processing' = 1, 'Shipped' = 2, 'Delivered' = 3)",
            "MyApp.Models");

        Assert.Contains("namespace MyApp.Models;", code);
        Assert.Contains("public enum OrderStatus", code);
        Assert.Contains("Pending = 0", code);
        Assert.Contains("Processing = 1", code);
        Assert.Contains("Shipped = 2", code);
        Assert.Contains("Delivered = 3", code);
        Assert.Contains("Enum8", code); // Comment should mention Enum8
    }

    [Fact]
    public void EnumCodeGenerator_GeneratesValidEnum16Code()
    {
        var generator = new ClickHouseEnumCodeGenerator();
        var code = generator.GenerateEnumCode(
            "Priority",
            "Enum16('Low' = 1, 'Medium' = 100, 'High' = 1000)",
            "MyApp");

        Assert.Contains("public enum Priority", code);
        Assert.Contains("Low = 1", code);
        Assert.Contains("Medium = 100", code);
        Assert.Contains("High = 1000", code);
        Assert.Contains("Enum16", code);
    }

    [Fact]
    public void EnumCodeGenerator_HandlesNegativeValues()
    {
        var generator = new ClickHouseEnumCodeGenerator();
        var code = generator.GenerateEnumCode(
            "Polarity",
            "Enum8('Negative' = -1, 'Neutral' = 0, 'Positive' = 1)",
            "Test");

        Assert.Contains("Negative = -1", code);
        Assert.Contains("Neutral = 0", code);
        Assert.Contains("Positive = 1", code);
    }

    [Fact]
    public void EnumCodeGenerator_SanitizesInvalidIdentifiers()
    {
        var generator = new ClickHouseEnumCodeGenerator();
        var code = generator.GenerateEnumCode(
            "Status",
            "Enum8('in-progress' = 1, '123-start' = 2, 'normal' = 3)",
            "Test");

        // Invalid characters should be replaced with underscores
        Assert.Contains("in_progress = 1", code);
        Assert.Contains("_123_start = 2", code);
        Assert.Contains("normal = 3", code);

        // Should have comment for sanitized names
        Assert.Contains("Original ClickHouse value: 'in-progress'", code);
        Assert.Contains("Original ClickHouse value: '123-start'", code);
    }

    [Fact]
    public void EnumCodeGenerator_HandlesEmptyOrInvalidDefinition()
    {
        var generator = new ClickHouseEnumCodeGenerator();

        var emptyResult = generator.GenerateEnumCode("Test", "Enum8()", "MyApp");
        Assert.Empty(emptyResult);

        var invalidResult = generator.GenerateEnumCode("Test", "NotAnEnum", "MyApp");
        Assert.Empty(invalidResult);
    }

    #endregion

    #region Database Model Factory Integration Tests

    [Fact]
    public async Task Scaffolding_EnumColumn_HasEnumAnnotations()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_enum (
                id UInt64,
                status Enum8('Pending' = 0, 'Processing' = 1, 'Shipped' = 2, 'Delivered' = 3)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_enum");
        Assert.NotNull(table);

        var statusCol = table.Columns.First(c => c.Name == "status");

        // Verify enum annotations are present
        var enumDef = statusCol[ClickHouseAnnotationNames.EnumDefinition] as string;
        Assert.NotNull(enumDef);
        Assert.Contains("Enum8", enumDef);
        Assert.Contains("Pending", enumDef);
        Assert.Contains("Shipped", enumDef);

        var enumTypeName = statusCol[ClickHouseAnnotationNames.EnumTypeName] as string;
        Assert.NotNull(enumTypeName);
        // Column name "status" should become "Status" enum type name
        Assert.Equal("Status", enumTypeName);
    }

    [Fact]
    public async Task Scaffolding_NullableEnumColumn_PreservesNullabilityAndAnnotations()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_nullable_enum (
                id UInt64,
                status Nullable(Enum8('Active' = 1, 'Inactive' = 0))
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_nullable_enum");
        Assert.NotNull(table);

        var statusCol = table.Columns.First(c => c.Name == "status");
        Assert.True(statusCol.IsNullable);

        // Enum annotations should still be present
        var enumDef = statusCol[ClickHouseAnnotationNames.EnumDefinition] as string;
        Assert.NotNull(enumDef);
        Assert.Contains("Active", enumDef);
        Assert.Contains("Inactive", enumDef);
    }

    [Fact]
    public async Task Scaffolding_Enum16Column_HandlesLargeValues()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_enum16 (
                id UInt64,
                priority Enum16('Critical' = 1000, 'High' = 500, 'Medium' = 100, 'Low' = 1)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_enum16");
        Assert.NotNull(table);

        var priorityCol = table.Columns.First(c => c.Name == "priority");

        var enumDef = priorityCol[ClickHouseAnnotationNames.EnumDefinition] as string;
        Assert.NotNull(enumDef);
        Assert.Contains("Enum16", enumDef);
        Assert.Contains("Critical", enumDef);
        Assert.Contains("1000", enumDef);
    }

    // NOTE: LowCardinality(Enum8) is not supported by ClickHouse
    // LowCardinality only works with numbers, strings, Date, or DateTime

    [Fact]
    public async Task Scaffolding_MultipleEnumColumns_GeneratesUniqueTypeNames()
    {
        await using var context = CreateContext(GetConnectionString());
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS scaffold_multi_enum (
                id UInt64,
                order_status Enum8('Pending' = 0, 'Complete' = 1),
                payment_status Enum8('Unpaid' = 0, 'Paid' = 1)
            )
            ENGINE = MergeTree()
            ORDER BY id
            """);

        var factory = CreateDatabaseModelFactory();
        var model = factory.Create(GetConnectionString(), new DatabaseModelFactoryOptions());

        var table = model.Tables.FirstOrDefault(t => t.Name == "scaffold_multi_enum");
        Assert.NotNull(table);

        var orderStatusCol = table.Columns.First(c => c.Name == "order_status");
        var paymentStatusCol = table.Columns.First(c => c.Name == "payment_status");

        var orderEnumName = orderStatusCol[ClickHouseAnnotationNames.EnumTypeName] as string;
        var paymentEnumName = paymentStatusCol[ClickHouseAnnotationNames.EnumTypeName] as string;

        Assert.NotNull(orderEnumName);
        Assert.NotNull(paymentEnumName);
        Assert.NotEqual(orderEnumName, paymentEnumName);
    }

    #endregion

    #region Helpers

    private EnumScaffoldingTestContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<EnumScaffoldingTestContext>()
            .UseClickHouse(connectionString)
            .Options;

        return new EnumScaffoldingTestContext(options);
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

public class EnumScaffoldingTestContext : DbContext
{
    public EnumScaffoldingTestContext(DbContextOptions<EnumScaffoldingTestContext> options)
        : base(options) { }
}
