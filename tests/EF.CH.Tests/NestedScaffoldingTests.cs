using ClickHouse.Driver.ADO;
using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Scaffolding.Internal;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

/// <summary>
/// Integration tests for scaffolding tables with Nested columns.
/// </summary>
public class NestedScaffoldingTests : IAsyncLifetime
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
    public async Task Scaffolding_NestedColumn_ReturnsNestedStoreType()
    {
        // Create table with Nested
        await ExecuteSqlAsync(@"
            CREATE TABLE TestNested (
                Id UUID,
                Goals Nested(ID UInt32, EventTime DateTime64(3))
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Verify table exists
        var table = model.Tables.SingleOrDefault(t => t.Name == "TestNested");
        Assert.NotNull(table);

        // Verify Goals column exists with Nested store type
        var goalsColumn = table.Columns.SingleOrDefault(c => c.Name == "Goals");
        Assert.NotNull(goalsColumn);
        Assert.Contains("Nested(", goalsColumn.StoreType);
    }

    [Fact]
    public async Task Scaffolding_NestedColumn_HasFieldAnnotation()
    {
        // Create table with Nested
        await ExecuteSqlAsync(@"
            CREATE TABLE TestNestedAnnotation (
                Id UUID,
                Goals Nested(ID UInt32, EventTime DateTime64(3))
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Get the column
        var table = model.Tables.Single(t => t.Name == "TestNestedAnnotation");
        var goalsColumn = table.Columns.Single(c => c.Name == "Goals");

        // Verify annotation contains field info
        var annotation = goalsColumn[ClickHouseAnnotationNames.NestedFields];
        Assert.NotNull(annotation);

        var fields = (string[])annotation;
        Assert.Equal(2, fields.Length);
        Assert.Contains("ID", fields[0]);
        Assert.Contains("EventTime", fields[1]);
    }

    [Fact]
    public async Task Scaffolding_NestedColumn_HasDocumentationComment()
    {
        // Create table with Nested
        await ExecuteSqlAsync(@"
            CREATE TABLE test_nested_comment (
                Id UUID,
                Goals Nested(ID UInt32, EventTime DateTime64(3))
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Get the column
        var table = model.Tables.Single(t => t.Name == "test_nested_comment");
        var goalsColumn = table.Columns.Single(c => c.Name == "Goals");

        // Verify comment contains expected content
        Assert.NotNull(goalsColumn.Comment);
        Assert.Contains("ClickHouse Nested type with fields", goalsColumn.Comment);
        Assert.Contains("TODO:", goalsColumn.Comment);
        Assert.Contains("public record", goalsColumn.Comment);
        Assert.Contains("Goal", goalsColumn.Comment); // Record name contains "Goal" (singular)
    }

    [Fact]
    public async Task Scaffolding_NestedColumn_GeneratesCorrectClrTypeNames()
    {
        // Create table with various field types
        await ExecuteSqlAsync(@"
            CREATE TABLE TestNestedTypes (
                Id UUID,
                Data Nested(
                    IntField Int32,
                    LongField Int64,
                    DoubleField Float64,
                    StringField String,
                    DateField Date,
                    GuidField UUID
                )
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Get the column
        var table = model.Tables.Single(t => t.Name == "TestNestedTypes");
        var dataColumn = table.Columns.Single(c => c.Name == "Data");

        // Verify comment contains C# type names (not ClickHouse type names)
        Assert.NotNull(dataColumn.Comment);
        Assert.Contains("int", dataColumn.Comment);
        Assert.Contains("long", dataColumn.Comment);
        Assert.Contains("double", dataColumn.Comment);
        Assert.Contains("string", dataColumn.Comment);
        Assert.Contains("DateOnly", dataColumn.Comment);
        Assert.Contains("Guid", dataColumn.Comment);
    }

    [Fact]
    public async Task Scaffolding_NestedColumn_WithNullableFields()
    {
        // Create table with Nullable nested fields
        await ExecuteSqlAsync(@"
            CREATE TABLE TestNestedNullable (
                Id UUID,
                Items Nested(Name String, Value Nullable(Int32))
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Get the column
        var table = model.Tables.Single(t => t.Name == "TestNestedNullable");
        var itemsColumn = table.Columns.Single(c => c.Name == "Items");

        // Verify store type and annotation
        Assert.Contains("Nested(", itemsColumn.StoreType);
        var annotation = itemsColumn[ClickHouseAnnotationNames.NestedFields];
        Assert.NotNull(annotation);
    }

    [Fact]
    public async Task Scaffolding_MultipleNestedColumns()
    {
        // Create table with multiple Nested columns
        await ExecuteSqlAsync(@"
            CREATE TABLE TestMultipleNested (
                Id UUID,
                Goals Nested(ID UInt32, Time DateTime64(3)),
                Assists Nested(PlayerID UInt32, Type String)
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Get the table
        var table = model.Tables.Single(t => t.Name == "TestMultipleNested");

        // Verify both nested columns exist
        var goalsColumn = table.Columns.SingleOrDefault(c => c.Name == "Goals");
        var assistsColumn = table.Columns.SingleOrDefault(c => c.Name == "Assists");

        Assert.NotNull(goalsColumn);
        Assert.NotNull(assistsColumn);
        Assert.Contains("Nested(", goalsColumn.StoreType);
        Assert.Contains("Nested(", assistsColumn.StoreType);

        // Verify each has its own annotation
        Assert.NotNull(goalsColumn[ClickHouseAnnotationNames.NestedFields]);
        Assert.NotNull(assistsColumn[ClickHouseAnnotationNames.NestedFields]);
    }

    [Fact]
    public async Task Scaffolding_NestedColumn_RecordNameFromPluralProperty()
    {
        // Create table with plural property name
        await ExecuteSqlAsync(@"
            CREATE TABLE Orders (
                Id UUID,
                Items Nested(ProductId UInt32, Quantity Int32)
            ) ENGINE = MergeTree() ORDER BY Id");

        // Scaffold
        var model = Scaffold();

        // Get the column
        var table = model.Tables.Single(t => t.Name == "Orders");
        var itemsColumn = table.Columns.Single(c => c.Name == "Items");

        // Verify the generated record name removes the 's' from 'Items'
        Assert.NotNull(itemsColumn.Comment);
        Assert.Contains("OrdersItem", itemsColumn.Comment); // Not "OrdersItems"
    }

    private async Task ExecuteSqlAsync(string sql)
    {
        using var connection = new ClickHouseConnection(GetConnectionString());
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private DatabaseModel Scaffold()
    {
        var logger = NullLogger<ClickHouseDatabaseModelFactory>.Instance;

        // Create type mapping source with required dependencies
        var typeMappingSource = CreateTypeMappingSource();

        var factory = new ClickHouseDatabaseModelFactory(logger, typeMappingSource);

        return factory.Create(
            GetConnectionString(),
            new DatabaseModelFactoryOptions());
    }

    private static ClickHouseTypeMappingSource CreateTypeMappingSource()
    {
        // Create minimal dependencies for type mapping source
        var valueConverterSelector = new ValueConverterSelector(
            new ValueConverterSelectorDependencies());

        var jsonValueReaderWriterSource = new JsonValueReaderWriterSource(
            new JsonValueReaderWriterSourceDependencies());

        var typeMappingSourceDependencies = new TypeMappingSourceDependencies(
            valueConverterSelector,
            jsonValueReaderWriterSource,
            Array.Empty<ITypeMappingSourcePlugin>());

        var relationalTypeMappingSourceDependencies = new RelationalTypeMappingSourceDependencies(
            Array.Empty<IRelationalTypeMappingSourcePlugin>());

        return new ClickHouseTypeMappingSource(
            typeMappingSourceDependencies,
            relationalTypeMappingSourceDependencies);
    }
}
