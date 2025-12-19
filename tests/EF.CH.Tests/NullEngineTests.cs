using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests;

public class NullEngineTests
{
    [Fact]
    public void UseNullEngine_SetsEngineAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestNullEntity>(entity =>
        {
            entity.ToTable("events_raw");
            entity.HasKey(e => e.Id);
            entity.UseNullEngine();
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestNullEntity))!;

        Assert.Equal("Null", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
    }

    [Fact]
    public void CreateTable_WithNullEngine_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events_raw",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Timestamp", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Data", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Null");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"events_raw\"", sql);
        Assert.Contains("ENGINE = Null", sql);
        // Null engine should NOT have ORDER BY
        Assert.DoesNotContain("ORDER BY", sql);
        // Null engine should NOT have parentheses
        Assert.DoesNotContain("Null()", sql);
    }

    [Fact]
    public void CreateTable_WithNullEngine_NoOrderByGenerated()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "discard_table",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(int), ColumnType = "Int32" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "Id" } }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Null");

        var sql = GenerateSql(generator, operation);

        // Even with a PK defined, Null engine should not generate ORDER BY
        Assert.Contains("ENGINE = Null", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Fact]
    public void NullEngine_DoesNotRequireOrderBy()
    {
        // Unlike MergeTree, Null engine should not throw when ORDER BY is not specified
        var builder = new ModelBuilder();

        builder.Entity<TestNullEntity>(entity =>
        {
            entity.ToTable("null_table");
            entity.HasKey(e => e.Id);
            entity.UseNullEngine();
            // No ORDER BY columns specified - should be valid for Null engine
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestNullEntity))!;

        Assert.Equal("Null", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Null(entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Fact]
    public void NullEngine_GenericOverload_Works()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestNullEntity>(entity =>
        {
            entity.ToTable("generic_null");
            entity.HasKey(e => e.Id);
            entity.UseNullEngine(); // Using the generic overload
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestNullEntity))!;

        Assert.Equal("Null", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new TestDbContext(options);
    }

    private static IMigrationsSqlGenerator GetMigrationsSqlGenerator(DbContext context)
    {
        return ((IInfrastructure<IServiceProvider>)context).Instance.GetService<IMigrationsSqlGenerator>()!;
    }

    private static string GenerateSql(IMigrationsSqlGenerator generator, MigrationOperation operation)
    {
        var commands = generator.Generate(new[] { operation });
        return string.Join("\n", commands.Select(c => c.CommandText));
    }
}

public class TestNullEntity
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Data { get; set; } = string.Empty;
}
