using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using EF.CH.Tests.Sql;
using Xunit;

namespace EF.CH.Tests.Engines;

public class SimpleEngineTests
{
    [Theory]
    [InlineData("Memory")]
    [InlineData("Log")]
    [InlineData("TinyLog")]
    [InlineData("StripeLog")]
    public void UseEngine_SetsEngineAnnotation(string engineName)
    {
        var builder = new ModelBuilder();

        builder.Entity<TestSimpleEngineEntity>(entity =>
        {
            entity.ToTable("test_table");
            entity.HasKey(e => e.Id);
            ApplyEngine(entity, engineName);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestSimpleEngineEntity))!;

        Assert.Equal(engineName, entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
    }

    [Theory]
    [InlineData("Memory")]
    [InlineData("Log")]
    [InlineData("TinyLog")]
    [InlineData("StripeLog")]
    public void CreateTable_GeneratesCorrectDdl(string engineName)
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, engineName);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains($"ENGINE = {engineName}", sql);
        Assert.DoesNotContain("ORDER BY", sql);
        Assert.DoesNotContain($"{engineName}()", sql);
    }

    [Theory]
    [InlineData("Memory")]
    [InlineData("Log")]
    [InlineData("TinyLog")]
    [InlineData("StripeLog")]
    public void CreateTable_NoOrderByEvenWithPrimaryKey(string engineName)
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(int), ColumnType = "Int32" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "Id" } }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, engineName);

        var sql = GenerateSql(generator, operation);

        Assert.Contains($"ENGINE = {engineName}", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Theory]
    [InlineData("Memory")]
    [InlineData("Log")]
    [InlineData("TinyLog")]
    [InlineData("StripeLog")]
    public void Engine_DoesNotRequireOrderBy(string engineName)
    {
        var builder = new ModelBuilder();

        builder.Entity<TestSimpleEngineEntity>(entity =>
        {
            entity.ToTable("test_table");
            entity.HasKey(e => e.Id);
            ApplyEngine(entity, engineName);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestSimpleEngineEntity))!;

        Assert.Equal(engineName, entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Null(entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Theory]
    [InlineData("Memory")]
    [InlineData("Log")]
    [InlineData("TinyLog")]
    [InlineData("StripeLog")]
    public void GenericOverload_Works(string engineName)
    {
        var builder = new ModelBuilder();

        builder.Entity<TestSimpleEngineEntity>(entity =>
        {
            entity.ToTable("test_table");
            entity.HasKey(e => e.Id);
            ApplyGenericEngine(entity, engineName);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestSimpleEngineEntity))!;

        Assert.Equal(engineName, entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
    }

    private static void ApplyEngine(EntityTypeBuilder builder, string engineName) => _ = engineName switch
    {
        "Memory" => builder.UseMemoryEngine(),
        "Log" => builder.UseLogEngine(),
        "TinyLog" => builder.UseTinyLogEngine(),
        "StripeLog" => builder.UseStripeLogEngine(),
        _ => throw new ArgumentException($"Unknown engine: {engineName}")
    };

    private static void ApplyGenericEngine(EntityTypeBuilder<TestSimpleEngineEntity> builder, string engineName) => _ = engineName switch
    {
        "Memory" => builder.UseMemoryEngine(),
        "Log" => builder.UseLogEngine(),
        "TinyLog" => builder.UseTinyLogEngine(),
        "StripeLog" => builder.UseStripeLogEngine(),
        _ => throw new ArgumentException($"Unknown engine: {engineName}")
    };

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

public class TestSimpleEngineEntity
{
    public Guid Id { get; set; }
    public string Value { get; set; } = string.Empty;
}
