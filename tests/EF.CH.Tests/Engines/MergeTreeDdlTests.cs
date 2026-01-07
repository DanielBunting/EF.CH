using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using EF.CH.Tests.Sql;
using Xunit;

namespace EF.CH.Tests.Engines;

public class MergeTreeDdlTests
{
    [Fact]
    public void CreateTable_WithMergeTree_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "EventTime", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "EventType", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Data", ClrType = typeof(string), ColumnType = "String", IsNullable = true }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "EventTime", "Id" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"events\"", sql);
        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("ORDER BY (\"EventTime\", \"Id\")", sql);
    }

    [Fact]
    public void CreateTable_WithPartitionBy_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "logs",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Timestamp", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Message", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Timestamp" });
        operation.AddAnnotation(ClickHouseAnnotationNames.PartitionBy, "toYYYYMM(Timestamp)");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("PARTITION BY toYYYYMM(Timestamp)", sql);
        Assert.Contains("ORDER BY (\"Timestamp\")", sql);
    }

    [Fact]
    public void CreateTable_WithReplacingMergeTree_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "users",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "Version");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplacingMergeTree(\"Version\")", sql);
        Assert.Contains("ORDER BY (\"Id\")", sql);
    }

    [Fact]
    public void CreateTable_WithTtl_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "temp_data",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "CreatedAt", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Data", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "CreatedAt" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Ttl, "CreatedAt + INTERVAL 30 DAY");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("TTL CreatedAt + INTERVAL 30 DAY", sql);
    }

    [Fact]
    public void CreateTable_WithNullableColumn_WrapsWithNullable()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "test",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "NullableField", ClrType = typeof(string), ColumnType = "String", IsNullable = true }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("Nullable(String)", sql);
    }

    [Fact]
    public void CreateTable_DefaultsToMergeTree_WhenNoEngineSpecified()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "simple",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "Id" } }
        };

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("ORDER BY (\"Id\")", sql); // Uses PK for ORDER BY
    }

    [Fact]
    public void DropTable_GeneratesIfExists()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new DropTableOperation { Name = "test_table" };

        var sql = GenerateSql(generator, operation);

        Assert.Contains("DROP TABLE IF EXISTS", sql);
        Assert.Contains("\"test_table\"", sql);
    }

    [Fact]
    public void ModelBuilder_UseMergeTree_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestMergeTreeEntity>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("EventTime", "Id");
            entity.HasPartitionBy("toYYYYMM(EventTime)");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestMergeTreeEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "EventTime", "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal("toYYYYMM(EventTime)", entityType.FindAnnotation(ClickHouseAnnotationNames.PartitionBy)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseReplacingMergeTree_SetsVersionColumn()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestMergeTreeEntity>(entity =>
        {
            entity.ToTable("versioned");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree("Version", "Id");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestMergeTreeEntity))!;

        Assert.Equal("ReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseReplacingMergeTree_WithIsDeleted_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplacingWithDeleteEntity>(entity =>
        {
            entity.ToTable("deletable");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree(
                x => x.Version,
                x => x.IsDeleted,
                x => x.Id);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplacingWithDeleteEntity))!;

        Assert.Equal("ReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal("IsDeleted", entityType.FindAnnotation(ClickHouseAnnotationNames.IsDeletedColumn)?.Value);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Fact]
    public void CreateTable_WithReplacingMergeTree_AndIsDeleted_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "deletable_users",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" },
                new AddColumnOperation { Name = "IsDeleted", ClrType = typeof(byte), ColumnType = "UInt8" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "Version");
        operation.AddAnnotation(ClickHouseAnnotationNames.IsDeletedColumn, "IsDeleted");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplacingMergeTree(\"Version\", \"IsDeleted\")", sql);
        Assert.Contains("ORDER BY (\"Id\")", sql);
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

public class TestMergeTreeEntity
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string EventType { get; set; } = string.Empty;
    public long Version { get; set; }
}

public class TestReplacingWithDeleteEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public long Version { get; set; }
    public byte IsDeleted { get; set; }  // UInt8: 0 = active, 1 = deleted
}
