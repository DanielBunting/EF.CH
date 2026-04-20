using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using EF.CH.Tests.Sql;
using Xunit;

namespace EF.CH.Tests.Engines;

public class KeeperMapDdlTests
{
    [Fact]
    public void CreateTable_WithKeeperMap_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "kv",
            Columns =
            {
                new AddColumnOperation { Name = "Key", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "KeeperMap");
        operation.AddAnnotation(ClickHouseAnnotationNames.KeeperMapRootPath, "/efch/test");
        operation.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "Key" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("ENGINE = KeeperMap('/efch/test')", sql);
        Assert.Contains("PRIMARY KEY (\"Key\")", sql);
        Assert.DoesNotContain("ORDER BY", sql);
        Assert.DoesNotContain("PARTITION BY", sql);
        Assert.DoesNotContain("TTL", sql);
    }

    [Fact]
    public void CreateTable_WithKeeperMap_KeysLimit_EmitsSecondArgument()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "kv",
            Columns =
            {
                new AddColumnOperation { Name = "Key", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "KeeperMap");
        operation.AddAnnotation(ClickHouseAnnotationNames.KeeperMapRootPath, "/efch/test");
        operation.AddAnnotation(ClickHouseAnnotationNames.KeeperMapKeysLimit, (ulong)1000);
        operation.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "Key" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = KeeperMap('/efch/test', 1000)", sql);
    }

    [Fact]
    public void CreateTable_WithKeeperMap_IgnoresStrayOrderByAndTtl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "kv",
            Columns =
            {
                new AddColumnOperation { Name = "Key", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "KeeperMap");
        operation.AddAnnotation(ClickHouseAnnotationNames.KeeperMapRootPath, "/efch/test");
        operation.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "Key" });
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Key" });
        operation.AddAnnotation(ClickHouseAnnotationNames.PartitionBy, "toYYYYMM(CreatedAt)");
        operation.AddAnnotation(ClickHouseAnnotationNames.Ttl, "CreatedAt + INTERVAL 30 DAY");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = KeeperMap('/efch/test')", sql);
        Assert.Contains("PRIMARY KEY (\"Key\")", sql);
        Assert.DoesNotContain("ORDER BY", sql);
        Assert.DoesNotContain("PARTITION BY", sql);
        Assert.DoesNotContain("TTL", sql);
    }

    [Fact]
    public void CreateTable_WithKeeperMap_EscapesSingleQuotesInRootPath()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "kv",
            Columns =
            {
                new AddColumnOperation { Name = "Key", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "KeeperMap");
        operation.AddAnnotation(ClickHouseAnnotationNames.KeeperMapRootPath, "/efch/it's/weird");
        operation.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "Key" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = KeeperMap('/efch/it''s/weird')", sql);
    }

    [Fact]
    public void ModelBuilder_UseKeeperMapEngine_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestKeeperMapEntity>(entity =>
        {
            entity.ToTable("kv");
            entity.HasKey(e => e.Key);
            entity.UseKeeperMapEngine("/efch/test", x => x.Key);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestKeeperMapEntity))!;

        Assert.Equal("KeeperMap", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("/efch/test", entityType.FindAnnotation(ClickHouseAnnotationNames.KeeperMapRootPath)?.Value);
        Assert.Equal(new[] { "Key" }, entityType.FindAnnotation(ClickHouseAnnotationNames.PrimaryKey)?.Value);
        Assert.Null(entityType.FindAnnotation(ClickHouseAnnotationNames.KeeperMapKeysLimit)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseKeeperMapEngine_WithKeysLimit_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestKeeperMapEntity>(entity =>
        {
            entity.ToTable("kv");
            entity.HasKey(e => e.Key);
            entity.UseKeeperMapEngine("/efch/test", x => x.Key, keysLimit: 5000);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestKeeperMapEntity))!;

        Assert.Equal((ulong)5000, entityType.FindAnnotation(ClickHouseAnnotationNames.KeeperMapKeysLimit)?.Value);
    }

    [Fact]
    public void UseKeeperMapEngine_RejectsEmptyRootPath()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentException>(() =>
            builder.Entity<TestKeeperMapEntity>(entity =>
            {
                entity.UseKeeperMapEngine("", "Key");
            }));
    }

    [Fact]
    public void UseKeeperMapEngine_RejectsEmptyPrimaryKey()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentException>(() =>
            builder.Entity<TestKeeperMapEntity>(entity =>
            {
                entity.UseKeeperMapEngine("/efch/test", "");
            }));
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

public class TestKeeperMapEntity
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}
