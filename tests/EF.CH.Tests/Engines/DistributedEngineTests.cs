using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using EF.CH.Tests.Sql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Engines;

public class DistributedEngineTests
{
    [Fact]
    public void CreateTable_WithDistributed_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events_distributed",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "EventTime", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Distributed");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedCluster, "my_cluster");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedDatabase, "default");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedTable, "events_local");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"events_distributed\"", sql);
        Assert.Contains("ENGINE = Distributed('my_cluster', 'default', 'events_local')", sql);
        Assert.DoesNotContain("ORDER BY", sql);
    }

    [Fact]
    public void CreateTable_WithDistributed_CurrentDatabase_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events_distributed",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Distributed");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedCluster, "my_cluster");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedDatabase, "currentDatabase()");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedTable, "events_local");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = Distributed('my_cluster', currentDatabase(), 'events_local')", sql);
    }

    [Fact]
    public void CreateTable_WithDistributed_ShardingKey_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events_distributed",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Distributed");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedCluster, "my_cluster");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedDatabase, "default");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedTable, "events_local");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedShardingKey, "UserId");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = Distributed('my_cluster', 'default', 'events_local', UserId)", sql);
    }

    [Fact]
    public void CreateTable_WithDistributed_ShardingKeyExpression_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events_distributed",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Distributed");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedCluster, "my_cluster");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedDatabase, "default");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedTable, "events_local");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedShardingKey, "cityHash64(UserId)");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = Distributed('my_cluster', 'default', 'events_local', cityHash64(UserId))", sql);
    }

    [Fact]
    public void CreateTable_WithDistributed_ShardingKeyAndPolicy_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events_distributed",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "Distributed");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedCluster, "my_cluster");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedDatabase, "default");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedTable, "events_local");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedShardingKey, "UserId");
        operation.AddAnnotation(ClickHouseAnnotationNames.DistributedPolicyName, "ssd_policy");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = Distributed('my_cluster', 'default', 'events_local', UserId, 'ssd_policy')", sql);
    }

    [Fact]
    public void ModelBuilder_UseDistributed_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events_distributed");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("my_cluster", "default", "events_local");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal("Distributed", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("my_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedCluster)?.Value);
        Assert.Equal("default", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedDatabase)?.Value);
        Assert.Equal("events_local", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedTable)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseDistributed_TwoParams_UsesCurrentDatabase()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events_distributed");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("my_cluster", "events_local");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal("Distributed", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("my_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedCluster)?.Value);
        Assert.Equal("currentDatabase()", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedDatabase)?.Value);
        Assert.Equal("events_local", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedTable)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseDistributed_WithShardingKey_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events_distributed");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("my_cluster", "events_local")
                  .WithShardingKey(x => x.UserId);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal("Distributed", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("UserId", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedShardingKey)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseDistributed_WithRawShardingKey_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events_distributed");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("my_cluster", "events_local")
                  .WithShardingKey("cityHash64(UserId)");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal("cityHash64(UserId)", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedShardingKey)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseDistributed_WithPolicy_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events_distributed");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("my_cluster", "events_local")
                  .WithShardingKey(x => x.UserId)
                  .WithPolicy("ssd_policy");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal("ssd_policy", entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedPolicyName)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseDistributed_FluentChaining_Works()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            // Use And() to chain back to EntityTypeBuilder
            entity.UseDistributed("my_cluster", "default", "events_local")
                  .WithShardingKey(x => x.UserId)
                  .And()
                  .HasComment("Distributed events table");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal("Distributed", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Distributed events table", entityType.GetComment());
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

public class TestDistributedEntity
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public long UserId { get; set; }
}
