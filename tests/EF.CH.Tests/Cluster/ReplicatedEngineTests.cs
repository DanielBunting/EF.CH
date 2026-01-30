using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// Tests for replicated MergeTree engine DDL generation.
/// </summary>
public class ReplicatedEngineTests
{
    [Fact]
    public void CreateTable_WithReplicatedMergeTree_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "replicated_events",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "EventTime", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "EventType", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "EventTime", "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"replicated_events\"", sql);
        Assert.Contains("ENGINE = ReplicatedMergeTree", sql);
        Assert.Contains("ORDER BY (\"EventTime\", \"Id\")", sql);
    }

    [Fact]
    public void CreateTable_WithReplicatedReplacingMergeTree_AndVersion_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "replicated_users",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedReplacingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "Version");
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplicatedReplacingMergeTree", sql);
        Assert.Contains("\"Version\"", sql);
        Assert.Contains("ORDER BY (\"Id\")", sql);
    }

    [Fact]
    public void CreateTable_WithReplicatedSummingMergeTree_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "replicated_stats",
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" },
                new AddColumnOperation { Name = "Count", ClrType = typeof(long), ColumnType = "Int64" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplicatedSummingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Date" });
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplicatedSummingMergeTree", sql);
        Assert.Contains("ORDER BY (\"Date\")", sql);
    }

    [Fact]
    public void ModelBuilder_UseReplicatedMergeTree_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id });
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "EventTime", "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseReplicatedReplacingMergeTree_WithVersion_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedVersionedEntity>(entity =>
        {
            entity.ToTable("replicated_versioned");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedVersionedEntity))!;

        Assert.Equal("ReplicatedReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseReplicatedSummingMergeTree_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedStatsEntity>(entity =>
        {
            entity.ToTable("replicated_stats");
            entity.HasNoKey();
            entity.UseReplicatedSummingMergeTree(x => x.Date);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedStatsEntity))!;

        Assert.Equal("ReplicatedSummingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "Date" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseReplicatedAggregatingMergeTree_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedStatsEntity>(entity =>
        {
            entity.ToTable("replicated_agg");
            entity.HasNoKey();
            entity.UseReplicatedAggregatingMergeTree(x => x.Date);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedStatsEntity))!;

        Assert.Equal("ReplicatedAggregatingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    #region Fluent Chain Pattern Tests

    [Fact]
    public void FluentChain_WithCluster_SetsClusterAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithCluster("geo_cluster");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
    }

    [Fact]
    public void FluentChain_WithReplication_SetsReplicationAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/geo/{database}/{table}", "{replica}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("/clickhouse/geo/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
        Assert.Equal("{replica}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicaName)?.Value);
    }

    [Fact]
    public void FluentChain_WithTableGroup_SetsTableGroupAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithTableGroup("Core");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Core", entityType.FindAnnotation(ClickHouseAnnotationNames.TableGroup)?.Value);
    }

    [Fact]
    public void FluentChain_FullConfiguration_SetsAllAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithCluster("geo_cluster")
                  .WithReplication("/clickhouse/geo/{database}/{table}")
                  .WithTableGroup("Core");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "EventTime", "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("/clickhouse/geo/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
        Assert.Equal("{replica}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicaName)?.Value);
        Assert.Equal("Core", entityType.FindAnnotation(ClickHouseAnnotationNames.TableGroup)?.Value);
    }

    [Fact]
    public void FluentChain_ImplicitConversion_AllowsChainingViaAnd()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            // Use And() to explicitly get EntityTypeBuilder for continued chaining
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithCluster("geo_cluster")
                  .And()
                  .HasPartitionByMonth(x => x.EventTime);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("toYYYYMM(\"EventTime\")", entityType.FindAnnotation(ClickHouseAnnotationNames.PartitionBy)?.Value);
    }

    [Fact]
    public void FluentChain_ImplicitConversion_WorksWithAssignment()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            // Implicit conversion works when the result is used in a context expecting EntityTypeBuilder
            EntityTypeBuilder<TestReplicatedEntity> etb = entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithCluster("geo_cluster");
            etb.HasPartitionByMonth(x => x.EventTime);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("toYYYYMM(\"EventTime\")", entityType.FindAnnotation(ClickHouseAnnotationNames.PartitionBy)?.Value);
    }

    [Fact]
    public void FluentChain_AndMethod_ReturnsEntityTypeBuilder()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id })
                  .WithCluster("geo_cluster")
                  .And()  // Explicit conversion back
                  .HasPartitionByMonth(x => x.EventTime);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("toYYYYMM(\"EventTime\")", entityType.FindAnnotation(ClickHouseAnnotationNames.PartitionBy)?.Value);
    }

    [Fact]
    public void FluentChain_ReplicatedReplacingMergeTree_WorksWithFluent()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedVersionedEntity>(entity =>
        {
            entity.ToTable("replicated_versioned");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedReplacingMergeTree(x => x.Version, x => x.Id)
                  .WithCluster("geo_cluster")
                  .WithReplication("/clickhouse/geo/{database}/{table}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedVersionedEntity))!;

        Assert.Equal("ReplicatedReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("/clickhouse/geo/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
    }

    [Fact]
    public void FluentChain_BackwardCompatibility_SeparateCalls_StillWork()
    {
        // Ensure the old pattern (separate calls) still works
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => new { x.EventTime, x.Id });
            entity.UseCluster("geo_cluster");
            entity.HasReplication("/clickhouse/geo/{database}/{table}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("ReplicatedMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("/clickhouse/geo/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
    }

    #endregion

    private static TestReplicatedDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestReplicatedDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new TestReplicatedDbContext(options);
    }

    private static IMigrationsSqlGenerator GetMigrationsSqlGenerator(DbContext context)
    {
        return ((IInfrastructure<IServiceProvider>)context).Instance.GetRequiredService<IMigrationsSqlGenerator>();
    }

    private static string GenerateSql(IMigrationsSqlGenerator generator, MigrationOperation operation)
    {
        var commands = generator.Generate([operation]);
        return string.Join("\n", commands.Select(c => c.CommandText));
    }
}

public class TestReplicatedDbContext : DbContext
{
    public TestReplicatedDbContext(DbContextOptions<TestReplicatedDbContext> options) : base(options) { }
}

public class TestReplicatedEntity
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string EventType { get; set; } = string.Empty;
}

public class TestReplicatedVersionedEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public long Version { get; set; }
}

public class TestReplicatedStatsEntity
{
    public DateTime Date { get; set; }
    public long Count { get; set; }
}
