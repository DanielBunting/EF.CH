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
/// Tests for replicated MergeTree engine DDL generation. After the engine-API
/// consolidation, replication is a property of the engine: <c>WithReplication</c>
/// sets the <c>IsReplicated</c> annotation, and the SQL generator prepends the
/// <c>Replicated</c> prefix and ZK args at emit time. The <c>Engine</c>
/// annotation always holds the base name.
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

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
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

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
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

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Date" });
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplicatedSummingMergeTree", sql);
        Assert.Contains("ORDER BY (\"Date\")", sql);
    }

    [Fact]
    public void ModelBuilder_UseMergeTree_WithReplication_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/tables/{uuid}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "EventTime", "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseReplacingMergeTree_WithVersion_WithReplication_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedVersionedEntity>(entity =>
        {
            entity.ToTable("replicated_versioned");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree(x => x.Id)
                  .WithVersion(x => x.Version)
                  .WithReplication("/clickhouse/tables/{uuid}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedVersionedEntity))!;

        Assert.Equal("ReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseSummingMergeTree_WithReplication_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedStatsEntity>(entity =>
        {
            entity.ToTable("replicated_stats");
            entity.HasNoKey();
            entity.UseSummingMergeTree(x => x.Date)
                  .WithReplication("/clickhouse/tables/{uuid}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedStatsEntity))!;

        Assert.Equal("SummingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "Date" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseAggregatingMergeTree_WithReplication_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedStatsEntity>(entity =>
        {
            entity.ToTable("replicated_agg");
            entity.HasNoKey();
            entity.UseAggregatingMergeTree(x => x.Date)
                  .WithReplication("/clickhouse/tables/{uuid}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedStatsEntity))!;

        Assert.Equal("AggregatingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
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
            entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/tables/{uuid}")
                  .WithCluster("geo_cluster");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
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
            entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/geo/{database}/{table}", "{replica}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
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
            entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/tables/{uuid}")
                  .WithTableGroup("Core");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
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
            entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithCluster("geo_cluster")
                  .WithReplication("/clickhouse/geo/{database}/{table}")
                  .WithTableGroup("Core");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "EventTime", "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("/clickhouse/geo/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
        Assert.Equal("{replica}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicaName)?.Value);
        Assert.Equal("Core", entityType.FindAnnotation(ClickHouseAnnotationNames.TableGroup)?.Value);
    }

    [Fact]
    public void FluentChain_AndMethod_ReturnsEntityTypeBuilder()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/tables/{uuid}")
                  .WithCluster("geo_cluster")
                  .And()
                  .HasPartitionBy(x => x.EventTime, PartitionGranularity.Month);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
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
            EntityTypeBuilder<TestReplicatedEntity> etb = entity.UseMergeTree(x => new { x.EventTime, x.Id })
                  .WithReplication("/clickhouse/tables/{uuid}")
                  .WithCluster("geo_cluster");
            etb.HasPartitionBy(x => x.EventTime, PartitionGranularity.Month);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
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
            entity.UseReplacingMergeTree(x => x.Id)
                  .WithVersion(x => x.Version)
                  .WithCluster("geo_cluster")
                  .WithReplication("/clickhouse/geo/{database}/{table}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedVersionedEntity))!;

        Assert.Equal("ReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
        Assert.Equal("/clickhouse/geo/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
    }

    #endregion

    #region Engine-specific knobs combined with replication
    // These chains were previously impossible: legacy UseReplicatedReplacingMergeTree
    // returned ReplicatedEngineBuilder which lacked WithIsDeleted, WithSign, etc. After
    // consolidation, every MergeTree builder gets WithReplication for free.

    [Fact]
    public void ReplacingMergeTree_WithVersion_WithIsDeleted_WithReplication_GeneratesCorrectDdl()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestReplacingDeletedEntity>(entity =>
        {
            entity.ToTable("replicated_replacing");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree(x => x.Id)
                  .WithVersion(x => x.Version)
                  .WithIsDeleted(x => x.IsDeleted)
                  .WithReplication("/clickhouse/tables/{uuid}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplacingDeletedEntity))!;

        Assert.Equal("ReplacingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal("IsDeleted", entityType.FindAnnotation(ClickHouseAnnotationNames.IsDeletedColumn)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);

        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation { Name = "replicated_replacing" };
        operation.Columns.Add(new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" });
        operation.Columns.Add(new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" });
        operation.Columns.Add(new AddColumnOperation { Name = "IsDeleted", ClrType = typeof(byte), ColumnType = "UInt8" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "Version");
        operation.AddAnnotation(ClickHouseAnnotationNames.IsDeletedColumn, "IsDeleted");
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);
        Assert.Contains("ENGINE = ReplicatedReplacingMergeTree", sql);
        Assert.Contains("\"Version\"", sql);
        Assert.Contains("\"IsDeleted\"", sql);
    }

    [Fact]
    public void CollapsingMergeTree_WithSign_WithReplication_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation { Name = "replicated_collapsing" };
        operation.Columns.Add(new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" });
        operation.Columns.Add(new AddColumnOperation { Name = "Sign", ClrType = typeof(sbyte), ColumnType = "Int8" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.SignColumn, "Sign");
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplicatedCollapsingMergeTree", sql);
        Assert.Contains("\"Sign\"", sql);
    }

    [Fact]
    public void VersionedCollapsingMergeTree_WithSignAndVersion_WithReplication_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation { Name = "replicated_vcollapsing" };
        operation.Columns.Add(new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" });
        operation.Columns.Add(new AddColumnOperation { Name = "Sign", ClrType = typeof(sbyte), ColumnType = "Int8" });
        operation.Columns.Add(new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "VersionedCollapsingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.SignColumn, "Sign");
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "Version");
        operation.AddAnnotation(ClickHouseAnnotationNames.IsReplicated, true);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = ReplicatedVersionedCollapsingMergeTree", sql);
        Assert.Contains("\"Sign\"", sql);
        Assert.Contains("\"Version\"", sql);
    }

    [Fact]
    public void MergeTree_WithReplication_WithClusterMacro_GeneratesCorrectDdl()
    {
        // Regression guard for the new MergeTreeBuilder<T> + the no-arg WithCluster() macro form.
        var builder = new ModelBuilder();

        builder.Entity<TestReplicatedEntity>(entity =>
        {
            entity.ToTable("macro_cluster");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id)
                  .WithReplication("/clickhouse/tables/{uuid}")
                  .WithCluster();
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestReplicatedEntity))!;

        Assert.Equal("MergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsReplicated)?.Value);
        Assert.Equal(ClickHouseClusterMacros.Cluster, entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
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

public class TestReplacingDeletedEntity
{
    public Guid Id { get; set; }
    public long Version { get; set; }
    public byte IsDeleted { get; set; }
}
