using EF.CH.Configuration;
using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// Tests for table group configuration and cluster DDL generation.
/// </summary>
public class TableGroupTests
{
    [Fact]
    public void ModelBuilder_UseTableGroup_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestClusterEntity>(entity =>
        {
            entity.ToTable("clustered_events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
            entity.UseTableGroup("Core");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestClusterEntity))!;

        Assert.Equal("Core", entityType.FindAnnotation(ClickHouseAnnotationNames.TableGroup)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseCluster_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestClusterEntity>(entity =>
        {
            entity.ToTable("clustered_events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
            entity.UseCluster("geo_cluster");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestClusterEntity))!;

        Assert.Equal("geo_cluster", entityType.FindAnnotation(ClickHouseAnnotationNames.EntityClusterName)?.Value);
    }

    [Fact]
    public void ModelBuilder_IsLocalOnly_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestClusterEntity>(entity =>
        {
            entity.ToTable("local_cache");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
            entity.IsLocalOnly();
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestClusterEntity))!;

        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.IsLocalOnly)?.Value);
    }

    [Fact]
    public void ModelBuilder_HasReplication_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestClusterEntity>(entity =>
        {
            entity.ToTable("replicated_events");
            entity.HasKey(e => e.Id);
            entity.UseReplicatedMergeTree(x => x.Id);
            entity.HasReplication("/clickhouse/tables/{database}/{table}", "{replica}");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestClusterEntity))!;

        Assert.Equal("/clickhouse/tables/{database}/{table}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicatedPath)?.Value);
        Assert.Equal("{replica}", entityType.FindAnnotation(ClickHouseAnnotationNames.ReplicaName)?.Value);
    }

    [Fact]
    public void CreateTable_WithCluster_GeneratesOnClusterClause()
    {
        var options = new DbContextOptionsBuilder<TestClusterDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o => o.UseCluster("geo_cluster"))
            .Options;

        using var context = new TestClusterDbContext(options);
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Data", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE IF NOT EXISTS \"events\" ON CLUSTER \"geo_cluster\"", sql);
    }

    [Fact]
    public void CreateTable_LocalOnly_DoesNotGenerateOnClusterClause()
    {
        var options = new DbContextOptionsBuilder<TestClusterDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o => o.UseCluster("geo_cluster"))
            .Options;

        using var context = new TestClusterDbContext(options);
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "local_cache",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.IsLocalOnly, true);

        var sql = GenerateSql(generator, operation);

        Assert.DoesNotContain("ON CLUSTER", sql);
    }

    [Fact]
    public void DropTable_WithCluster_GeneratesOnClusterClause()
    {
        var options = new DbContextOptionsBuilder<TestClusterDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o => o.UseCluster("geo_cluster"))
            .Options;

        using var context = new TestClusterDbContext(options);
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new DropTableOperation { Name = "events" };

        var sql = GenerateSql(generator, operation);

        Assert.Contains("DROP TABLE IF EXISTS \"events\" ON CLUSTER \"geo_cluster\"", sql);
    }

    [Fact]
    public void Configuration_Binding_WorksCorrectly()
    {
        var config = new ClickHouseConfiguration
        {
            Connections =
            {
                ["Primary"] = new ConnectionConfig
                {
                    Database = "production",
                    WriteEndpoint = "dc1:8123",
                    ReadEndpoints = { "dc2:8123", "dc1:8123" },
                    ReadStrategy = ReadStrategy.PreferFirst
                }
            },
            Clusters =
            {
                ["geo_cluster"] = new ClusterConfig
                {
                    Connection = "Primary",
                    Replication = new ReplicationConfig
                    {
                        ZooKeeperBasePath = "/clickhouse/geo/{database}"
                    }
                }
            },
            TableGroups =
            {
                ["Core"] = new TableGroupConfig
                {
                    Cluster = "geo_cluster",
                    Replicated = true
                },
                ["LocalCache"] = new TableGroupConfig
                {
                    Cluster = null,
                    Replicated = false
                }
            },
            Defaults = new DefaultsConfig
            {
                TableGroup = "Core"
            }
        };

        Assert.Equal("production", config.Connections["Primary"].Database);
        Assert.Equal("dc1:8123", config.Connections["Primary"].WriteEndpoint);
        Assert.Equal(2, config.Connections["Primary"].ReadEndpoints.Count);
        Assert.Equal("Primary", config.Clusters["geo_cluster"].Connection);
        Assert.Equal("/clickhouse/geo/{database}", config.Clusters["geo_cluster"].Replication.ZooKeeperBasePath);
        Assert.Equal("geo_cluster", config.TableGroups["Core"].Cluster);
        Assert.True(config.TableGroups["Core"].Replicated);
        Assert.Null(config.TableGroups["LocalCache"].Cluster);
        Assert.False(config.TableGroups["LocalCache"].Replicated);
        Assert.Equal("Core", config.Defaults.TableGroup);
    }

    [Fact]
    public void FluentBuilder_AddConnection_WorksCorrectly()
    {
        var options = new DbContextOptionsBuilder<TestClusterDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o => o
                .AddConnection("Primary", conn => conn
                    .Database("production")
                    .WriteEndpoint("dc1:8123")
                    .ReadEndpoints("dc2:8123", "dc1:8123")
                    .ReadStrategy(ReadStrategy.RoundRobin)
                    .WithFailover(f => f.MaxRetries(5))))
            .Options;

        var extension = options.FindExtension<EF.CH.Infrastructure.ClickHouseOptionsExtension>();
        Assert.NotNull(extension?.Configuration);
        Assert.True(extension.Configuration.Connections.ContainsKey("Primary"));
        Assert.Equal("production", extension.Configuration.Connections["Primary"].Database);
        Assert.Equal("dc1:8123", extension.Configuration.Connections["Primary"].WriteEndpoint);
        Assert.Equal(2, extension.Configuration.Connections["Primary"].ReadEndpoints.Count);
        Assert.Equal(ReadStrategy.RoundRobin, extension.Configuration.Connections["Primary"].ReadStrategy);
        Assert.Equal(5, extension.Configuration.Connections["Primary"].Failover.MaxRetries);
    }

    [Fact]
    public void FluentBuilder_AddCluster_WorksCorrectly()
    {
        var options = new DbContextOptionsBuilder<TestClusterDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o => o
                .AddCluster("geo_cluster", cluster => cluster
                    .UseConnection("Primary")
                    .WithReplication(r => r
                        .ZooKeeperBasePath("/clickhouse/geo/{database}")
                        .ReplicaNameMacro("{replica}"))))
            .Options;

        var extension = options.FindExtension<EF.CH.Infrastructure.ClickHouseOptionsExtension>();
        Assert.NotNull(extension?.Configuration);
        Assert.True(extension.Configuration.Clusters.ContainsKey("geo_cluster"));
        Assert.Equal("Primary", extension.Configuration.Clusters["geo_cluster"].Connection);
        Assert.Equal("/clickhouse/geo/{database}", extension.Configuration.Clusters["geo_cluster"].Replication.ZooKeeperBasePath);
    }

    [Fact]
    public void FluentBuilder_AddTableGroup_WorksCorrectly()
    {
        var options = new DbContextOptionsBuilder<TestClusterDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o => o
                .AddTableGroup("Core", group => group
                    .UseCluster("geo_cluster")
                    .Replicated()
                    .Description("Core business entities"))
                .AddTableGroup("LocalCache", group => group
                    .NoCluster()
                    .NotReplicated())
                .DefaultTableGroup("Core"))
            .Options;

        var extension = options.FindExtension<EF.CH.Infrastructure.ClickHouseOptionsExtension>();
        Assert.NotNull(extension?.Configuration);
        Assert.True(extension.Configuration.TableGroups.ContainsKey("Core"));
        Assert.Equal("geo_cluster", extension.Configuration.TableGroups["Core"].Cluster);
        Assert.True(extension.Configuration.TableGroups["Core"].Replicated);
        Assert.Null(extension.Configuration.TableGroups["LocalCache"].Cluster);
        Assert.False(extension.Configuration.TableGroups["LocalCache"].Replicated);
        Assert.Equal("Core", extension.Configuration.Defaults.TableGroup);
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

public class TestClusterDbContext : DbContext
{
    public TestClusterDbContext(DbContextOptions<TestClusterDbContext> options) : base(options) { }
}

public class TestClusterEntity
{
    public Guid Id { get; set; }
    public string Data { get; set; } = string.Empty;
}
