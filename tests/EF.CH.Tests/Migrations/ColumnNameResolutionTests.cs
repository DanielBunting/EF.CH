using EF.CH.Design.Internal;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Tests that property names are correctly resolved to column names in migration
/// SQL generation and annotation code generation when custom column mappings exist.
/// </summary>
public class ColumnNameResolutionTests
{
    #region SQL Generation Tests

    [Fact]
    public void OrderBy_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.TimeseriesId).HasColumnName("timeseries_id");
                entity.Property(e => e.PeriodStart).HasColumnName("period_start");
                entity.UseMergeTree(x => new { x.TimeseriesId, x.PeriodStart });
            });
        });

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "timeseries_id", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "period_start", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
            }
        };

        // Annotations store property names (the bug scenario)
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "TimeseriesId", "PeriodStart" });

        var sql = GenerateSql(operation, model);

        Assert.Contains("ORDER BY (\"timeseries_id\", \"period_start\")", sql);
        Assert.DoesNotContain("TimeseriesId", sql);
        Assert.DoesNotContain("PeriodStart", sql);
    }

    [Fact]
    public void PartitionBy_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.PeriodStart).HasColumnName("period_start");
                entity.UseMergeTree(x => x.Id);
                entity.HasPartitionByMonth(x => x.PeriodStart);
            });
        });

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "period_start", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.PartitionBy, "toYYYYMM(\"PeriodStart\")");

        var sql = GenerateSql(operation, model);

        Assert.Contains("PARTITION BY toYYYYMM(\"period_start\")", sql);
        Assert.DoesNotContain("PeriodStart", sql);
    }

    [Fact]
    public void ReplacingMergeTree_VersionColumn_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.TimeseriesId).HasColumnName("timeseries_id");
                entity.Property(e => e.IngestOrder).HasColumnName("ingest_order");
                entity.UseReplacingMergeTree(x => x.IngestOrder, x => new { x.TimeseriesId });
            });
        });

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "timeseries_id", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "ingest_order", ClrType = typeof(long), ColumnType = "Int64" },
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "ReplacingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "TimeseriesId" });
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "IngestOrder");

        var sql = GenerateSql(operation, model);

        Assert.Contains("ReplacingMergeTree(\"ingest_order\")", sql);
        Assert.Contains("ORDER BY (\"timeseries_id\")", sql);
        Assert.DoesNotContain("IngestOrder", sql);
        Assert.DoesNotContain("TimeseriesId", sql);
    }

    [Fact]
    public void Ttl_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.PeriodStart).HasColumnName("period_start");
                entity.UseMergeTree(x => x.Id);
                entity.HasTtl(x => x.PeriodStart, TimeSpan.FromDays(30));
            });
        });

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "period_start", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Ttl, "\"PeriodStart\" + INTERVAL 30 DAY");

        var sql = GenerateSql(operation, model);

        Assert.Contains("TTL \"period_start\" + INTERVAL 30 DAY", sql);
        Assert.DoesNotContain("PeriodStart", sql);
    }

    [Fact]
    public void SampleBy_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.TimeseriesId).HasColumnName("timeseries_id");
                entity.UseMergeTree(x => x.Id);
                entity.HasSampleBy(x => x.TimeseriesId);
            });
        });

        var operation = new CreateTableOperation
        {
            Name = "test_table",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "timeseries_id", ClrType = typeof(int), ColumnType = "Int32" },
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.SampleBy, "\"TimeseriesId\"");

        var sql = GenerateSql(operation, model);

        Assert.Contains("SAMPLE BY \"timeseries_id\"", sql);
        Assert.DoesNotContain("TimeseriesId", sql);
    }

    [Fact]
    public void WithoutCustomColumnNames_PropertyNamesPassThrough()
    {
        var operation = new CreateTableOperation
        {
            Name = "events",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "EventTime", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "EventTime", "Id" });

        var sql = GenerateSql(operation);

        Assert.Contains("ORDER BY (\"EventTime\", \"Id\")", sql);
    }

    #endregion

    #region Annotation Code Generation Tests

    [Fact]
    public void CodeGen_OrderBy_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.TimeseriesId).HasColumnName("timeseries_id");
                entity.Property(e => e.PeriodStart).HasColumnName("period_start");
                entity.UseMergeTree(x => new { x.TimeseriesId, x.PeriodStart });
            });
        });

        var codeGenerator = CreateAnnotationCodeGenerator();
        var entityType = model.FindEntityType(typeof(SnakeCaseEntity))!;

        var annotations = entityType.GetAnnotations()
            .Where(a => a.Name.StartsWith(ClickHouseAnnotationNames.Prefix))
            .ToDictionary(a => a.Name, a => a);

        var calls = codeGenerator.GenerateFluentApiCalls(entityType, annotations);

        var mergeTreeCall = calls.First(c => c.Method == nameof(ClickHouseEntityTypeBuilderExtensions.UseMergeTree));
        var args = mergeTreeCall.Arguments;

        Assert.Contains("timeseries_id", args.Cast<string>());
        Assert.Contains("period_start", args.Cast<string>());
        Assert.DoesNotContain("TimeseriesId", args.Cast<string>());
        Assert.DoesNotContain("PeriodStart", args.Cast<string>());
    }

    [Fact]
    public void CodeGen_PartitionBy_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.PeriodStart).HasColumnName("period_start");
                entity.UseMergeTree(x => x.Id);
                entity.HasPartitionByMonth(x => x.PeriodStart);
            });
        });

        var codeGenerator = CreateAnnotationCodeGenerator();
        var entityType = model.FindEntityType(typeof(SnakeCaseEntity))!;

        var annotations = entityType.GetAnnotations()
            .Where(a => a.Name.StartsWith(ClickHouseAnnotationNames.Prefix))
            .ToDictionary(a => a.Name, a => a);

        var calls = codeGenerator.GenerateFluentApiCalls(entityType, annotations);

        var partitionCall = calls.First(c => c.Method == nameof(ClickHouseEntityTypeBuilderExtensions.HasPartitionBy));
        var expression = (string)partitionCall.Arguments[0];

        Assert.Contains("period_start", expression);
        Assert.DoesNotContain("PeriodStart", expression);
    }

    [Fact]
    public void CodeGen_ReplacingMergeTree_WithCustomColumnNames_ResolvesToColumnNames()
    {
        var model = BuildModel(builder =>
        {
            builder.Entity<SnakeCaseEntity>(entity =>
            {
                entity.ToTable("test_table");
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).HasColumnName("id");
                entity.Property(e => e.TimeseriesId).HasColumnName("timeseries_id");
                entity.Property(e => e.IngestOrder).HasColumnName("ingest_order");
                entity.UseReplacingMergeTree(x => x.IngestOrder, x => new { x.TimeseriesId });
            });
        });

        var codeGenerator = CreateAnnotationCodeGenerator();
        var entityType = model.FindEntityType(typeof(SnakeCaseEntity))!;

        var annotations = entityType.GetAnnotations()
            .Where(a => a.Name.StartsWith(ClickHouseAnnotationNames.Prefix))
            .ToDictionary(a => a.Name, a => a);

        var calls = codeGenerator.GenerateFluentApiCalls(entityType, annotations);

        var engineCall = calls.First(c => c.Method == nameof(ClickHouseEntityTypeBuilderExtensions.UseReplacingMergeTree));
        var versionArg = (string)engineCall.Arguments[0];
        var orderByArg = (string[])engineCall.Arguments[1];

        Assert.Equal("ingest_order", versionArg);
        Assert.Contains("timeseries_id", orderByArg);
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Builds a finalized IModel directly via ModelBuilder (no DbContext caching).
    /// </summary>
    private static IModel BuildModel(Action<ModelBuilder> configure)
    {
        // Use a ClickHouse-configured context to get the right conventions
        var options = new DbContextOptionsBuilder<ColumnNameTestContext>()
            .UseClickHouse($"Host=localhost;Database=test_{Guid.NewGuid():N}")
            .EnableServiceProviderCaching(false)
            .Options;

        using var context = new ColumnNameTestContext(options, configure);
        return context.Model;
    }

    private static IMigrationsSqlGenerator GetMigrationsSqlGenerator()
    {
        var options = new DbContextOptionsBuilder<ColumnNameTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new ColumnNameTestContext(options, _ => { });
        return ((IInfrastructure<IServiceProvider>)context).Instance.GetService<IMigrationsSqlGenerator>()!;
    }

    private static ClickHouseAnnotationCodeGenerator CreateAnnotationCodeGenerator()
    {
        var services = new ServiceCollection();
        new ClickHouseDesignTimeServices().ConfigureDesignTimeServices(services);
        var sp = services.BuildServiceProvider();
        var deps = sp.GetRequiredService<AnnotationCodeGeneratorDependencies>();
        return new ClickHouseAnnotationCodeGenerator(deps);
    }

    private static string GenerateSql(MigrationOperation operation, IModel? model = null)
    {
        var generator = GetMigrationsSqlGenerator();
        var commands = generator.Generate(new[] { operation }, model);
        return string.Join("\n", commands.Select(c => c.CommandText));
    }

    #endregion
}

public class ColumnNameTestContext : DbContext
{
    private readonly Action<ModelBuilder> _configure;

    public ColumnNameTestContext(DbContextOptions<ColumnNameTestContext> options, Action<ModelBuilder> configure)
        : base(options)
    {
        _configure = configure;
    }

    public DbSet<SnakeCaseEntity> SnakeCaseEntities => Set<SnakeCaseEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        _configure(modelBuilder);
    }
}

public class SnakeCaseEntity
{
    public Guid Id { get; set; }
    public int TimeseriesId { get; set; }
    public DateTime PeriodStart { get; set; }
    public DateTime PeriodEnd { get; set; }
    public DateTime DataPublishedTime { get; set; }
    public long IngestOrder { get; set; }
}
