using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Internal;
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
            entity.UseReplacingMergeTree(x => x.Id)
                .WithVersion(x => x.Version)
                .WithIsDeleted(x => x.IsDeleted);
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

    // -- Regression pins for the fluent ReplacingMergeTree builder ----------------
    // The old multi-parameter overloads (UseReplacingMergeTree<T, TVersion>(...),
    // UseReplacingMergeTree<T, TVersion, TIsDeleted>(...)) were removed in favor of
    // the .WithVersion / .WithIsDeleted builder. These tests pin that the fluent
    // form produces the same DDL contract those overloads upheld:
    //   ENGINE = ReplacingMergeTree([ver [, is_deleted]]) ... ORDER BY (...)
    //
    // Routed through the real MigrationsModelDiffer + SQL generator so any future
    // annotation the production migration path emits is automatically observed.

    [Fact]
    public void ModelBuilder_UseReplacingMergeTree_Fluent_WithVersion_GeneratesExpectedDdl()
    {
        var sql = GenerateDdlFromModelViaRealDiffer<TestReplacingWithDeleteEntity>(
            entity =>
            {
                entity.ToTable("versioned");
                entity.HasKey(e => e.Id);
                entity.UseReplacingMergeTree(x => x.Id).WithVersion(x => x.Version);
            });

        Assert.Contains("ENGINE = ReplacingMergeTree(\"Version\")", sql);
        Assert.Contains("ORDER BY (\"Id\")", sql);
        Assert.DoesNotContain("IsDeleted)", sql);
    }

    [Fact]
    public void ModelBuilder_UseReplacingMergeTree_Fluent_WithVersionAndIsDeleted_GeneratesExpectedDdl()
    {
        var sql = GenerateDdlFromModelViaRealDiffer<TestReplacingWithDeleteEntity>(
            entity =>
            {
                entity.ToTable("deletable");
                entity.HasKey(e => e.Id);
                entity.UseReplacingMergeTree(x => x.Id)
                    .WithVersion(x => x.Version)
                    .WithIsDeleted(x => x.IsDeleted);
            });

        Assert.Contains("ENGINE = ReplacingMergeTree(\"Version\", \"IsDeleted\")", sql);
        Assert.Contains("ORDER BY (\"Id\")", sql);
    }

    [Fact]
    public void CreateTable_WithPrimaryKey_ShorterThanOrderBy_GeneratesPrimaryKeyClause()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "events",
            Columns =
            {
                new AddColumnOperation { Name = "UserId", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Timestamp", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "EventId", ClrType = typeof(Guid), ColumnType = "UUID" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "UserId", "Timestamp", "EventId" });
        operation.AddAnnotation(ClickHouseAnnotationNames.PrimaryKey, new[] { "UserId", "Timestamp" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ORDER BY (\"UserId\", \"Timestamp\", \"EventId\")", sql);
        Assert.Contains("PRIMARY KEY (\"UserId\", \"Timestamp\")", sql);
        Assert.True(
            sql.IndexOf("ORDER BY", StringComparison.Ordinal)
                < sql.IndexOf("PRIMARY KEY", StringComparison.Ordinal),
            "PRIMARY KEY must follow ORDER BY in ClickHouse syntax order.");
    }

    [Fact]
    public void ModelBuilder_WithPrimaryKey_PrefixOfOrderBy_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestMergeTreeEntity>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(e => new { e.EventTime, e.Id })
                .WithPrimaryKey(e => e.EventTime);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestMergeTreeEntity))!;

        Assert.Equal(
            new[] { "EventTime" },
            entityType.FindAnnotation(ClickHouseAnnotationNames.PrimaryKey)?.Value);
        Assert.Equal(
            new[] { "EventTime", "Id" },
            entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Fact]
    public void ModelBuilder_WithPrimaryKey_Composite_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestMergeTreeEntity>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseReplacingMergeTree(e => new { e.EventTime, e.Id, e.EventType })
                .WithPrimaryKey(e => new { e.EventTime, e.Id });
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestMergeTreeEntity))!;

        Assert.Equal(
            new[] { "EventTime", "Id" },
            entityType.FindAnnotation(ClickHouseAnnotationNames.PrimaryKey)?.Value);
    }

    [Fact]
    public void ModelBuilder_WithPrimaryKey_NotAPrefixOfOrderBy_Throws()
    {
        var builder = new ModelBuilder();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.Entity<TestMergeTreeEntity>(entity =>
            {
                entity.ToTable("events");
                entity.HasKey(e => e.Id);
                entity.UseMergeTree(e => new { e.EventTime, e.Id })
                    .WithPrimaryKey(e => e.Id);
            }));

        Assert.Contains("must form a prefix", ex.Message);
    }

    [Fact]
    public void ModelBuilder_WithPrimaryKey_ColumnNotInOrderBy_Throws()
    {
        var builder = new ModelBuilder();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.Entity<TestMergeTreeEntity>(entity =>
            {
                entity.ToTable("events");
                entity.HasKey(e => e.Id);
                entity.UseMergeTree(e => e.EventTime)
                    .WithPrimaryKey(e => e.EventType);
            }));

        Assert.Contains("must form a prefix", ex.Message);
    }

    [Fact]
    public void ModelBuilder_WithPrimaryKey_LongerThanOrderBy_Throws()
    {
        var builder = new ModelBuilder();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.Entity<TestMergeTreeEntity>(entity =>
            {
                entity.ToTable("events");
                entity.HasKey(e => e.Id);
                entity.UseMergeTree(e => e.EventTime)
                    .WithPrimaryKey(e => new { e.EventTime, e.Id });
            }));

        Assert.Contains("must form a prefix", ex.Message);
    }

    [Fact]
    public void ModelBuilder_WithPrimaryKey_EqualToOrderBy_Allowed()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestMergeTreeEntity>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(e => new { e.EventTime, e.Id })
                .WithPrimaryKey(e => new { e.EventTime, e.Id });
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestMergeTreeEntity))!;

        Assert.Equal(
            new[] { "EventTime", "Id" },
            entityType.FindAnnotation(ClickHouseAnnotationNames.PrimaryKey)?.Value);
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

    /// <summary>
    /// Routes the model through the real <see cref="IMigrationsModelDiffer"/>
    /// + <see cref="IMigrationsSqlGenerator"/> pipeline. No annotation
    /// allowlist, no CLR-type fallback table — anything the production
    /// migration path emits, this test sees.
    /// </summary>
    private static string GenerateDdlFromModelViaRealDiffer<TEntity>(
        Action<EntityTypeBuilder<TEntity>> configure)
        where TEntity : class
    {
        // EF Core caches the model per context type. Two tests sharing
        // FluentDdlContext would otherwise see the first test's frozen model.
        // EnableServiceProviderCaching(false) gives each call a fresh
        // service provider (and therefore a fresh model cache).
        var options = new DbContextOptionsBuilder<FluentDdlContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .EnableServiceProviderCaching(false)
            .Options;
        using var context = new FluentDdlContext(options, mb => mb.Entity<TEntity>(configure));

        var sp = ((IInfrastructure<IServiceProvider>)context).Instance;
        var differ = sp.GetRequiredService<IMigrationsModelDiffer>();
        var generator = sp.GetRequiredService<IMigrationsSqlGenerator>();
        // The diff requires the design-time relational model (IsExcludedFromMigrations
        // and other diff inputs are stored there, not on the read-optimized context.Model).
        var designModel = sp.GetRequiredService<Microsoft.EntityFrameworkCore.Metadata.IDesignTimeModel>().Model;

        var operations = differ.GetDifferences(source: null, target: designModel.GetRelationalModel());
        // Pass the design-time model — the SQL generator's engine-clause path
        // falls back to entity-type annotations when operation annotations are
        // absent (which the differ doesn't propagate for engine knobs).
        var commands = generator.Generate(operations, designModel);
        return string.Join("\n", commands.Select(c => c.CommandText));
    }

    private sealed class FluentDdlContext : DbContext
    {
        private readonly Action<ModelBuilder> _configure;

        public FluentDdlContext(DbContextOptions<FluentDdlContext> options, Action<ModelBuilder> configure)
            : base(options) => _configure = configure;

        protected override void OnModelCreating(ModelBuilder mb) => _configure(mb);
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
