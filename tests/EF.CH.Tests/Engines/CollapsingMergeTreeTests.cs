using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Engines;

public class CollapsingMergeTreeTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_WithCollapsingMergeTree_GeneratesCorrectDdl()
    {
        using var context = CreateContext("Host=localhost");
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "user_sessions",
            Columns =
            {
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "PageViews", ClrType = typeof(int), ColumnType = "UInt32" },
                new AddColumnOperation { Name = "Duration", ClrType = typeof(int), ColumnType = "UInt32" },
                new AddColumnOperation { Name = "Sign", ClrType = typeof(sbyte), ColumnType = "Int8" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "CollapsingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.SignColumn, "Sign");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "UserId" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"user_sessions\"", sql);
        Assert.Contains("ENGINE = CollapsingMergeTree(\"Sign\")", sql);
        Assert.Contains("ORDER BY (\"UserId\")", sql);
    }

    [Fact]
    public void CreateTable_WithVersionedCollapsingMergeTree_GeneratesCorrectDdl()
    {
        using var context = CreateContext("Host=localhost");
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "versioned_sessions",
            Columns =
            {
                new AddColumnOperation { Name = "UserId", ClrType = typeof(long), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "PageViews", ClrType = typeof(int), ColumnType = "UInt32" },
                new AddColumnOperation { Name = "Sign", ClrType = typeof(sbyte), ColumnType = "Int8" },
                new AddColumnOperation { Name = "Version", ClrType = typeof(uint), ColumnType = "UInt32" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "VersionedCollapsingMergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.SignColumn, "Sign");
        operation.AddAnnotation(ClickHouseAnnotationNames.VersionColumn, "Version");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "UserId" });

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("\"versioned_sessions\"", sql);
        Assert.Contains("ENGINE = VersionedCollapsingMergeTree(\"Sign\", \"Version\")", sql);
        Assert.Contains("ORDER BY (\"UserId\")", sql);
    }

    #endregion

    #region Fluent API Tests

    [Fact]
    public void ModelBuilder_UseCollapsingMergeTree_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<CollapsingTestEntity>(entity =>
        {
            entity.ToTable("collapsing_test");
            entity.HasKey(e => e.Id);
            entity.UseCollapsingMergeTree(x => x.Sign, x => x.Id);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CollapsingTestEntity))!;

        Assert.Equal("CollapsingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Sign", entityType.FindAnnotation(ClickHouseAnnotationNames.SignColumn)?.Value);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseVersionedCollapsingMergeTree_SetsAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<VersionedCollapsingTestEntity>(entity =>
        {
            entity.ToTable("versioned_test");
            entity.HasKey(e => e.Id);
            entity.UseVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => x.Id);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(VersionedCollapsingTestEntity))!;

        Assert.Equal("VersionedCollapsingMergeTree", entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal("Sign", entityType.FindAnnotation(ClickHouseAnnotationNames.SignColumn)?.Value);
        Assert.Equal("Version", entityType.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Fact]
    public void ModelBuilder_UseCollapsingMergeTree_WithMultipleOrderByColumns()
    {
        var builder = new ModelBuilder();

        builder.Entity<CollapsingTestEntity>(entity =>
        {
            entity.ToTable("multi_order");
            entity.HasKey(e => e.Id);
            entity.UseCollapsingMergeTree(x => x.Sign, x => new { x.UserId, x.EventTime });
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CollapsingTestEntity))!;

        Assert.Equal(new[] { "UserId", "EventTime" }, entityType.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CollapsingMergeTree_CanCreateTableAndInsert()
    {
        await using var context = CreateContext(GetConnectionString());

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "CollapsingTest" (
                "UserId" UInt64,
                "PageViews" UInt32,
                "Duration" UInt32,
                "Sign" Int8
            )
            ENGINE = CollapsingMergeTree("Sign")
            ORDER BY "UserId"
            """);

        // Insert a state row (+1)
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""CollapsingTest"" (""UserId"", ""PageViews"", ""Duration"", ""Sign"") VALUES (1, 5, 100, 1)");

        var count = await context.Database.SqlQueryRaw<ulong>(
            @"SELECT count() AS ""Value"" FROM ""CollapsingTest"""
        ).FirstOrDefaultAsync();

        Assert.Equal(1UL, count);
    }

    [Fact]
    public async Task CollapsingMergeTree_CollapsesRowsAfterOptimize()
    {
        await using var context = CreateContext(GetConnectionString());

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "CollapsingOptimize" (
                "UserId" UInt64,
                "PageViews" UInt32,
                "Sign" Int8
            )
            ENGINE = CollapsingMergeTree("Sign")
            ORDER BY "UserId"
            """);

        // Insert state (+1), then cancel (-1), then new state (+1)
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""CollapsingOptimize"" VALUES (1, 5, 1)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""CollapsingOptimize"" VALUES (1, 5, -1)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""CollapsingOptimize"" VALUES (1, 10, 1)");

        // Force merge
        await context.Database.ExecuteSqlRawAsync(
            @"OPTIMIZE TABLE ""CollapsingOptimize"" FINAL");

        // Should have 1 row after collapsing
        var count = await context.Database.SqlQueryRaw<ulong>(
            @"SELECT count() AS ""Value"" FROM ""CollapsingOptimize"""
        ).FirstOrDefaultAsync();

        Assert.Equal(1UL, count);

        // Verify it's the latest state
        var pageViews = await context.Database.SqlQueryRaw<uint>(
            @"SELECT ""PageViews"" AS ""Value"" FROM ""CollapsingOptimize"""
        ).FirstOrDefaultAsync();

        Assert.Equal(10U, pageViews);
    }

    [Fact]
    public async Task VersionedCollapsingMergeTree_CanCreateTableAndInsert()
    {
        await using var context = CreateContext(GetConnectionString());

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "VersionedCollapsingTest" (
                "UserId" UInt64,
                "PageViews" UInt32,
                "Sign" Int8,
                "Version" UInt32
            )
            ENGINE = VersionedCollapsingMergeTree("Sign", "Version")
            ORDER BY "UserId"
            """);

        // Insert rows (can be out of order with versioning)
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""VersionedCollapsingTest"" VALUES (1, 5, 1, 1)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""VersionedCollapsingTest"" VALUES (1, 5, -1, 1)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""VersionedCollapsingTest"" VALUES (1, 10, 1, 2)");

        // Force merge
        await context.Database.ExecuteSqlRawAsync(
            @"OPTIMIZE TABLE ""VersionedCollapsingTest"" FINAL");

        var count = await context.Database.SqlQueryRaw<ulong>(
            @"SELECT count() AS ""Value"" FROM ""VersionedCollapsingTest"""
        ).FirstOrDefaultAsync();

        Assert.Equal(1UL, count);
    }

    #endregion

    #region Helpers

    private CollapsingMergeTreeTestContext CreateContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<CollapsingMergeTreeTestContext>()
            .UseClickHouse(connectionString)
            .Options;

        return new CollapsingMergeTreeTestContext(options);
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

    #endregion
}

#region Test Entities and Context

public class CollapsingTestEntity
{
    public long Id { get; set; }
    public long UserId { get; set; }
    public DateTime EventTime { get; set; }
    public int PageViews { get; set; }
    public int Duration { get; set; }
    public sbyte Sign { get; set; }
}

public class VersionedCollapsingTestEntity
{
    public long Id { get; set; }
    public long UserId { get; set; }
    public int PageViews { get; set; }
    public sbyte Sign { get; set; }
    public uint Version { get; set; }
}

public class CollapsingMergeTreeTestContext : DbContext
{
    public CollapsingMergeTreeTestContext(DbContextOptions<CollapsingMergeTreeTestContext> options)
        : base(options) { }
}

#endregion
