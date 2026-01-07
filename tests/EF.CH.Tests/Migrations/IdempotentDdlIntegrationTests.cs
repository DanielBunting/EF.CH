using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Integration tests verifying that idempotent DDL actually executes without errors
/// when run multiple times against a real ClickHouse instance.
/// This validates the IF EXISTS / IF NOT EXISTS clauses work correctly.
/// </summary>
public class IdempotentDdlIntegrationTests : IAsyncLifetime
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

    #region CREATE TABLE IF NOT EXISTS

    [Fact]
    public async Task CreateTableIfNotExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        var sql = """
            CREATE TABLE IF NOT EXISTS "IdempotentTest" (
                "Id" UUID,
                "Name" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """;

        // First execution - creates table
        await context.Database.ExecuteSqlRawAsync(sql);

        // Second execution - should be idempotent (no error)
        await context.Database.ExecuteSqlRawAsync(sql);

        // Verify table exists
        var exists = await TableExistsAsync(context, "IdempotentTest");
        Assert.True(exists);
    }

    [Fact]
    public async Task CreateTableIfNotExists_WithDifferentEngines_NoError()
    {
        await using var context = CreateContext();

        // MergeTree
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "MergeTreeTable" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "MergeTreeTable" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // ReplacingMergeTree
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "ReplacingTable" ("Id" UUID, "Version" Int64)
            ENGINE = ReplacingMergeTree("Version") ORDER BY ("Id")
            """);
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "ReplacingTable" ("Id" UUID, "Version" Int64)
            ENGINE = ReplacingMergeTree("Version") ORDER BY ("Id")
            """);

        // SummingMergeTree
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "SummingTable" ("Id" UUID, "Count" Int64)
            ENGINE = SummingMergeTree() ORDER BY ("Id")
            """);
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "SummingTable" ("Id" UUID, "Count" Int64)
            ENGINE = SummingMergeTree() ORDER BY ("Id")
            """);

        Assert.True(await TableExistsAsync(context, "MergeTreeTable"));
        Assert.True(await TableExistsAsync(context, "ReplacingTable"));
        Assert.True(await TableExistsAsync(context, "SummingTable"));
    }

    #endregion

    #region DROP TABLE IF EXISTS

    [Fact]
    public async Task DropTableIfExists_TableDoesNotExist_NoError()
    {
        await using var context = CreateContext();

        // Drop non-existent table - should not error
        await context.Database.ExecuteSqlRawAsync(
            "DROP TABLE IF EXISTS \"NonExistentTable\"");

        // No exception = success
    }

    [Fact]
    public async Task DropTableIfExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropTest" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // First drop - removes table
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"DropTest\"");

        // Second drop - should be idempotent
        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"DropTest\"");

        Assert.False(await TableExistsAsync(context, "DropTest"));
    }

    #endregion

    #region ADD COLUMN IF NOT EXISTS

    [Fact]
    public async Task AddColumnIfNotExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ColumnTest" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // First add column
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"ColumnTest\" ADD COLUMN IF NOT EXISTS \"NewCol\" String");

        // Second add column - should be idempotent
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"ColumnTest\" ADD COLUMN IF NOT EXISTS \"NewCol\" String");

        Assert.True(await ColumnExistsAsync(context, "ColumnTest", "NewCol"));
    }

    [Fact]
    public async Task AddColumnIfNotExists_MultipleColumns_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "MultiColumnTest" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Add multiple columns idempotently
        var columns = new[] { "Col1", "Col2", "Col3" };
        foreach (var col in columns)
        {
            await context.Database.ExecuteSqlRawAsync(
                $"ALTER TABLE \"MultiColumnTest\" ADD COLUMN IF NOT EXISTS \"{col}\" String");
            // Run twice
            await context.Database.ExecuteSqlRawAsync(
                $"ALTER TABLE \"MultiColumnTest\" ADD COLUMN IF NOT EXISTS \"{col}\" String");
        }

        foreach (var col in columns)
        {
            Assert.True(await ColumnExistsAsync(context, "MultiColumnTest", col));
        }
    }

    #endregion

    #region DROP COLUMN IF EXISTS

    [Fact]
    public async Task DropColumnIfExists_ColumnDoesNotExist_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropColumnTest" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Drop non-existent column - should not error
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropColumnTest\" DROP COLUMN IF EXISTS \"NonExistentCol\"");
    }

    [Fact]
    public async Task DropColumnIfExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropColumnTest2" ("Id" UUID, "ToRemove" String)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // First drop
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropColumnTest2\" DROP COLUMN IF EXISTS \"ToRemove\"");

        // Second drop - should be idempotent
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropColumnTest2\" DROP COLUMN IF EXISTS \"ToRemove\"");

        Assert.False(await ColumnExistsAsync(context, "DropColumnTest2", "ToRemove"));
    }

    #endregion

    #region ADD INDEX IF NOT EXISTS

    [Fact]
    public async Task AddIndexIfNotExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "IndexTest" (
                "Id" UUID,
                "Timestamp" DateTime64(3)
            )
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // First add index
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "IndexTest" ADD INDEX IF NOT EXISTS "IX_Timestamp" ("Timestamp") TYPE minmax GRANULARITY 3
            """);

        // Second add index - should be idempotent
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "IndexTest" ADD INDEX IF NOT EXISTS "IX_Timestamp" ("Timestamp") TYPE minmax GRANULARITY 3
            """);

        Assert.True(await IndexExistsAsync(context, "IndexTest", "IX_Timestamp"));
    }

    [Fact]
    public async Task AddIndexIfNotExists_DifferentIndexTypes_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "MultiIndexTest" (
                "Id" UUID,
                "Timestamp" DateTime64(3),
                "Tags" Array(String),
                "Message" String,
                "Status" String
            )
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Minmax index
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "MultiIndexTest" ADD INDEX IF NOT EXISTS "IX_Timestamp" ("Timestamp") TYPE minmax GRANULARITY 3
            """);
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "MultiIndexTest" ADD INDEX IF NOT EXISTS "IX_Timestamp" ("Timestamp") TYPE minmax GRANULARITY 3
            """);

        // Bloom filter index
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "MultiIndexTest" ADD INDEX IF NOT EXISTS "IX_Tags" ("Tags") TYPE bloom_filter(0.025) GRANULARITY 3
            """);
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "MultiIndexTest" ADD INDEX IF NOT EXISTS "IX_Tags" ("Tags") TYPE bloom_filter(0.025) GRANULARITY 3
            """);

        // Set index
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "MultiIndexTest" ADD INDEX IF NOT EXISTS "IX_Status" ("Status") TYPE set(100) GRANULARITY 2
            """);
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "MultiIndexTest" ADD INDEX IF NOT EXISTS "IX_Status" ("Status") TYPE set(100) GRANULARITY 2
            """);

        Assert.True(await IndexExistsAsync(context, "MultiIndexTest", "IX_Timestamp"));
        Assert.True(await IndexExistsAsync(context, "MultiIndexTest", "IX_Tags"));
        Assert.True(await IndexExistsAsync(context, "MultiIndexTest", "IX_Status"));
    }

    #endregion

    #region DROP INDEX IF EXISTS

    [Fact]
    public async Task DropIndexIfExists_IndexDoesNotExist_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropIndexTest" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Drop non-existent index - should not error
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropIndexTest\" DROP INDEX IF EXISTS \"NonExistentIndex\"");
    }

    [Fact]
    public async Task DropIndexIfExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropIndexTest2" ("Id" UUID, "Timestamp" DateTime64(3))
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "DropIndexTest2" ADD INDEX "IX_ToRemove" ("Timestamp") TYPE minmax GRANULARITY 3
            """);

        // First drop
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropIndexTest2\" DROP INDEX IF EXISTS \"IX_ToRemove\"");

        // Second drop - should be idempotent
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropIndexTest2\" DROP INDEX IF EXISTS \"IX_ToRemove\"");

        Assert.False(await IndexExistsAsync(context, "DropIndexTest2", "IX_ToRemove"));
    }

    #endregion

    #region ADD PROJECTION IF NOT EXISTS

    [Fact]
    public async Task AddProjectionIfNotExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ProjectionTest" (
                "Id" UUID,
                "Status" String,
                "CreatedAt" DateTime64(3)
            )
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // First add projection
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "ProjectionTest" ADD PROJECTION IF NOT EXISTS "prj_by_status" (SELECT * ORDER BY ("Status", "CreatedAt"))
            """);

        // Second add projection - should be idempotent
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "ProjectionTest" ADD PROJECTION IF NOT EXISTS "prj_by_status" (SELECT * ORDER BY ("Status", "CreatedAt"))
            """);

        Assert.True(await ProjectionExistsAsync(context, "ProjectionTest", "prj_by_status"));
    }

    [Fact]
    public async Task AddProjectionIfNotExists_AggregateProjection_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "AggProjectionTest" (
                "Id" UUID,
                "Category" String,
                "Amount" Decimal(18, 4),
                "CreatedAt" DateTime64(3)
            )
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Aggregate projection
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "AggProjectionTest" ADD PROJECTION IF NOT EXISTS "prj_daily_totals" (
                SELECT toDate("CreatedAt") AS "Date", "Category", sum("Amount") AS "Total"
                GROUP BY "Date", "Category"
            )
            """);

        // Run twice
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "AggProjectionTest" ADD PROJECTION IF NOT EXISTS "prj_daily_totals" (
                SELECT toDate("CreatedAt") AS "Date", "Category", sum("Amount") AS "Total"
                GROUP BY "Date", "Category"
            )
            """);

        Assert.True(await ProjectionExistsAsync(context, "AggProjectionTest", "prj_daily_totals"));
    }

    #endregion

    #region DROP PROJECTION IF EXISTS

    [Fact]
    public async Task DropProjectionIfExists_ProjectionDoesNotExist_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropProjectionTest" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Drop non-existent projection - should not error
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropProjectionTest\" DROP PROJECTION IF EXISTS \"NonExistentProjection\"");
    }

    [Fact]
    public async Task DropProjectionIfExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "DropProjectionTest2" ("Id" UUID, "Status" String)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "DropProjectionTest2" ADD PROJECTION "prj_to_remove" (SELECT * ORDER BY ("Status"))
            """);

        // First drop
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropProjectionTest2\" DROP PROJECTION IF EXISTS \"prj_to_remove\"");

        // Second drop - should be idempotent
        await context.Database.ExecuteSqlRawAsync(
            "ALTER TABLE \"DropProjectionTest2\" DROP PROJECTION IF EXISTS \"prj_to_remove\"");

        Assert.False(await ProjectionExistsAsync(context, "DropProjectionTest2", "prj_to_remove"));
    }

    #endregion

    #region CREATE MATERIALIZED VIEW IF NOT EXISTS

    [Fact]
    public async Task CreateMaterializedViewIfNotExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "SourceEvents" (
                "Id" UUID,
                "EventType" String,
                "CreatedAt" DateTime64(3)
            )
            ENGINE = MergeTree() ORDER BY ("CreatedAt", "Id")
            """);

        // Create materialized view
        var mvSql = """
            CREATE MATERIALIZED VIEW IF NOT EXISTS "EventSummary"
            ENGINE = SummingMergeTree()
            ORDER BY ("EventType")
            AS SELECT "EventType", count() AS "Count"
            FROM "SourceEvents"
            GROUP BY "EventType"
            """;

        // First creation
        await context.Database.ExecuteSqlRawAsync(mvSql);

        // Second creation - should be idempotent
        await context.Database.ExecuteSqlRawAsync(mvSql);

        Assert.True(await TableExistsAsync(context, "EventSummary"));
    }

    #endregion

    #region CREATE DICTIONARY IF NOT EXISTS

    [Fact]
    public async Task CreateDictionaryIfNotExists_RunTwice_NoError()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Countries" (
                "Id" UInt64,
                "Name" String,
                "Code" String
            )
            ENGINE = MergeTree() ORDER BY ("Id")
            """);

        // Insert some data
        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "Countries" VALUES (1, 'United States', 'US'), (2, 'United Kingdom', 'GB')
            """);

        // Create dictionary
        var dictSql = """
            CREATE DICTIONARY IF NOT EXISTS "country_dict"
            (
                "Id" UInt64,
                "Name" String,
                "Code" String
            )
            PRIMARY KEY "Id"
            SOURCE(CLICKHOUSE(TABLE 'Countries'))
            LAYOUT(HASHED())
            LIFETIME(300)
            """;

        // First creation
        await context.Database.ExecuteSqlRawAsync(dictSql);

        // Second creation - should be idempotent
        await context.Database.ExecuteSqlRawAsync(dictSql);

        Assert.True(await DictionaryExistsAsync(context, "country_dict"));
    }

    #endregion

    #region Complex Multi-Step Scenario

    [Fact]
    public async Task CompleteStepMigration_AllStepsIdempotent()
    {
        await using var context = CreateContext();

        // Simulate a multi-step migration where each step can be retried
        // Step 1: Create table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Orders" (
                "Id" UUID,
                "CustomerId" UUID,
                "Status" String,
                "Amount" Decimal(18, 4),
                "CreatedAt" DateTime64(3)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM("CreatedAt")
            ORDER BY ("CreatedAt", "Id")
            """);

        // Step 2: Add index
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Orders" ADD INDEX IF NOT EXISTS "IX_Orders_Status" ("Status") TYPE set(100) GRANULARITY 2
            """);

        // Step 3: Add projection
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Orders" ADD PROJECTION IF NOT EXISTS "prj_by_customer" (SELECT * ORDER BY ("CustomerId", "CreatedAt"))
            """);

        // Now simulate a retry - all steps should be idempotent
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Orders" (
                "Id" UUID,
                "CustomerId" UUID,
                "Status" String,
                "Amount" Decimal(18, 4),
                "CreatedAt" DateTime64(3)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM("CreatedAt")
            ORDER BY ("CreatedAt", "Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Orders" ADD INDEX IF NOT EXISTS "IX_Orders_Status" ("Status") TYPE set(100) GRANULARITY 2
            """);

        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Orders" ADD PROJECTION IF NOT EXISTS "prj_by_customer" (SELECT * ORDER BY ("CustomerId", "CreatedAt"))
            """);

        // Verify all objects exist
        Assert.True(await TableExistsAsync(context, "Orders"));
        Assert.True(await IndexExistsAsync(context, "Orders", "IX_Orders_Status"));
        Assert.True(await ProjectionExistsAsync(context, "Orders", "prj_by_customer"));
    }

    #endregion

    #region Helper Methods

    private TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new TestDbContext(options);
    }

    private static async Task<bool> TableExistsAsync(DbContext context, string tableName)
    {
        var result = await context.Database.SqlQueryRaw<string>(
            $"SELECT name AS Value FROM system.tables WHERE database = currentDatabase() AND name = '{tableName}'"
        ).FirstOrDefaultAsync();

        return result != null;
    }

    private static async Task<bool> ColumnExistsAsync(DbContext context, string tableName, string columnName)
    {
        var result = await context.Database.SqlQueryRaw<string>(
            $"SELECT name AS Value FROM system.columns WHERE database = currentDatabase() AND table = '{tableName}' AND name = '{columnName}'"
        ).FirstOrDefaultAsync();

        return result != null;
    }

    private static async Task<bool> IndexExistsAsync(DbContext context, string tableName, string indexName)
    {
        var result = await context.Database.SqlQueryRaw<string>(
            $"SELECT name AS Value FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = '{tableName}' AND name = '{indexName}'"
        ).FirstOrDefaultAsync();

        return result != null;
    }

    private static async Task<bool> ProjectionExistsAsync(DbContext context, string tableName, string projectionName)
    {
        var result = await context.Database.SqlQueryRaw<string>(
            $"SELECT name AS Value FROM system.projections WHERE database = currentDatabase() AND table = '{tableName}' AND name = '{projectionName}'"
        ).FirstOrDefaultAsync();

        return result != null;
    }

    private static async Task<bool> DictionaryExistsAsync(DbContext context, string dictionaryName)
    {
        var result = await context.Database.SqlQueryRaw<string>(
            $"SELECT name AS Value FROM system.dictionaries WHERE database = currentDatabase() AND name = '{dictionaryName}'"
        ).FirstOrDefaultAsync();

        return result != null;
    }

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options) : base(options) { }
    }

    #endregion
}
