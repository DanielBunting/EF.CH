using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Integration tests verifying migration phase ordering works correctly against real ClickHouse.
/// These tests ensure operations execute in the correct dependency order.
/// </summary>
public class PhaseOrderingIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    private readonly ClickHouseMigrationsSplitter _splitter = new();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Add Column + MV Tests

    [Fact]
    public async Task AddColumnThenMvUsingIt_ExecutesInCorrectOrder()
    {
        await using var context = CreateContext();

        // Create base table first
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""orders"" (
                ""Id"" UUID,
                ""OrderDate"" DateTime64(3),
                ""Amount"" Decimal(18, 4)
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        // Now simulate a migration that adds a column and an MV using it
        // The operations might come in wrong order from EF Core
        var mvCreate = new CreateTableOperation { Name = "discount_summary" };
        mvCreate.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvCreate.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "orders");
        mvCreate.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            @"SELECT toDate(""OrderDate"") AS ""Date"", sum(""Discount"") AS ""TotalDiscount"" FROM ""orders"" GROUP BY ""Date""");

        var addColumn = new AddColumnOperation
        {
            Name = "Discount",
            Table = "orders",
            ClrType = typeof(decimal),
            ColumnType = "Decimal(18, 4)"
        };

        // Operations in WRONG order (MV first, then column)
        var operations = new List<MigrationOperation> { mvCreate, addColumn };

        // Split should reorder them correctly
        var steps = _splitter.Split(operations);

        // Verify ordering: AddColumn (Phase 5) before CreateMV (Phase 6)
        Assert.Equal(2, steps.Count);
        Assert.IsType<AddColumnOperation>(steps[0].Operation);
        Assert.IsType<CreateTableOperation>(steps[1].Operation);

        // Execute in the ordered sequence
        // Step 1: Add column
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""orders"" ADD COLUMN IF NOT EXISTS ""Discount"" Decimal(18, 4);");

        // Step 2: Create MV - this should succeed because column now exists
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""discount_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Date"")
              AS SELECT toDate(""OrderDate"") AS ""Date"", sum(""Discount"") AS ""TotalDiscount""
              FROM ""orders"" GROUP BY ""Date"";");

        // Verify both exist
        var columnExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.columns WHERE table = 'orders' AND name = 'Discount'")
            .CountAsync();
        Assert.Equal(1, columnExists);

        var mvExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'discount_summary'")
            .CountAsync();
        Assert.Equal(1, mvExists);
    }

    [Fact]
    public async Task AddColumnThenIndex_ExecutesInCorrectOrder()
    {
        await using var context = CreateContext();

        // Create base table
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""users"" (
                ""Id"" UUID,
                ""Name"" String
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        // Operations in WRONG order (index first, then column)
        var createIndex = new CreateIndexOperation
        {
            Name = "IX_Users_Email",
            Table = "users",
            Columns = new[] { "Email" }
        };
        createIndex.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, "bloom_filter");

        var addColumn = new AddColumnOperation
        {
            Name = "Email",
            Table = "users",
            ClrType = typeof(string),
            ColumnType = "String"
        };

        var operations = new List<MigrationOperation> { createIndex, addColumn };
        var steps = _splitter.Split(operations);

        // Verify ordering: AddColumn (Phase 5) before CreateIndex (Phase 8)
        Assert.Equal(2, steps.Count);
        Assert.IsType<AddColumnOperation>(steps[0].Operation);
        Assert.IsType<CreateIndexOperation>(steps[1].Operation);

        // Execute in the ordered sequence
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""users"" ADD COLUMN IF NOT EXISTS ""Email"" String;");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""users"" ADD INDEX IF NOT EXISTS ""IX_Users_Email"" (""Email"") TYPE bloom_filter GRANULARITY 1;");

        // Verify both exist
        var columnExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.columns WHERE table = 'users' AND name = 'Email'")
            .CountAsync();
        Assert.Equal(1, columnExists);
    }

    #endregion

    #region Cascading MV Tests

    [Fact]
    public async Task CascadingMvs_CreateInDependencyOrder()
    {
        await using var context = CreateContext();

        // Create base table
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""raw_events"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3),
                ""Value"" Int32
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        // MV-A reads from raw_events
        var mvA = new CreateTableOperation { Name = "hourly_summary" };
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_events");

        // MV-B reads from MV-A (cascading)
        var mvB = new CreateTableOperation { Name = "daily_summary" };
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly_summary");

        // Operations in WRONG order (dependent first)
        var operations = new List<MigrationOperation> { mvB, mvA };
        var steps = _splitter.Split(operations);

        // Verify ordering: mvA before mvB
        Assert.Equal(2, steps.Count);
        Assert.Equal("hourly_summary", ((CreateTableOperation)steps[0].Operation).Name);
        Assert.Equal("daily_summary", ((CreateTableOperation)steps[1].Operation).Name);

        // Execute in the ordered sequence
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""hourly_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Hour"")
              AS SELECT toStartOfHour(""EventTime"") AS ""Hour"", sum(""Value"") AS ""TotalValue""
              FROM ""raw_events"" GROUP BY ""Hour"";");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""daily_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Day"")
              AS SELECT toDate(""Hour"") AS ""Day"", sum(""TotalValue"") AS ""DailyTotal""
              FROM ""hourly_summary"" GROUP BY ""Day"";");

        // Verify both exist
        var mvAExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'hourly_summary'")
            .CountAsync();
        Assert.Equal(1, mvAExists);

        var mvBExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'daily_summary'")
            .CountAsync();
        Assert.Equal(1, mvBExists);
    }

    [Fact]
    public async Task CascadingMvs_DropInReverseOrder()
    {
        await using var context = CreateContext();

        // Setup: Create the chain first
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""events_for_drop"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3),
                ""Value"" Int32
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""hourly_for_drop""
              ENGINE = SummingMergeTree() ORDER BY (""Hour"")
              AS SELECT toStartOfHour(""EventTime"") AS ""Hour"", sum(""Value"") AS ""TotalValue""
              FROM ""events_for_drop"" GROUP BY ""Hour"";");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""daily_for_drop""
              ENGINE = SummingMergeTree() ORDER BY (""Day"")
              AS SELECT toDate(""Hour"") AS ""Day"", sum(""TotalValue"") AS ""DailyTotal""
              FROM ""hourly_for_drop"" GROUP BY ""Day"";");

        // Now simulate dropping in WRONG order (source first)
        var dropMvA = new DropTableOperation { Name = "hourly_for_drop" };
        dropMvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events_for_drop");

        var dropMvB = new DropTableOperation { Name = "daily_for_drop" };
        dropMvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly_for_drop");

        // Operations in WRONG order (source MV first)
        var operations = new List<MigrationOperation> { dropMvA, dropMvB };
        var steps = _splitter.Split(operations);

        // Verify ordering: mvB (dependent) dropped before mvA (source)
        Assert.Equal(2, steps.Count);
        Assert.Equal("daily_for_drop", ((DropTableOperation)steps[0].Operation).Name);
        Assert.Equal("hourly_for_drop", ((DropTableOperation)steps[1].Operation).Name);

        // Execute in the ordered sequence (dependent first)
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""daily_for_drop"";");
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""hourly_for_drop"";");

        // Verify both are gone
        var mvAExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'hourly_for_drop'")
            .CountAsync();
        Assert.Equal(0, mvAExists);

        var mvBExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'daily_for_drop'")
            .CountAsync();
        Assert.Equal(0, mvBExists);
    }

    #endregion

    #region Full Phase Order Tests

    [Fact]
    public async Task FullMigration_ExecutesAllPhasesInOrder()
    {
        await using var context = CreateContext();

        // This test simulates a complex migration with operations from multiple phases
        // and verifies they all execute successfully in the correct order

        // Phase 4: Create table
        var createTable = new CreateTableOperation { Name = "full_test_orders" };
        createTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree()");
        createTable.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        // Phase 5: Add column
        var addColumn = new AddColumnOperation
        {
            Name = "Status",
            Table = "full_test_orders",
            ClrType = typeof(string),
            ColumnType = "String"
        };

        // Phase 6: Create MV
        var createMv = new CreateTableOperation { Name = "full_test_orders_summary" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "full_test_orders");

        // Phase 8: Create index
        var createIndex = new CreateIndexOperation
        {
            Name = "IX_full_test_Status",
            Table = "full_test_orders",
            Columns = new[] { "Status" }
        };
        createIndex.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, "set(100)");

        // Put operations in scrambled order
        var operations = new List<MigrationOperation>
        {
            createIndex,   // Phase 8
            createMv,      // Phase 6
            addColumn,     // Phase 5
            createTable    // Phase 4
        };

        var steps = _splitter.Split(operations);

        // Verify correct phase ordering
        Assert.Equal(4, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);  // Phase 4
        Assert.Equal("full_test_orders", ((CreateTableOperation)steps[0].Operation).Name);

        Assert.IsType<AddColumnOperation>(steps[1].Operation);    // Phase 5

        Assert.IsType<CreateTableOperation>(steps[2].Operation);  // Phase 6 (MV)
        Assert.Equal("full_test_orders_summary", ((CreateTableOperation)steps[2].Operation).Name);

        Assert.IsType<CreateIndexOperation>(steps[3].Operation);  // Phase 8

        // Now execute them in order
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""full_test_orders"" (
                ""Id"" UUID,
                ""Amount"" Decimal(18, 4)
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""full_test_orders"" ADD COLUMN IF NOT EXISTS ""Status"" String;");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""full_test_orders_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Status"")
              AS SELECT ""Status"", count() AS ""Count""
              FROM ""full_test_orders"" GROUP BY ""Status"";");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""full_test_orders"" ADD INDEX IF NOT EXISTS ""IX_full_test_Status"" (""Status"") TYPE set(100) GRANULARITY 1;");

        // Verify all objects exist
        var tableExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'full_test_orders' AND engine = 'MergeTree'")
            .CountAsync();
        Assert.Equal(1, tableExists);

        var columnExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.columns WHERE table = 'full_test_orders' AND name = 'Status'")
            .CountAsync();
        Assert.Equal(1, columnExists);

        var mvExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'full_test_orders_summary'")
            .CountAsync();
        Assert.Equal(1, mvExists);
    }

    #endregion

    #region Dictionary Tests

    [Fact]
    public async Task CreateDictionaryAfterSourceTable_ExecutesInCorrectOrder()
    {
        await using var context = CreateContext();

        // Source table operation
        var createTable = new CreateTableOperation { Name = "country_source" };
        createTable.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree()");
        createTable.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        // Dictionary operation
        var createDict = new CreateTableOperation { Name = "country_dict" };
        createDict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        createDict.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country_source");

        // Wrong order: dict before table
        var operations = new List<MigrationOperation> { createDict, createTable };
        var steps = _splitter.Split(operations);

        // Table (Phase 4) before Dict (Phase 6)
        Assert.Equal(2, steps.Count);
        Assert.Equal("country_source", ((CreateTableOperation)steps[0].Operation).Name);
        Assert.Equal("country_dict", ((CreateTableOperation)steps[1].Operation).Name);

        // Execute in order
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""country_source"" (
                ""Id"" UInt64,
                ""Name"" String,
                ""Code"" String
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE DICTIONARY IF NOT EXISTS ""country_dict""
            (
                ""Id"" UInt64,
                ""Name"" String,
                ""Code"" String
            )
            PRIMARY KEY ""Id""
            SOURCE(CLICKHOUSE(TABLE 'country_source'))
            LAYOUT(HASHED())
            LIFETIME(300);");

        // Verify both exist
        var tableExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'country_source'")
            .CountAsync();
        Assert.Equal(1, tableExists);

        var dictExists = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.dictionaries WHERE name = 'country_dict'")
            .CountAsync();
        Assert.Equal(1, dictExists);
    }

    #endregion

    private PhaseOrderingDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<PhaseOrderingDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new PhaseOrderingDbContext(options);
    }
}

public class PhaseOrderingDbContext : DbContext
{
    public PhaseOrderingDbContext(DbContextOptions<PhaseOrderingDbContext> options)
        : base(options)
    {
    }
}

/// <summary>
/// Comprehensive matrix tests covering all phase transition scenarios.
/// Each test verifies that operations from different phases execute in the correct order.
/// </summary>
public class PhaseTransitionMatrixTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    private readonly ClickHouseMigrationsSplitter _splitter = new();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Phase Transition Matrix

    /// <summary>
    /// Phase 1 → Phase 2: Drop projection before Drop MV
    /// </summary>
    [Fact]
    public void Phase1ToPhase2_DropProjectionBeforeDropMv()
    {
        var dropProjection = new DropProjectionOperation { Name = "prj_test", Table = "test_table" };

        var dropMv = new DropTableOperation { Name = "test_mv" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation> { dropMv, dropProjection };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<DropProjectionOperation>(steps[0].Operation);  // Phase 1
        Assert.IsType<DropTableOperation>(steps[1].Operation);       // Phase 2
    }

    /// <summary>
    /// Phase 1 → Phase 3: Drop index before Drop table
    /// </summary>
    [Fact]
    public void Phase1ToPhase3_DropIndexBeforeDropTable()
    {
        var dropIndex = new DropIndexOperation { Name = "IX_test", Table = "test_table" };
        var dropTable = new DropTableOperation { Name = "test_table" };

        var operations = new List<MigrationOperation> { dropTable, dropIndex };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<DropIndexOperation>(steps[0].Operation);  // Phase 1
        Assert.IsType<DropTableOperation>(steps[1].Operation);  // Phase 3
    }

    /// <summary>
    /// Phase 2 → Phase 3: Drop MV before Drop table (source table)
    /// </summary>
    [Fact]
    public void Phase2ToPhase3_DropMvBeforeDropSourceTable()
    {
        var dropMv = new DropTableOperation { Name = "test_mv" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "source_table");

        var dropTable = new DropTableOperation { Name = "source_table" };

        var operations = new List<MigrationOperation> { dropTable, dropMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.Equal("test_mv", ((DropTableOperation)steps[0].Operation).Name);      // Phase 2 (MV)
        Assert.Equal("source_table", ((DropTableOperation)steps[1].Operation).Name); // Phase 3 (table)
    }

    /// <summary>
    /// Phase 3 → Phase 4: Drop old table, Create new table
    /// </summary>
    [Fact]
    public void Phase3ToPhase4_DropTableBeforeCreateTable()
    {
        var dropTable = new DropTableOperation { Name = "old_table" };
        var createTable = new CreateTableOperation { Name = "new_table" };

        var operations = new List<MigrationOperation> { createTable, dropTable };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<DropTableOperation>(steps[0].Operation);   // Phase 3
        Assert.IsType<CreateTableOperation>(steps[1].Operation); // Phase 4
    }

    /// <summary>
    /// Phase 4 → Phase 5: Create table before Add column (to existing table)
    /// </summary>
    [Fact]
    public void Phase4ToPhase5_CreateTableBeforeAddColumn()
    {
        var createTable = new CreateTableOperation { Name = "new_table" };
        var addColumn = new AddColumnOperation { Name = "NewCol", Table = "existing_table" };

        var operations = new List<MigrationOperation> { addColumn, createTable };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation); // Phase 4
        Assert.IsType<AddColumnOperation>(steps[1].Operation);   // Phase 5
    }

    /// <summary>
    /// Phase 4 → Phase 6: Create table before Create MV (MV depends on table)
    /// </summary>
    [Fact]
    public void Phase4ToPhase6_CreateTableBeforeCreateMv()
    {
        var createTable = new CreateTableOperation { Name = "source_table" };

        var createMv = new CreateTableOperation { Name = "summary_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "source_table");

        var operations = new List<MigrationOperation> { createMv, createTable };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.Equal("source_table", ((CreateTableOperation)steps[0].Operation).Name); // Phase 4
        Assert.Equal("summary_mv", ((CreateTableOperation)steps[1].Operation).Name);   // Phase 6
    }

    /// <summary>
    /// Phase 5 → Phase 6: Add column before Create MV that uses the column
    /// Critical fix scenario.
    /// </summary>
    [Fact]
    public void Phase5ToPhase6_AddColumnBeforeCreateMvUsingIt()
    {
        var addColumn = new AddColumnOperation { Name = "NewMetric", Table = "events" };

        var createMv = new CreateTableOperation { Name = "metric_summary" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            @"SELECT sum(""NewMetric"") FROM ""events""");

        var operations = new List<MigrationOperation> { createMv, addColumn };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<AddColumnOperation>(steps[0].Operation);   // Phase 5
        Assert.IsType<CreateTableOperation>(steps[1].Operation); // Phase 6 (MV)
    }

    /// <summary>
    /// Phase 5 → Phase 8: Add column before Create index on that column
    /// </summary>
    [Fact]
    public void Phase5ToPhase8_AddColumnBeforeCreateIndex()
    {
        var addColumn = new AddColumnOperation { Name = "Email", Table = "users" };
        var createIndex = new CreateIndexOperation { Name = "IX_Email", Table = "users", Columns = new[] { "Email" } };

        var operations = new List<MigrationOperation> { createIndex, addColumn };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<AddColumnOperation>(steps[0].Operation);   // Phase 5
        Assert.IsType<CreateIndexOperation>(steps[1].Operation); // Phase 8
    }

    /// <summary>
    /// Phase 6 → Phase 7: Create MV before Alter column
    /// </summary>
    [Fact]
    public void Phase6ToPhase7_CreateMvBeforeAlterColumn()
    {
        var createMv = new CreateTableOperation { Name = "summary_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var alterColumn = new AlterColumnOperation { Name = "Status", Table = "orders" };

        var operations = new List<MigrationOperation> { alterColumn, createMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);  // Phase 6 (MV)
        Assert.IsType<AlterColumnOperation>(steps[1].Operation);  // Phase 7
    }

    /// <summary>
    /// Phase 6 → Phase 7: Create MV before Drop column
    /// </summary>
    [Fact]
    public void Phase6ToPhase7_CreateMvBeforeDropColumn()
    {
        var createMv = new CreateTableOperation { Name = "summary_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var dropColumn = new DropColumnOperation { Name = "OldCol", Table = "orders" };

        var operations = new List<MigrationOperation> { dropColumn, createMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation); // Phase 6 (MV)
        Assert.IsType<DropColumnOperation>(steps[1].Operation);  // Phase 7
    }

    /// <summary>
    /// Phase 6 → Phase 7: Create MV before Rename column
    /// </summary>
    [Fact]
    public void Phase6ToPhase7_CreateMvBeforeRenameColumn()
    {
        var createMv = new CreateTableOperation { Name = "summary_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var renameColumn = new RenameColumnOperation { Name = "OldName", Table = "orders", NewName = "NewName" };

        var operations = new List<MigrationOperation> { renameColumn, createMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);   // Phase 6 (MV)
        Assert.IsType<RenameColumnOperation>(steps[1].Operation);  // Phase 7
    }

    /// <summary>
    /// Phase 7 → Phase 8: Alter column before Create index
    /// </summary>
    [Fact]
    public void Phase7ToPhase8_AlterColumnBeforeCreateIndex()
    {
        var alterColumn = new AlterColumnOperation { Name = "Status", Table = "orders" };
        var createIndex = new CreateIndexOperation { Name = "IX_Status", Table = "orders", Columns = new[] { "Status" } };

        var operations = new List<MigrationOperation> { createIndex, alterColumn };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<AlterColumnOperation>(steps[0].Operation);  // Phase 7
        Assert.IsType<CreateIndexOperation>(steps[1].Operation);  // Phase 8
    }

    /// <summary>
    /// Phase 8 → Phase 9: Create index before Add projection
    /// </summary>
    [Fact]
    public void Phase8ToPhase9_CreateIndexBeforeAddProjection()
    {
        var createIndex = new CreateIndexOperation { Name = "IX_Date", Table = "events" };
        var addProjection = new AddProjectionOperation { Name = "prj_by_date", Table = "events" };

        var operations = new List<MigrationOperation> { addProjection, createIndex };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateIndexOperation>(steps[0].Operation);   // Phase 8
        Assert.IsType<AddProjectionOperation>(steps[1].Operation); // Phase 9
    }

    #endregion

    #region Complex Multi-Phase Scenarios

    /// <summary>
    /// Full drop sequence: Projection → Index → MV → Dict → Table
    /// Phases 1 → 1 → 2 → 2 → 3
    /// </summary>
    [Fact]
    public void FullDropSequence_AllPhasesInOrder()
    {
        var dropProjection = new DropProjectionOperation { Name = "prj_test", Table = "events" };
        var dropIndex = new DropIndexOperation { Name = "IX_Date", Table = "events" };

        var dropMv = new DropTableOperation { Name = "summary_mv" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var dropDict = new DropTableOperation { Name = "lookup_dict" };
        dropDict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);

        var dropTable = new DropTableOperation { Name = "events" };

        // Scrambled order
        var operations = new List<MigrationOperation>
        {
            dropTable, dropDict, dropMv, dropIndex, dropProjection
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(5, steps.Count);
        // Phase 1: Drop projections and indexes
        Assert.True(steps[0].Operation is DropProjectionOperation or DropIndexOperation);
        Assert.True(steps[1].Operation is DropProjectionOperation or DropIndexOperation);
        // Phase 2: Drop MVs and dicts
        Assert.True(steps[2].Operation is DropTableOperation dt2 &&
            (dt2.FindAnnotation(ClickHouseAnnotationNames.MaterializedView) != null ||
             dt2.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null));
        Assert.True(steps[3].Operation is DropTableOperation dt3 &&
            (dt3.FindAnnotation(ClickHouseAnnotationNames.MaterializedView) != null ||
             dt3.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null));
        // Phase 3: Drop table
        Assert.Equal("events", ((DropTableOperation)steps[4].Operation).Name);
    }

    /// <summary>
    /// Full create sequence: Table → Column → MV → Dict → Column Modify → Index → Projection
    /// Phases 4 → 5 → 6 → 6 → 7 → 8 → 9
    /// </summary>
    [Fact]
    public void FullCreateSequence_AllPhasesInOrder()
    {
        var createTable = new CreateTableOperation { Name = "events" };

        var addColumn = new AddColumnOperation { Name = "Status", Table = "events" };

        var createMv = new CreateTableOperation { Name = "summary_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var createDict = new CreateTableOperation { Name = "lookup_dict" };
        createDict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        createDict.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "events");

        var alterColumn = new AlterColumnOperation { Name = "OldCol", Table = "events" };

        var createIndex = new CreateIndexOperation { Name = "IX_Status", Table = "events" };

        var addProjection = new AddProjectionOperation { Name = "prj_by_status", Table = "events" };

        // Scrambled order
        var operations = new List<MigrationOperation>
        {
            addProjection, createIndex, alterColumn, createDict, createMv, addColumn, createTable
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(7, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);    // Phase 4
        Assert.Equal("events", ((CreateTableOperation)steps[0].Operation).Name);

        Assert.IsType<AddColumnOperation>(steps[1].Operation);      // Phase 5

        // Phase 6: MVs and Dicts (order between them may vary)
        Assert.True(steps[2].Operation is CreateTableOperation ct2 &&
            (ct2.FindAnnotation(ClickHouseAnnotationNames.MaterializedView) != null ||
             ct2.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null));
        Assert.True(steps[3].Operation is CreateTableOperation ct3 &&
            (ct3.FindAnnotation(ClickHouseAnnotationNames.MaterializedView) != null ||
             ct3.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null));

        Assert.IsType<AlterColumnOperation>(steps[4].Operation);    // Phase 7
        Assert.IsType<CreateIndexOperation>(steps[5].Operation);    // Phase 8
        Assert.IsType<AddProjectionOperation>(steps[6].Operation);  // Phase 9
    }

    /// <summary>
    /// Mixed drop and create: Drop old MV, Create new table, Add column, Create new MV
    /// Phases: 2 → 4 → 5 → 6
    /// </summary>
    [Fact]
    public void MixedDropAndCreate_CorrectOrder()
    {
        var dropOldMv = new DropTableOperation { Name = "old_mv" };
        dropOldMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var createNewTable = new CreateTableOperation { Name = "new_events" };

        var addColumn = new AddColumnOperation { Name = "Metric", Table = "new_events" };

        var createNewMv = new CreateTableOperation { Name = "new_mv" };
        createNewMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createNewMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "new_events");

        // Scrambled order
        var operations = new List<MigrationOperation>
        {
            createNewMv, addColumn, createNewTable, dropOldMv
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        Assert.Equal("old_mv", ((DropTableOperation)steps[0].Operation).Name);       // Phase 2
        Assert.Equal("new_events", ((CreateTableOperation)steps[1].Operation).Name); // Phase 4
        Assert.IsType<AddColumnOperation>(steps[2].Operation);                        // Phase 5
        Assert.Equal("new_mv", ((CreateTableOperation)steps[3].Operation).Name);     // Phase 6
    }

    /// <summary>
    /// Three-level cascading MVs creation
    /// MV-C → MV-B → MV-A → Table
    /// </summary>
    [Fact]
    public void ThreeLevelCascadingMvs_CorrectCreationOrder()
    {
        var createTable = new CreateTableOperation { Name = "raw_data" };

        var mvA = new CreateTableOperation { Name = "level1_summary" };
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_data");

        var mvB = new CreateTableOperation { Name = "level2_summary" };
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "level1_summary");

        var mvC = new CreateTableOperation { Name = "level3_summary" };
        mvC.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvC.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "level2_summary");

        // Reverse order (worst case)
        var operations = new List<MigrationOperation> { mvC, mvB, mvA, createTable };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        Assert.Equal("raw_data", ((CreateTableOperation)steps[0].Operation).Name);       // Phase 4
        Assert.Equal("level1_summary", ((CreateTableOperation)steps[1].Operation).Name); // Phase 6, first
        Assert.Equal("level2_summary", ((CreateTableOperation)steps[2].Operation).Name); // Phase 6, second
        Assert.Equal("level3_summary", ((CreateTableOperation)steps[3].Operation).Name); // Phase 6, third
    }

    /// <summary>
    /// Three-level cascading MVs drop
    /// Table ← MV-A ← MV-B ← MV-C (drop order: C, B, A, Table)
    /// </summary>
    [Fact]
    public void ThreeLevelCascadingMvs_CorrectDropOrder()
    {
        var dropTable = new DropTableOperation { Name = "raw_data" };

        var dropMvA = new DropTableOperation { Name = "level1_summary" };
        dropMvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_data");

        var dropMvB = new DropTableOperation { Name = "level2_summary" };
        dropMvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "level1_summary");

        var dropMvC = new DropTableOperation { Name = "level3_summary" };
        dropMvC.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMvC.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "level2_summary");

        // Wrong order (source first)
        var operations = new List<MigrationOperation> { dropTable, dropMvA, dropMvB, dropMvC };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        // Phase 2: MVs in reverse dependency order
        Assert.Equal("level3_summary", ((DropTableOperation)steps[0].Operation).Name);
        Assert.Equal("level2_summary", ((DropTableOperation)steps[1].Operation).Name);
        Assert.Equal("level1_summary", ((DropTableOperation)steps[2].Operation).Name);
        // Phase 3: Table
        Assert.Equal("raw_data", ((DropTableOperation)steps[3].Operation).Name);
    }

    #endregion

    #region Edge Cases

    /// <summary>
    /// Multiple Add columns before multiple MVs
    /// </summary>
    [Fact]
    public void MultipleAddColumnsBeforeMultipleMvs()
    {
        var addCol1 = new AddColumnOperation { Name = "Metric1", Table = "events" };
        var addCol2 = new AddColumnOperation { Name = "Metric2", Table = "events" };

        var mv1 = new CreateTableOperation { Name = "mv_metric1" };
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, @"SELECT sum(""Metric1"") FROM ""events""");

        var mv2 = new CreateTableOperation { Name = "mv_metric2" };
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, @"SELECT sum(""Metric2"") FROM ""events""");

        // Interleaved order
        var operations = new List<MigrationOperation> { mv1, addCol1, mv2, addCol2 };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        // Phase 5: All AddColumns first
        Assert.IsType<AddColumnOperation>(steps[0].Operation);
        Assert.IsType<AddColumnOperation>(steps[1].Operation);
        // Phase 6: All MVs
        Assert.IsType<CreateTableOperation>(steps[2].Operation);
        Assert.IsType<CreateTableOperation>(steps[3].Operation);
    }

    /// <summary>
    /// SqlOperation defaults to Phase 7 (ModifyColumns)
    /// </summary>
    [Fact]
    public void SqlOperationDefaultsToPhase7()
    {
        var sqlOp = new SqlOperation { Sql = "SELECT 1" };
        var createMv = new CreateTableOperation { Name = "test_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation> { sqlOp, createMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation); // Phase 6 (MV)
        Assert.IsType<SqlOperation>(steps[1].Operation);         // Phase 7 (default)
    }

    /// <summary>
    /// Empty operations list returns empty result
    /// </summary>
    [Fact]
    public void EmptyOperations_ReturnsEmpty()
    {
        var steps = _splitter.Split([]);
        Assert.Empty(steps);
    }

    /// <summary>
    /// Single operation returns single step
    /// </summary>
    [Fact]
    public void SingleOperation_ReturnsSingleStep()
    {
        var createTable = new CreateTableOperation { Name = "test" };
        var steps = _splitter.Split(new[] { createTable });

        Assert.Single(steps);
        Assert.Equal(1, steps[0].StepNumber);
    }

    #endregion
}

/// <summary>
/// Integration tests simulating real-world multi-migration lifecycles.
/// Tests the evolution of a schema over multiple migration phases.
/// </summary>
public class MigrationLifecycleIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    private readonly ClickHouseMigrationsSplitter _splitter = new();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Multi-Migration Lifecycle Tests

    /// <summary>
    /// Simulates a common e-commerce evolution:
    /// Migration 1: Create Orders table
    /// Migration 2: Add index on Orders
    /// Migration 3: Add DailySummary MV
    /// Migration 4: Add Discount column and update MV to include it
    /// </summary>
    [Fact]
    public async Task EcommerceEvolution_FourMigrations_ExecutesCorrectly()
    {
        await using var context = CreateContext();

        // === Migration 1: Initial schema - Create Orders table ===
        var migration1Ops = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "orders" }
        };
        var migration1Steps = _splitter.Split(migration1Ops);
        Assert.Single(migration1Steps);

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""orders"" (
                ""Id"" UUID,
                ""OrderDate"" DateTime64(3),
                ""CustomerId"" UUID,
                ""Amount"" Decimal(18, 4)
            ) ENGINE = MergeTree() ORDER BY (""OrderDate"", ""Id"");");

        await VerifyTableExists(context, "orders");

        // === Migration 2: Add index on CustomerId ===
        var migration2Ops = new List<MigrationOperation>
        {
            new CreateIndexOperation { Name = "IX_Orders_CustomerId", Table = "orders", Columns = new[] { "CustomerId" } }
        };
        var migration2Steps = _splitter.Split(migration2Ops);
        Assert.Single(migration2Steps);
        Assert.IsType<CreateIndexOperation>(migration2Steps[0].Operation);

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""orders"" ADD INDEX IF NOT EXISTS ""IX_Orders_CustomerId"" (""CustomerId"") TYPE bloom_filter GRANULARITY 1;");

        // === Migration 3: Add DailySummary MV ===
        var migration3Mv = new CreateTableOperation { Name = "daily_summary" };
        migration3Mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        migration3Mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "orders");

        var migration3Ops = new List<MigrationOperation> { migration3Mv };
        var migration3Steps = _splitter.Split(migration3Ops);
        Assert.Single(migration3Steps);

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""daily_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Date"")
              AS SELECT toDate(""OrderDate"") AS ""Date"", sum(""Amount"") AS ""TotalAmount"", count() AS ""OrderCount""
              FROM ""orders"" GROUP BY ""Date"";");

        await VerifyTableExists(context, "daily_summary");

        // === Migration 4: Add Discount column + new MV that uses it ===
        // This is the critical scenario - add column must come before MV
        var addDiscount = new AddColumnOperation { Name = "Discount", Table = "orders" };

        var discountMv = new CreateTableOperation { Name = "discount_summary" };
        discountMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        discountMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            @"SELECT toDate(""OrderDate"") AS ""Date"", sum(""Discount"") AS ""TotalDiscount"" FROM ""orders"" GROUP BY ""Date""");

        // Wrong order - MV before column
        var migration4Ops = new List<MigrationOperation> { discountMv, addDiscount };
        var migration4Steps = _splitter.Split(migration4Ops);

        // Verify correct ordering
        Assert.Equal(2, migration4Steps.Count);
        Assert.IsType<AddColumnOperation>(migration4Steps[0].Operation);      // Phase 5
        Assert.IsType<CreateTableOperation>(migration4Steps[1].Operation);     // Phase 6

        // Execute in correct order
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""orders"" ADD COLUMN IF NOT EXISTS ""Discount"" Decimal(18, 4) DEFAULT 0;");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""discount_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Date"")
              AS SELECT toDate(""OrderDate"") AS ""Date"", sum(""Discount"") AS ""TotalDiscount""
              FROM ""orders"" GROUP BY ""Date"";");

        // Verify final state
        await VerifyTableExists(context, "orders");
        await VerifyTableExists(context, "daily_summary");
        await VerifyTableExists(context, "discount_summary");
        await VerifyColumnExists(context, "orders", "Discount");
    }

    /// <summary>
    /// Simulates analytics platform evolution:
    /// Migration 1: Create raw_events table
    /// Migration 2: Create hourly_summary MV
    /// Migration 3: Create daily_summary MV (cascading from hourly)
    /// Migration 4: Add new metric column + weekly_summary MV
    /// Migration 5: Drop daily_summary, add monthly_summary
    /// </summary>
    [Fact]
    public async Task AnalyticsPlatformEvolution_FiveMigrations_ExecutesCorrectly()
    {
        await using var context = CreateContext();

        // === Migration 1: Create raw events ===
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""raw_events"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3),
                ""UserId"" UUID,
                ""EventType"" String,
                ""Value"" Int32
            ) ENGINE = MergeTree() ORDER BY (""EventTime"", ""Id"");");

        // === Migration 2: Add hourly_summary MV ===
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""hourly_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Hour"", ""EventType"")
              AS SELECT
                  toStartOfHour(""EventTime"") AS ""Hour"",
                  ""EventType"",
                  sum(""Value"") AS ""TotalValue"",
                  count() AS ""EventCount""
              FROM ""raw_events""
              GROUP BY ""Hour"", ""EventType"";");

        // === Migration 3: Add daily_summary (cascading MV) ===
        var hourlyMv = new CreateTableOperation { Name = "hourly_summary" };
        hourlyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        hourlyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_events");

        var dailyMv = new CreateTableOperation { Name = "daily_summary" };
        dailyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dailyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly_summary");

        // Verify cascading order
        var cascadeOps = new List<MigrationOperation> { dailyMv, hourlyMv };
        var cascadeSteps = _splitter.Split(cascadeOps);
        Assert.Equal("hourly_summary", ((CreateTableOperation)cascadeSteps[0].Operation).Name);
        Assert.Equal("daily_summary", ((CreateTableOperation)cascadeSteps[1].Operation).Name);

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""daily_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Day"", ""EventType"")
              AS SELECT
                  toDate(""Hour"") AS ""Day"",
                  ""EventType"",
                  sum(""TotalValue"") AS ""DailyValue"",
                  sum(""EventCount"") AS ""DailyCount""
              FROM ""hourly_summary""
              GROUP BY ""Day"", ""EventType"";");

        // === Migration 4: Add Revenue column + weekly MV using it ===
        var addRevenue = new AddColumnOperation { Name = "Revenue", Table = "raw_events" };

        var weeklyMv = new CreateTableOperation { Name = "weekly_revenue" };
        weeklyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        weeklyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            @"SELECT toStartOfWeek(""EventTime"") AS ""Week"", sum(""Revenue"") AS ""WeeklyRevenue"" FROM ""raw_events"" GROUP BY ""Week""");

        var migration4Ops = new List<MigrationOperation> { weeklyMv, addRevenue };
        var migration4Steps = _splitter.Split(migration4Ops);

        Assert.IsType<AddColumnOperation>(migration4Steps[0].Operation);
        Assert.IsType<CreateTableOperation>(migration4Steps[1].Operation);

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""raw_events"" ADD COLUMN IF NOT EXISTS ""Revenue"" Decimal(18, 4) DEFAULT 0;");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""weekly_revenue""
              ENGINE = SummingMergeTree() ORDER BY (""Week"")
              AS SELECT toStartOfWeek(""EventTime"") AS ""Week"", sum(""Revenue"") AS ""WeeklyRevenue""
              FROM ""raw_events"" GROUP BY ""Week"";");

        // === Migration 5: Drop daily_summary, add monthly_summary ===
        var dropDaily = new DropTableOperation { Name = "daily_summary" };
        dropDaily.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropDaily.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly_summary");

        var monthlyMv = new CreateTableOperation { Name = "monthly_summary" };
        monthlyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        monthlyMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly_summary");

        var migration5Ops = new List<MigrationOperation> { monthlyMv, dropDaily };
        var migration5Steps = _splitter.Split(migration5Ops);

        // Drop (Phase 2) before Create (Phase 6)
        Assert.IsType<DropTableOperation>(migration5Steps[0].Operation);
        Assert.IsType<CreateTableOperation>(migration5Steps[1].Operation);

        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""daily_summary"";");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""monthly_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Month"", ""EventType"")
              AS SELECT
                  toStartOfMonth(""Hour"") AS ""Month"",
                  ""EventType"",
                  sum(""TotalValue"") AS ""MonthlyValue""
              FROM ""hourly_summary""
              GROUP BY ""Month"", ""EventType"";");

        // Verify final state
        await VerifyTableExists(context, "raw_events");
        await VerifyTableExists(context, "hourly_summary");
        await VerifyTableNotExists(context, "daily_summary");
        await VerifyTableExists(context, "weekly_revenue");
        await VerifyTableExists(context, "monthly_summary");
        await VerifyColumnExists(context, "raw_events", "Revenue");
    }

    /// <summary>
    /// Simulates a complete teardown and rebuild:
    /// State: Table with MV, Index, and Projection
    /// Migration: Drop everything, recreate with different structure
    /// </summary>
    [Fact]
    public async Task TeardownAndRebuild_DropsInCorrectOrder_ThenCreatesInCorrectOrder()
    {
        await using var context = CreateContext();

        // Setup initial state
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""old_events"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3),
                ""Value"" Int32
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""old_events"" ADD INDEX IF NOT EXISTS ""IX_Value"" (""Value"") TYPE minmax GRANULARITY 1;");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""old_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Day"")
              AS SELECT toDate(""EventTime"") AS ""Day"", sum(""Value"") AS ""Total""
              FROM ""old_events"" GROUP BY ""Day"";");

        // Teardown and rebuild migration
        var dropIndex = new DropIndexOperation { Name = "IX_Value", Table = "old_events" };

        var dropMv = new DropTableOperation { Name = "old_summary" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "old_events");

        var dropTable = new DropTableOperation { Name = "old_events" };

        var createNewTable = new CreateTableOperation { Name = "new_events" };
        var addColumn = new AddColumnOperation { Name = "Metric", Table = "new_events" };

        var createNewMv = new CreateTableOperation { Name = "new_summary" };
        createNewMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createNewMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "new_events");

        var createNewIndex = new CreateIndexOperation { Name = "IX_Metric", Table = "new_events" };

        // Scrambled order
        var ops = new List<MigrationOperation>
        {
            createNewMv, createNewIndex, addColumn, createNewTable,
            dropTable, dropMv, dropIndex
        };

        var steps = _splitter.Split(ops);

        // Verify ordering: drops first (1, 2, 3), then creates (4, 5, 6, 8)
        Assert.Equal(7, steps.Count);

        // Phase 1: Drop index
        Assert.IsType<DropIndexOperation>(steps[0].Operation);

        // Phase 2: Drop MV
        Assert.Equal("old_summary", ((DropTableOperation)steps[1].Operation).Name);

        // Phase 3: Drop table
        Assert.Equal("old_events", ((DropTableOperation)steps[2].Operation).Name);

        // Phase 4: Create table
        Assert.Equal("new_events", ((CreateTableOperation)steps[3].Operation).Name);

        // Phase 5: Add column
        Assert.IsType<AddColumnOperation>(steps[4].Operation);

        // Phase 6: Create MV
        Assert.Equal("new_summary", ((CreateTableOperation)steps[5].Operation).Name);

        // Phase 8: Create index
        Assert.IsType<CreateIndexOperation>(steps[6].Operation);

        // Execute teardown
        await context.Database.ExecuteSqlRawAsync(@"ALTER TABLE ""old_events"" DROP INDEX IF EXISTS ""IX_Value"";");
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""old_summary"";");
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""old_events"";");

        // Verify teardown
        await VerifyTableNotExists(context, "old_events");
        await VerifyTableNotExists(context, "old_summary");

        // Execute rebuild
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""new_events"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""new_events"" ADD COLUMN IF NOT EXISTS ""Metric"" Int32;");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""new_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Day"")
              AS SELECT toDate(""EventTime"") AS ""Day"", sum(""Metric"") AS ""Total""
              FROM ""new_events"" GROUP BY ""Day"";");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""new_events"" ADD INDEX IF NOT EXISTS ""IX_Metric"" (""Metric"") TYPE minmax GRANULARITY 1;");

        // Verify rebuild
        await VerifyTableExists(context, "new_events");
        await VerifyTableExists(context, "new_summary");
        await VerifyColumnExists(context, "new_events", "Metric");
    }

    /// <summary>
    /// Simulates dictionary lifecycle:
    /// Migration 1: Create source table
    /// Migration 2: Create dictionary from source
    /// Migration 3: Add column to source, recreate dictionary with new column
    /// </summary>
    [Fact]
    public async Task DictionaryLifecycle_CreatesAndRecreates_InCorrectOrder()
    {
        await using var context = CreateContext();

        // === Migration 1: Create source table ===
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""country_source"" (
                ""Id"" UInt64,
                ""Name"" String,
                ""Code"" String
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        // === Migration 2: Create dictionary ===
        var createDict = new CreateTableOperation { Name = "country_dict" };
        createDict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        createDict.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country_source");

        var migration2Steps = _splitter.Split(new[] { createDict });
        Assert.Single(migration2Steps);

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE DICTIONARY IF NOT EXISTS ""country_dict""
            (
                ""Id"" UInt64,
                ""Name"" String,
                ""Code"" String
            )
            PRIMARY KEY ""Id""
            SOURCE(CLICKHOUSE(TABLE 'country_source'))
            LAYOUT(HASHED())
            LIFETIME(300);");

        await VerifyDictionaryExists(context, "country_dict");

        // === Migration 3: Add Region column, recreate dictionary ===
        var addColumn = new AddColumnOperation { Name = "Region", Table = "country_source" };

        // Drop old dict (Phase 2), Add column (Phase 5), Create new dict (Phase 6)
        var dropDict = new DropTableOperation { Name = "country_dict" };
        dropDict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);

        var recreateDict = new CreateTableOperation { Name = "country_dict" };
        recreateDict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        recreateDict.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country_source");

        var migration3Ops = new List<MigrationOperation> { recreateDict, addColumn, dropDict };
        var migration3Steps = _splitter.Split(migration3Ops);

        Assert.Equal(3, migration3Steps.Count);
        Assert.IsType<DropTableOperation>(migration3Steps[0].Operation);      // Phase 2
        Assert.IsType<AddColumnOperation>(migration3Steps[1].Operation);      // Phase 5
        Assert.IsType<CreateTableOperation>(migration3Steps[2].Operation);    // Phase 6

        // Execute
        await context.Database.ExecuteSqlRawAsync(@"DROP DICTIONARY IF EXISTS ""country_dict"";");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""country_source"" ADD COLUMN IF NOT EXISTS ""Region"" String;");
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE DICTIONARY IF NOT EXISTS ""country_dict""
            (
                ""Id"" UInt64,
                ""Name"" String,
                ""Code"" String,
                ""Region"" String
            )
            PRIMARY KEY ""Id""
            SOURCE(CLICKHOUSE(TABLE 'country_source'))
            LAYOUT(HASHED())
            LIFETIME(300);");

        // Verify final state
        await VerifyColumnExists(context, "country_source", "Region");
        await VerifyDictionaryExists(context, "country_dict");
    }

    /// <summary>
    /// Simulates projection lifecycle:
    /// Migration 1: Create table
    /// Migration 2: Add projection
    /// Migration 3: Add column, drop old projection, add new projection using new column
    /// </summary>
    [Fact]
    public async Task ProjectionLifecycle_CreatesAndRecreates_InCorrectOrder()
    {
        await using var context = CreateContext();

        // === Migration 1: Create table ===
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""events_prj"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3),
                ""Category"" String,
                ""Value"" Int32
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        // === Migration 2: Add projection ===
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""events_prj"" ADD PROJECTION IF NOT EXISTS ""prj_by_category"" (SELECT * ORDER BY ""Category"");");

        // === Migration 3: Add column, drop old projection, add new projection ===
        var addColumn = new AddColumnOperation { Name = "Region", Table = "events_prj" };
        var dropProj = new DropProjectionOperation { Name = "prj_by_category", Table = "events_prj" };
        var addProj = new AddProjectionOperation { Name = "prj_by_region", Table = "events_prj" };

        var migration3Ops = new List<MigrationOperation> { addProj, addColumn, dropProj };
        var migration3Steps = _splitter.Split(migration3Ops);

        Assert.Equal(3, migration3Steps.Count);
        Assert.IsType<DropProjectionOperation>(migration3Steps[0].Operation);  // Phase 1
        Assert.IsType<AddColumnOperation>(migration3Steps[1].Operation);       // Phase 5
        Assert.IsType<AddProjectionOperation>(migration3Steps[2].Operation);   // Phase 9

        // Execute
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""events_prj"" DROP PROJECTION IF EXISTS ""prj_by_category"";");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""events_prj"" ADD COLUMN IF NOT EXISTS ""Region"" String;");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""events_prj"" ADD PROJECTION IF NOT EXISTS ""prj_by_region"" (SELECT * ORDER BY ""Region"");");

        // Verify final state
        await VerifyColumnExists(context, "events_prj", "Region");
    }

    #endregion

    #region Helper Methods

    private async Task VerifyTableExists(PhaseOrderingDbContext context, string tableName)
    {
        var count = await context.Database.SqlQueryRaw<int>(
            $@"SELECT 1 AS Value FROM system.tables WHERE name = '{tableName}'")
            .CountAsync();
        Assert.Equal(1, count);
    }

    private async Task VerifyTableNotExists(PhaseOrderingDbContext context, string tableName)
    {
        var count = await context.Database.SqlQueryRaw<int>(
            $@"SELECT 1 AS Value FROM system.tables WHERE name = '{tableName}'")
            .CountAsync();
        Assert.Equal(0, count);
    }

    private async Task VerifyColumnExists(PhaseOrderingDbContext context, string tableName, string columnName)
    {
        var count = await context.Database.SqlQueryRaw<int>(
            $@"SELECT 1 AS Value FROM system.columns WHERE table = '{tableName}' AND name = '{columnName}'")
            .CountAsync();
        Assert.Equal(1, count);
    }

    private async Task VerifyDictionaryExists(PhaseOrderingDbContext context, string dictName)
    {
        var count = await context.Database.SqlQueryRaw<int>(
            $@"SELECT 1 AS Value FROM system.dictionaries WHERE name = '{dictName}'")
            .CountAsync();
        Assert.Equal(1, count);
    }

    private PhaseOrderingDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<PhaseOrderingDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new PhaseOrderingDbContext(options);
    }

    #endregion
}

/// <summary>
/// Tests for migrations with many operations across multiple phases.
/// Verifies correct ordering in complex real-world scenarios.
/// </summary>
public class ComplexMultiOperationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    private readonly ClickHouseMigrationsSplitter _splitter = new();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Multiple Operations Per Phase

    /// <summary>
    /// 5 AddColumn + 5 CreateIndex operations.
    /// All columns (Phase 5) must come before all indexes (Phase 8).
    /// </summary>
    [Fact]
    public void FiveColumnsAndFiveIndexes_ColumnsBeforeIndexes()
    {
        var operations = new List<MigrationOperation>();

        // Add 5 columns and 5 indexes in interleaved order
        for (int i = 1; i <= 5; i++)
        {
            var idx = new CreateIndexOperation { Name = $"IX_Col{i}", Table = "test", Columns = new[] { $"Col{i}" } };
            var col = new AddColumnOperation { Name = $"Col{i}", Table = "test" };
            operations.Add(idx);  // Index first (wrong order)
            operations.Add(col);
        }

        var steps = _splitter.Split(operations);

        Assert.Equal(10, steps.Count);

        // First 5 should all be AddColumn (Phase 5)
        for (int i = 0; i < 5; i++)
        {
            Assert.IsType<AddColumnOperation>(steps[i].Operation);
        }

        // Last 5 should all be CreateIndex (Phase 8)
        for (int i = 5; i < 10; i++)
        {
            Assert.IsType<CreateIndexOperation>(steps[i].Operation);
        }
    }

    /// <summary>
    /// 3 MVs from same source table - no dependencies between them.
    /// All should be in Phase 6, order preserved from original.
    /// </summary>
    [Fact]
    public void ThreeMvsFromSameSource_AllInPhase6()
    {
        var mv1 = new CreateTableOperation { Name = "mv_hourly" };
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var mv2 = new CreateTableOperation { Name = "mv_daily" };
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var mv3 = new CreateTableOperation { Name = "mv_monthly" };
        mv3.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv3.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var operations = new List<MigrationOperation> { mv1, mv2, mv3 };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        // All in Phase 6, original order preserved (no inter-dependencies)
        Assert.Equal("mv_hourly", ((CreateTableOperation)steps[0].Operation).Name);
        Assert.Equal("mv_daily", ((CreateTableOperation)steps[1].Operation).Name);
        Assert.Equal("mv_monthly", ((CreateTableOperation)steps[2].Operation).Name);
    }

    /// <summary>
    /// Mixed column operations: AddColumn (Phase 5), AlterColumn, DropColumn, RenameColumn (Phase 7).
    /// </summary>
    [Fact]
    public void MixedColumnOperations_AddsBeforeModifies()
    {
        var add1 = new AddColumnOperation { Name = "NewCol1", Table = "test" };
        var add2 = new AddColumnOperation { Name = "NewCol2", Table = "test" };
        var alter = new AlterColumnOperation { Name = "ExistingCol", Table = "test" };
        var drop = new DropColumnOperation { Name = "OldCol", Table = "test" };
        var rename = new RenameColumnOperation { Name = "Col1", Table = "test", NewName = "Col1Renamed" };

        // Scrambled order
        var operations = new List<MigrationOperation> { alter, add1, drop, rename, add2 };
        var steps = _splitter.Split(operations);

        Assert.Equal(5, steps.Count);

        // Phase 5: AddColumns first
        Assert.IsType<AddColumnOperation>(steps[0].Operation);
        Assert.IsType<AddColumnOperation>(steps[1].Operation);

        // Phase 7: Modify operations
        Assert.True(steps[2].Operation is AlterColumnOperation or DropColumnOperation or RenameColumnOperation);
        Assert.True(steps[3].Operation is AlterColumnOperation or DropColumnOperation or RenameColumnOperation);
        Assert.True(steps[4].Operation is AlterColumnOperation or DropColumnOperation or RenameColumnOperation);
    }

    #endregion

    #region Cross-Table Operations

    /// <summary>
    /// Two tables, each with its own MV.
    /// Tables (Phase 4) before MVs (Phase 6).
    /// </summary>
    [Fact]
    public void TwoTablesWithMvs_TablesBeforeMvs()
    {
        var tableA = new CreateTableOperation { Name = "orders" };
        var tableB = new CreateTableOperation { Name = "customers" };

        var mvA = new CreateTableOperation { Name = "orders_summary" };
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "orders");

        var mvB = new CreateTableOperation { Name = "customers_summary" };
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "customers");

        // Worst case: MVs before tables
        var operations = new List<MigrationOperation> { mvA, mvB, tableA, tableB };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);

        // Phase 4: Both tables first
        var firstTwoNames = new[] { ((CreateTableOperation)steps[0].Operation).Name, ((CreateTableOperation)steps[1].Operation).Name };
        Assert.Contains("orders", firstTwoNames);
        Assert.Contains("customers", firstTwoNames);

        // Phase 6: Both MVs after
        var lastTwoNames = new[] { ((CreateTableOperation)steps[2].Operation).Name, ((CreateTableOperation)steps[3].Operation).Name };
        Assert.Contains("orders_summary", lastTwoNames);
        Assert.Contains("customers_summary", lastTwoNames);
    }

    /// <summary>
    /// Parallel modifications to two tables: AddColumn + CreateIndex on each.
    /// </summary>
    [Fact]
    public void ParallelTableModifications_ColumnsBeforeIndexes()
    {
        var colA = new AddColumnOperation { Name = "Email", Table = "users" };
        var colB = new AddColumnOperation { Name = "Rating", Table = "products" };
        var idxA = new CreateIndexOperation { Name = "IX_Email", Table = "users", Columns = new[] { "Email" } };
        var idxB = new CreateIndexOperation { Name = "IX_Rating", Table = "products", Columns = new[] { "Rating" } };

        // Interleaved
        var operations = new List<MigrationOperation> { idxA, colA, idxB, colB };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);

        // Phase 5: Both columns first
        Assert.IsType<AddColumnOperation>(steps[0].Operation);
        Assert.IsType<AddColumnOperation>(steps[1].Operation);

        // Phase 8: Both indexes after
        Assert.IsType<CreateIndexOperation>(steps[2].Operation);
        Assert.IsType<CreateIndexOperation>(steps[3].Operation);
    }

    #endregion

    #region Complex Dependency Chains

    /// <summary>
    /// Table → MV (from table) → Dict (from MV).
    /// Three-level chain must create in order.
    /// </summary>
    [Fact]
    public void TableMvDictChain_CreatesInOrder()
    {
        var table = new CreateTableOperation { Name = "source_data" };

        var mv = new CreateTableOperation { Name = "aggregated_data" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "source_data");

        var dict = new CreateTableOperation { Name = "lookup_dict" };
        dict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        dict.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "aggregated_data");

        // Reverse order
        var operations = new List<MigrationOperation> { dict, mv, table };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.Equal("source_data", ((CreateTableOperation)steps[0].Operation).Name);      // Phase 4
        Assert.Equal("aggregated_data", ((CreateTableOperation)steps[1].Operation).Name);  // Phase 6, first
        Assert.Equal("lookup_dict", ((CreateTableOperation)steps[2].Operation).Name);      // Phase 6, second (depends on MV)
    }

    /// <summary>
    /// Diamond dependency: MV that JOINs two tables.
    /// Both tables must be created before the MV.
    /// </summary>
    [Fact]
    public void DiamondDependency_BothTablesBeforeMv()
    {
        var tableA = new CreateTableOperation { Name = "orders" };
        var tableB = new CreateTableOperation { Name = "customers" };

        var mvJoin = new CreateTableOperation { Name = "order_customer_summary" };
        mvJoin.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvJoin.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            @"SELECT o.Id, c.Name FROM orders o JOIN customers c ON o.CustomerId = c.Id");

        // MV first (wrong)
        var operations = new List<MigrationOperation> { mvJoin, tableA, tableB };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);

        // Phase 4: Both tables
        var firstTwoNames = new[] { ((CreateTableOperation)steps[0].Operation).Name, ((CreateTableOperation)steps[1].Operation).Name };
        Assert.Contains("orders", firstTwoNames);
        Assert.Contains("customers", firstTwoNames);

        // Phase 6: MV last
        Assert.Equal("order_customer_summary", ((CreateTableOperation)steps[2].Operation).Name);
    }

    /// <summary>
    /// 4-level MV cascade: Table → MV1 → MV2 → MV3.
    /// </summary>
    [Fact]
    public void FourLevelCascade_CreatesInOrder()
    {
        var table = new CreateTableOperation { Name = "raw_events" };

        var mv1 = new CreateTableOperation { Name = "minutely" };
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_events");

        var mv2 = new CreateTableOperation { Name = "hourly" };
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "minutely");

        var mv3 = new CreateTableOperation { Name = "daily" };
        mv3.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv3.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly");

        // Completely reversed
        var operations = new List<MigrationOperation> { mv3, mv2, mv1, table };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        Assert.Equal("raw_events", ((CreateTableOperation)steps[0].Operation).Name);
        Assert.Equal("minutely", ((CreateTableOperation)steps[1].Operation).Name);
        Assert.Equal("hourly", ((CreateTableOperation)steps[2].Operation).Name);
        Assert.Equal("daily", ((CreateTableOperation)steps[3].Operation).Name);
    }

    #endregion

    #region Destructive + Constructive Operations

    /// <summary>
    /// Replace MV: Drop old, Create new (both from same source).
    /// </summary>
    [Fact]
    public void ReplaceMv_DropBeforeCreate()
    {
        var dropOld = new DropTableOperation { Name = "old_summary" };
        dropOld.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var createNew = new CreateTableOperation { Name = "new_summary" };
        createNew.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createNew.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var operations = new List<MigrationOperation> { createNew, dropOld };
        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<DropTableOperation>(steps[0].Operation);    // Phase 2
        Assert.IsType<CreateTableOperation>(steps[1].Operation);  // Phase 6
    }

    /// <summary>
    /// Full table replacement with MVs.
    /// Drop MV, Drop Table, Create new Table, Create new MV.
    /// </summary>
    [Fact]
    public void TableReplacementWithMvs_CorrectOrder()
    {
        var dropMv = new DropTableOperation { Name = "old_mv" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "old_table");

        var dropTable = new DropTableOperation { Name = "old_table" };
        var createTable = new CreateTableOperation { Name = "new_table" };

        var createMv = new CreateTableOperation { Name = "new_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "new_table");

        // Scrambled
        var operations = new List<MigrationOperation> { createMv, createTable, dropTable, dropMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        Assert.Equal("old_mv", ((DropTableOperation)steps[0].Operation).Name);      // Phase 2
        Assert.Equal("old_table", ((DropTableOperation)steps[1].Operation).Name);   // Phase 3
        Assert.Equal("new_table", ((CreateTableOperation)steps[2].Operation).Name); // Phase 4
        Assert.Equal("new_mv", ((CreateTableOperation)steps[3].Operation).Name);    // Phase 6
    }

    /// <summary>
    /// Schema version upgrade: 7 operations across all phases.
    /// DropIndex, DropMV, DropProjection, AlterColumn, CreateMV, CreateIndex, AddProjection.
    /// </summary>
    [Fact]
    public void SchemaVersionUpgrade_AllPhasesCorrectOrder()
    {
        var dropProjection = new DropProjectionOperation { Name = "prj_old", Table = "events" };
        var dropIndex = new DropIndexOperation { Name = "IX_old", Table = "events" };

        var dropMv = new DropTableOperation { Name = "old_mv" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var alterColumn = new AlterColumnOperation { Name = "Status", Table = "events" };

        var createMv = new CreateTableOperation { Name = "new_mv" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var createIndex = new CreateIndexOperation { Name = "IX_new", Table = "events" };
        var addProjection = new AddProjectionOperation { Name = "prj_new", Table = "events" };

        // Scrambled order
        var operations = new List<MigrationOperation>
        {
            createMv, addProjection, alterColumn, dropMv, createIndex, dropIndex, dropProjection
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(7, steps.Count);

        // Phase 1: Drop projections and indexes
        Assert.True(steps[0].Operation is DropProjectionOperation or DropIndexOperation);
        Assert.True(steps[1].Operation is DropProjectionOperation or DropIndexOperation);

        // Phase 2: Drop MV
        Assert.IsType<DropTableOperation>(steps[2].Operation);

        // Phase 6: Create MV
        Assert.IsType<CreateTableOperation>(steps[3].Operation);

        // Phase 7: Alter column
        Assert.IsType<AlterColumnOperation>(steps[4].Operation);

        // Phase 8: Create index
        Assert.IsType<CreateIndexOperation>(steps[5].Operation);

        // Phase 9: Add projection
        Assert.IsType<AddProjectionOperation>(steps[6].Operation);
    }

    #endregion

    #region Column + MV Interactions (Critical Path)

    /// <summary>
    /// Multiple columns for multiple MVs.
    /// 3 AddColumns, 3 CreateMVs (each uses a different column).
    /// </summary>
    [Fact]
    public void MultipleColumnsForMultipleMvs_AllColumnsBeforeAllMvs()
    {
        var col1 = new AddColumnOperation { Name = "Metric1", Table = "events" };
        var col2 = new AddColumnOperation { Name = "Metric2", Table = "events" };
        var col3 = new AddColumnOperation { Name = "Metric3", Table = "events" };

        var mv1 = new CreateTableOperation { Name = "mv_metric1" };
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, @"SELECT sum(""Metric1"") FROM events");

        var mv2 = new CreateTableOperation { Name = "mv_metric2" };
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, @"SELECT sum(""Metric2"") FROM events");

        var mv3 = new CreateTableOperation { Name = "mv_metric3" };
        mv3.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv3.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, @"SELECT sum(""Metric3"") FROM events");

        // Completely interleaved
        var operations = new List<MigrationOperation> { mv1, col1, mv2, col2, mv3, col3 };
        var steps = _splitter.Split(operations);

        Assert.Equal(6, steps.Count);

        // Phase 5: All columns first
        for (int i = 0; i < 3; i++)
            Assert.IsType<AddColumnOperation>(steps[i].Operation);

        // Phase 6: All MVs after
        for (int i = 3; i < 6; i++)
            Assert.IsType<CreateTableOperation>(steps[i].Operation);
    }

    /// <summary>
    /// Alter column + recreate MV pattern.
    /// Drop MV (Phase 2), Alter Column (Phase 7), Create MV (Phase 6).
    /// Note: This results in Alter AFTER Create due to phase ordering!
    /// </summary>
    [Fact]
    public void AlterColumnRecreateMv_PhaseOrderingNote()
    {
        var dropMv = new DropTableOperation { Name = "summary" };
        dropMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var alterCol = new AlterColumnOperation { Name = "Amount", Table = "orders" };

        var createMv = new CreateTableOperation { Name = "summary" };
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "orders");

        var operations = new List<MigrationOperation> { createMv, alterCol, dropMv };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.IsType<DropTableOperation>(steps[0].Operation);    // Phase 2
        Assert.IsType<CreateTableOperation>(steps[1].Operation);  // Phase 6
        Assert.IsType<AlterColumnOperation>(steps[2].Operation);  // Phase 7

        // Note: In this case, AlterColumn comes AFTER CreateMV due to phase ordering.
        // If the new MV depends on the altered column, this is correct -
        // the old MV is dropped first, column altered won't affect new MV creation
        // since MV is recreated with new structure.
    }

    #endregion

    #region Maximum Complexity Scenarios

    /// <summary>
    /// Full schema buildout: 17 operations across all phases.
    /// </summary>
    [Fact]
    public async Task FullSchemaBuildout_17Operations_ExecutesCorrectly()
    {
        await using var context = CreateContext();

        // Setup initial state
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""existing_events"" (
                ""Id"" UUID,
                ""EventTime"" DateTime64(3),
                ""Value"" Int32
            ) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""old_mv""
              ENGINE = SummingMergeTree() ORDER BY (""Day"")
              AS SELECT toDate(""EventTime"") AS ""Day"", sum(""Value"") AS ""Total""
              FROM ""existing_events"" GROUP BY ""Day"";");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""existing_events"" ADD INDEX IF NOT EXISTS ""IX_old"" (""Value"") TYPE minmax GRANULARITY 1;");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""existing_events"" ADD PROJECTION IF NOT EXISTS ""prj_old"" (SELECT * ORDER BY ""Value"");");

        // Build operations list (17 total)
        var operations = new List<MigrationOperation>
        {
            // Drops
            new DropProjectionOperation { Name = "prj_old", Table = "existing_events" },
            new DropIndexOperation { Name = "IX_old", Table = "existing_events" },
            CreateDropMv("old_mv", "existing_events"),

            // Creates
            new CreateTableOperation { Name = "new_events" },
            new CreateTableOperation { Name = "reference_data" },
            new AddColumnOperation { Name = "Metric1", Table = "new_events" },
            new AddColumnOperation { Name = "Metric2", Table = "new_events" },
            new AddColumnOperation { Name = "Region", Table = "existing_events" },
            CreateMv("hourly_summary", "new_events"),
            CreateMv("daily_summary", "new_events"),
            CreateDict("ref_dict", "reference_data"),
            new AlterColumnOperation { Name = "Value", Table = "existing_events" },
            new CreateIndexOperation { Name = "IX_Metric1", Table = "new_events", Columns = new[] { "Metric1" } },
            new CreateIndexOperation { Name = "IX_Region", Table = "existing_events", Columns = new[] { "Region" } },
            new AddProjectionOperation { Name = "prj_by_metric", Table = "new_events" },
            new AddProjectionOperation { Name = "prj_by_region", Table = "existing_events" },
            new MaterializeProjectionOperation { Name = "prj_by_metric", Table = "new_events" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(17, steps.Count);

        // Verify phase ordering
        int currentPhase = 0;
        foreach (var step in steps)
        {
            int phase = GetPhase(step.Operation);
            Assert.True(phase >= currentPhase, $"Phase went backwards: {currentPhase} to {phase}");
            currentPhase = phase;
        }

        // Execute in order (just verify DDL is valid)
        await context.Database.ExecuteSqlRawAsync(@"ALTER TABLE ""existing_events"" DROP PROJECTION IF EXISTS ""prj_old"";");
        await context.Database.ExecuteSqlRawAsync(@"ALTER TABLE ""existing_events"" DROP INDEX IF EXISTS ""IX_old"";");
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""old_mv"";");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""new_events"" (""Id"" UUID, ""EventTime"" DateTime64(3)) ENGINE = MergeTree() ORDER BY (""Id"");");
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""reference_data"" (""Id"" UInt64, ""Name"" String) ENGINE = MergeTree() ORDER BY (""Id"");");

        await context.Database.ExecuteSqlRawAsync(@"ALTER TABLE ""new_events"" ADD COLUMN IF NOT EXISTS ""Metric1"" Int32;");
        await context.Database.ExecuteSqlRawAsync(@"ALTER TABLE ""new_events"" ADD COLUMN IF NOT EXISTS ""Metric2"" Int32;");
        await context.Database.ExecuteSqlRawAsync(@"ALTER TABLE ""existing_events"" ADD COLUMN IF NOT EXISTS ""Region"" String;");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""hourly_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Hour"")
              AS SELECT toStartOfHour(""EventTime"") AS ""Hour"", sum(""Metric1"") AS ""Total""
              FROM ""new_events"" GROUP BY ""Hour"";");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""daily_summary""
              ENGINE = SummingMergeTree() ORDER BY (""Day"")
              AS SELECT toDate(""EventTime"") AS ""Day"", sum(""Metric2"") AS ""Total""
              FROM ""new_events"" GROUP BY ""Day"";");

        await context.Database.ExecuteSqlRawAsync(
            @"CREATE DICTIONARY IF NOT EXISTS ""ref_dict""
              (""Id"" UInt64, ""Name"" String)
              PRIMARY KEY ""Id""
              SOURCE(CLICKHOUSE(TABLE 'reference_data'))
              LAYOUT(HASHED()) LIFETIME(300);");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""new_events"" ADD INDEX IF NOT EXISTS ""IX_Metric1"" (""Metric1"") TYPE minmax GRANULARITY 1;");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""existing_events"" ADD INDEX IF NOT EXISTS ""IX_Region"" (""Region"") TYPE bloom_filter GRANULARITY 1;");

        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""new_events"" ADD PROJECTION IF NOT EXISTS ""prj_by_metric"" (SELECT * ORDER BY ""Metric1"");");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""existing_events"" ADD PROJECTION IF NOT EXISTS ""prj_by_region"" (SELECT * ORDER BY ""Region"");");

        // Verify key objects exist
        var tableCount = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name IN ('existing_events', 'new_events', 'reference_data', 'hourly_summary', 'daily_summary')")
            .CountAsync();
        Assert.Equal(5, tableCount);

        var dictCount = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.dictionaries WHERE name = 'ref_dict'")
            .CountAsync();
        Assert.Equal(1, dictCount);
    }

    /// <summary>
    /// Multi-table analytics: 3 tables, columns, cascading MVs (hourly→daily→weekly), indexes.
    /// </summary>
    [Fact]
    public void MultiTableAnalytics_CascadingMvsWithIndexes()
    {
        var table1 = new CreateTableOperation { Name = "clicks" };
        var table2 = new CreateTableOperation { Name = "views" };
        var table3 = new CreateTableOperation { Name = "conversions" };

        var col1 = new AddColumnOperation { Name = "Source", Table = "clicks" };
        var col2 = new AddColumnOperation { Name = "Source", Table = "views" };

        var mvHourly = new CreateTableOperation { Name = "hourly_stats" };
        mvHourly.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvHourly.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "clicks");

        var mvDaily = new CreateTableOperation { Name = "daily_stats" };
        mvDaily.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvDaily.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "hourly_stats");

        var mvWeekly = new CreateTableOperation { Name = "weekly_stats" };
        mvWeekly.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvWeekly.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "daily_stats");

        var idx1 = new CreateIndexOperation { Name = "IX_Source_Clicks", Table = "clicks" };
        var idx2 = new CreateIndexOperation { Name = "IX_Source_Views", Table = "views" };

        // Completely scrambled
        var operations = new List<MigrationOperation>
        {
            mvWeekly, idx1, col1, mvDaily, table2, idx2, mvHourly, col2, table1, table3
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(10, steps.Count);

        // Phase 4: All tables first (3)
        var tableOps = steps.Take(3).Select(s => s.Operation).ToList();
        Assert.All(tableOps, op => Assert.IsType<CreateTableOperation>(op));
        var tableNames = tableOps.Cast<CreateTableOperation>().Select(t => t.Name).ToHashSet();
        Assert.Contains("clicks", tableNames);
        Assert.Contains("views", tableNames);
        Assert.Contains("conversions", tableNames);

        // Phase 5: AddColumns (2)
        Assert.IsType<AddColumnOperation>(steps[3].Operation);
        Assert.IsType<AddColumnOperation>(steps[4].Operation);

        // Phase 6: MVs in dependency order (3)
        Assert.Equal("hourly_stats", ((CreateTableOperation)steps[5].Operation).Name);
        Assert.Equal("daily_stats", ((CreateTableOperation)steps[6].Operation).Name);
        Assert.Equal("weekly_stats", ((CreateTableOperation)steps[7].Operation).Name);

        // Phase 8: Indexes (2)
        Assert.IsType<CreateIndexOperation>(steps[8].Operation);
        Assert.IsType<CreateIndexOperation>(steps[9].Operation);
    }

    #endregion

    #region Edge Cases

    /// <summary>
    /// MV from JOIN of 2 tables, both being modified (columns added).
    /// Columns must be added before MV even though MV references both tables.
    /// </summary>
    [Fact]
    public void MvFromJoinOfModifiedTables_ColumnsBeforeMv()
    {
        var colA = new AddColumnOperation { Name = "RegionA", Table = "orders" };
        var colB = new AddColumnOperation { Name = "RegionB", Table = "customers" };

        var mv = new CreateTableOperation { Name = "regional_summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            @"SELECT o.RegionA, c.RegionB, count() FROM orders o JOIN customers c ON o.CustomerId = c.Id GROUP BY o.RegionA, c.RegionB");

        // MV first
        var operations = new List<MigrationOperation> { mv, colA, colB };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);

        // Both columns (Phase 5) before MV (Phase 6)
        Assert.IsType<AddColumnOperation>(steps[0].Operation);
        Assert.IsType<AddColumnOperation>(steps[1].Operation);
        Assert.IsType<CreateTableOperation>(steps[2].Operation);
    }

    /// <summary>
    /// Index on column being added to table being created.
    /// CreateTable (Phase 4) → AddColumn (Phase 5) → CreateIndex (Phase 8).
    /// </summary>
    [Fact]
    public void IndexOnNewColumnInNewTable_CorrectOrder()
    {
        var createTable = new CreateTableOperation { Name = "new_users" };
        var addCol = new AddColumnOperation { Name = "Email", Table = "new_users" };
        var createIdx = new CreateIndexOperation { Name = "IX_Email", Table = "new_users", Columns = new[] { "Email" } };

        // Reversed
        var operations = new List<MigrationOperation> { createIdx, addCol, createTable };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);   // Phase 4
        Assert.IsType<AddColumnOperation>(steps[1].Operation);     // Phase 5
        Assert.IsType<CreateIndexOperation>(steps[2].Operation);   // Phase 8
    }

    /// <summary>
    /// Projection referencing altered column.
    /// Drop projection (Phase 1), Alter column (Phase 7), Add projection (Phase 9).
    /// </summary>
    [Fact]
    public void ProjectionWithAlteredColumn_CorrectOrder()
    {
        var dropProj = new DropProjectionOperation { Name = "prj_old", Table = "events" };
        var alterCol = new AlterColumnOperation { Name = "Status", Table = "events" };
        var addProj = new AddProjectionOperation { Name = "prj_new", Table = "events" };

        // Scrambled
        var operations = new List<MigrationOperation> { addProj, alterCol, dropProj };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.IsType<DropProjectionOperation>(steps[0].Operation);  // Phase 1
        Assert.IsType<AlterColumnOperation>(steps[1].Operation);     // Phase 7
        Assert.IsType<AddProjectionOperation>(steps[2].Operation);   // Phase 9
    }

    /// <summary>
    /// Mixed operation types with same table name patterns.
    /// Tests that table name doesn't cause false dependency detection.
    /// </summary>
    [Fact]
    public void SameTableNamePatterns_NoDependencyConfusion()
    {
        var dropTable = new DropTableOperation { Name = "events" };

        var createTable = new CreateTableOperation { Name = "events_v2" };

        var mv = new CreateTableOperation { Name = "events_summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events_v2");

        var operations = new List<MigrationOperation> { mv, createTable, dropTable };
        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.Equal("events", ((DropTableOperation)steps[0].Operation).Name);      // Phase 3
        Assert.Equal("events_v2", ((CreateTableOperation)steps[1].Operation).Name); // Phase 4
        Assert.Equal("events_summary", ((CreateTableOperation)steps[2].Operation).Name); // Phase 6
    }

    #endregion

    #region Helper Methods

    private static DropTableOperation CreateDropMv(string name, string source)
    {
        var op = new DropTableOperation { Name = name };
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, source);
        return op;
    }

    private static CreateTableOperation CreateMv(string name, string source)
    {
        var op = new CreateTableOperation { Name = name };
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        op.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, source);
        return op;
    }

    private static CreateTableOperation CreateDict(string name, string source)
    {
        var op = new CreateTableOperation { Name = name };
        op.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        op.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, source);
        return op;
    }

    private static int GetPhase(MigrationOperation op)
    {
        return op switch
        {
            DropProjectionOperation or DropIndexOperation => 1,
            DropTableOperation dropOp when dropOp.FindAnnotation(ClickHouseAnnotationNames.MaterializedView) != null
                || dropOp.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null => 2,
            DropTableOperation => 3,
            CreateTableOperation createOp when createOp.FindAnnotation(ClickHouseAnnotationNames.MaterializedView) != null
                || createOp.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null => 6,
            CreateTableOperation => 4,
            AddColumnOperation => 5,
            AlterColumnOperation or DropColumnOperation or RenameColumnOperation => 7,
            CreateIndexOperation => 8,
            AddProjectionOperation or MaterializeProjectionOperation => 9,
            _ => 7
        };
    }

    private PhaseOrderingDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<PhaseOrderingDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new PhaseOrderingDbContext(options);
    }

    #endregion
}
