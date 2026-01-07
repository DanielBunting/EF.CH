using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Integration tests verifying step migration history tracking.
/// Each step migration should get its own entry in __EFMigrationsHistory,
/// enabling resume after partial failure.
/// </summary>
public class StepMigrationHistoryIntegrationTests : IAsyncLifetime
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

    #region History Table Creation

    [Fact]
    public async Task HistoryTable_CreatedWithCorrectStructure()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        // Create history table
        var createScript = historyRepository.GetCreateIfNotExistsScript();
        await context.Database.ExecuteSqlRawAsync(createScript);

        // Verify structure
        var columns = await context.Database.SqlQueryRaw<string>(
            "SELECT name AS Value FROM system.columns WHERE database = currentDatabase() AND table = '__EFMigrationsHistory'"
        ).ToListAsync();

        Assert.Contains("MigrationId", columns);
        Assert.Contains("ProductVersion", columns);
    }

    [Fact]
    public async Task HistoryTable_CreateIfNotExists_Idempotent()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        var createScript = historyRepository.GetCreateIfNotExistsScript();

        // Run twice - should be idempotent
        await context.Database.ExecuteSqlRawAsync(createScript);
        await context.Database.ExecuteSqlRawAsync(createScript);

        var exists = await historyRepository.ExistsAsync();
        Assert.True(exists);
    }

    #endregion

    #region Step Migration Recording

    [Fact]
    public async Task StepMigrations_EachRecordedIndividually()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        // Create history table
        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // Simulate 3 step migrations
        var steps = new[]
        {
            new HistoryRow("20250107100000_AddOrders_001", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_002", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_003", "8.0.0")
        };

        foreach (var step in steps)
        {
            await context.Database.ExecuteSqlRawAsync(historyRepository.GetInsertScript(step));
        }

        // Verify all steps recorded
        var applied = await historyRepository.GetAppliedMigrationsAsync();
        var appliedList = applied.ToList();

        Assert.Equal(3, appliedList.Count);
        Assert.Contains(appliedList, m => m.MigrationId == "20250107100000_AddOrders_001");
        Assert.Contains(appliedList, m => m.MigrationId == "20250107100000_AddOrders_002");
        Assert.Contains(appliedList, m => m.MigrationId == "20250107100000_AddOrders_003");
    }

    [Fact]
    public async Task StepMigrations_OrderedByMigrationId()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // Insert in reverse order
        var steps = new[]
        {
            new HistoryRow("20250107100000_AddOrders_003", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_001", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_002", "8.0.0")
        };

        foreach (var step in steps)
        {
            await context.Database.ExecuteSqlRawAsync(historyRepository.GetInsertScript(step));
        }

        // Should be returned in sorted order
        var applied = (await historyRepository.GetAppliedMigrationsAsync()).ToList();

        Assert.Equal("20250107100000_AddOrders_001", applied[0].MigrationId);
        Assert.Equal("20250107100000_AddOrders_002", applied[1].MigrationId);
        Assert.Equal("20250107100000_AddOrders_003", applied[2].MigrationId);
    }

    #endregion

    #region Partial Failure Simulation

    [Fact]
    public async Task PartialFailure_OnlyCompletedStepsInHistory()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // Step 1: Create table (succeeds)
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Orders" ("Id" UUID)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetInsertScript(new HistoryRow("20250107_AddOrders_001", "8.0.0")));

        // Step 2: Would fail (simulate by not executing DDL, only checking history)
        // In real scenario, this would be an invalid SQL that throws

        // Verify only step 1 is in history
        var applied = (await historyRepository.GetAppliedMigrationsAsync()).ToList();

        Assert.Single(applied);
        Assert.Equal("20250107_AddOrders_001", applied[0].MigrationId);
    }

    [Fact]
    public async Task Resume_CanCheckWhichStepsAlreadyApplied()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // Simulate partial execution - steps 1 and 2 completed
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetInsertScript(new HistoryRow("20250107_AddOrders_001", "8.0.0")));
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetInsertScript(new HistoryRow("20250107_AddOrders_002", "8.0.0")));

        // On resume, check which steps need to run
        var applied = (await historyRepository.GetAppliedMigrationsAsync())
            .Select(m => m.MigrationId)
            .ToHashSet();

        var allSteps = new[]
        {
            "20250107_AddOrders_001",
            "20250107_AddOrders_002",
            "20250107_AddOrders_003"
        };

        var pendingSteps = allSteps.Where(s => !applied.Contains(s)).ToList();

        Assert.Single(pendingSteps);
        Assert.Equal("20250107_AddOrders_003", pendingSteps[0]);
    }

    #endregion

    #region Idempotent Resume Scenario

    [Fact]
    public async Task Resume_IdempotentDdlAndHistoryCheck()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // First run - execute step 1
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Orders" ("Id" UUID, "Status" String)
            ENGINE = MergeTree() ORDER BY ("Id")
            """);
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetInsertScript(new HistoryRow("20250107_AddOrders_001", "8.0.0")));

        // Simulate failure after step 1

        // Second run (resume) - should skip step 1, run step 2
        var applied = (await historyRepository.GetAppliedMigrationsAsync())
            .Select(m => m.MigrationId)
            .ToHashSet();

        // Step 1: Skip because already in history
        if (!applied.Contains("20250107_AddOrders_001"))
        {
            // This won't run
            await context.Database.ExecuteSqlRawAsync("""
                CREATE TABLE IF NOT EXISTS "Orders" ("Id" UUID, "Status" String)
                ENGINE = MergeTree() ORDER BY ("Id")
                """);
            await context.Database.ExecuteSqlRawAsync(
                historyRepository.GetInsertScript(new HistoryRow("20250107_AddOrders_001", "8.0.0")));
        }

        // Step 2: Run because not in history
        if (!applied.Contains("20250107_AddOrders_002"))
        {
            await context.Database.ExecuteSqlRawAsync("""
                ALTER TABLE "Orders" ADD INDEX IF NOT EXISTS "IX_Status" ("Status") TYPE set(100) GRANULARITY 2
                """);
            await context.Database.ExecuteSqlRawAsync(
                historyRepository.GetInsertScript(new HistoryRow("20250107_AddOrders_002", "8.0.0")));
        }

        // Verify both steps now in history
        var finalApplied = (await historyRepository.GetAppliedMigrationsAsync()).ToList();
        Assert.Equal(2, finalApplied.Count);

        // Verify table and index exist
        var tableExists = await context.Database.SqlQueryRaw<string>(
            "SELECT name AS Value FROM system.tables WHERE database = currentDatabase() AND name = 'Orders'"
        ).FirstOrDefaultAsync();
        Assert.NotNull(tableExists);

        var indexExists = await context.Database.SqlQueryRaw<string>(
            "SELECT name AS Value FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'Orders' AND name = 'IX_Status'"
        ).FirstOrDefaultAsync();
        Assert.NotNull(indexExists);
    }

    #endregion

    #region Multiple Migrations

    [Fact]
    public async Task MultipleMigrations_StepsInterleaved_TrackedCorrectly()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // Migration 1: AddOrders (3 steps)
        // Migration 2: AddCustomers (2 steps)
        var allSteps = new[]
        {
            new HistoryRow("20250107100000_AddOrders_001", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_002", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_003", "8.0.0"),
            new HistoryRow("20250108100000_AddCustomers_001", "8.0.0"),
            new HistoryRow("20250108100000_AddCustomers_002", "8.0.0")
        };

        foreach (var step in allSteps)
        {
            await context.Database.ExecuteSqlRawAsync(historyRepository.GetInsertScript(step));
        }

        var applied = (await historyRepository.GetAppliedMigrationsAsync()).ToList();

        Assert.Equal(5, applied.Count);

        // Verify order
        Assert.Equal("20250107100000_AddOrders_001", applied[0].MigrationId);
        Assert.Equal("20250107100000_AddOrders_002", applied[1].MigrationId);
        Assert.Equal("20250107100000_AddOrders_003", applied[2].MigrationId);
        Assert.Equal("20250108100000_AddCustomers_001", applied[3].MigrationId);
        Assert.Equal("20250108100000_AddCustomers_002", applied[4].MigrationId);
    }

    [Fact]
    public async Task FilterStepsByBaseMigration()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        var allSteps = new[]
        {
            new HistoryRow("20250107100000_AddOrders_001", "8.0.0"),
            new HistoryRow("20250107100000_AddOrders_002", "8.0.0"),
            new HistoryRow("20250108100000_AddCustomers_001", "8.0.0")
        };

        foreach (var step in allSteps)
        {
            await context.Database.ExecuteSqlRawAsync(historyRepository.GetInsertScript(step));
        }

        var applied = (await historyRepository.GetAppliedMigrationsAsync()).ToList();

        // Filter to just AddOrders steps
        var orderSteps = applied
            .Where(m => m.MigrationId.Contains("AddOrders"))
            .ToList();

        Assert.Equal(2, orderSteps.Count);

        // Filter to just AddCustomers steps
        var customerSteps = applied
            .Where(m => m.MigrationId.Contains("AddCustomers"))
            .ToList();

        Assert.Single(customerSteps);
    }

    #endregion

    #region Delete History Entry

    [Fact]
    public async Task DeleteHistoryEntry_RemovesSpecificStep()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        await context.Database.ExecuteSqlRawAsync(historyRepository.GetCreateIfNotExistsScript());

        // Insert steps
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetInsertScript(new HistoryRow("20250107_Test_001", "8.0.0")));
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetInsertScript(new HistoryRow("20250107_Test_002", "8.0.0")));

        // Delete step 2
        await context.Database.ExecuteSqlRawAsync(
            historyRepository.GetDeleteScript("20250107_Test_002"));

        // ClickHouse mutations are async, wait for completion
        await Task.Delay(1000);

        var applied = (await historyRepository.GetAppliedMigrationsAsync()).ToList();

        Assert.Single(applied);
        Assert.Equal("20250107_Test_001", applied[0].MigrationId);
    }

    #endregion

    private TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new TestDbContext(options);
    }

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options) : base(options) { }
    }
}
