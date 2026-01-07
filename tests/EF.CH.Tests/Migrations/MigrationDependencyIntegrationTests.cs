using EF.CH.Extensions;
using EF.CH.Migrations.Design;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Integration tests verifying that topologically sorted operations execute correctly
/// against a real ClickHouse instance. Tables must be created before their dependent
/// objects (indices, projections, materialized views, etc.).
/// </summary>
public class MigrationDependencyIntegrationTests : IAsyncLifetime
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

    #region Splitter Dependency Ordering Tests

    [Fact]
    public void Splitter_OrdersTableBeforeIndex()
    {
        var splitter = new ClickHouseMigrationsSplitter();

        // Input: Index before table (wrong order)
        var operations = new MigrationOperation[]
        {
            new CreateIndexOperation { Name = "IX_Orders_Date", Table = "Orders", Columns = ["OrderDate"] },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = splitter.Split(operations);

        // Output: Table should come first
        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        Assert.IsType<CreateIndexOperation>(steps[1].Operation);
    }

    [Fact]
    public void Splitter_OrdersTableBeforeProjection()
    {
        var splitter = new ClickHouseMigrationsSplitter();

        var operations = new MigrationOperation[]
        {
            new AddProjectionOperation { Table = "Orders", Name = "prj_by_date", SelectSql = "SELECT * ORDER BY OrderDate" },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        Assert.IsType<AddProjectionOperation>(steps[1].Operation);
    }

    [Fact]
    public void Splitter_OrdersTableBeforeAddColumn()
    {
        var splitter = new ClickHouseMigrationsSplitter();

        var operations = new MigrationOperation[]
        {
            new AddColumnOperation { Table = "Orders", Name = "NewColumn", ClrType = typeof(string) },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        Assert.IsType<AddColumnOperation>(steps[1].Operation);
    }

    [Fact]
    public void Splitter_PreservesOrderForIndependentOperations()
    {
        var splitter = new ClickHouseMigrationsSplitter();

        // Three independent tables - should maintain original order
        var operations = new MigrationOperation[]
        {
            new CreateTableOperation { Name = "Alpha" },
            new CreateTableOperation { Name = "Beta" },
            new CreateTableOperation { Name = "Gamma" }
        };

        var steps = splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.Equal("Alpha", ((CreateTableOperation)steps[0].Operation).Name);
        Assert.Equal("Beta", ((CreateTableOperation)steps[1].Operation).Name);
        Assert.Equal("Gamma", ((CreateTableOperation)steps[2].Operation).Name);
    }

    [Fact]
    public void Splitter_ComplexDependencyChain()
    {
        var splitter = new ClickHouseMigrationsSplitter();

        // Complex scenario: table, column, index, projection, materialize
        var operations = new MigrationOperation[]
        {
            new MaterializeProjectionOperation { Table = "Orders", Name = "prj_daily" },
            new AddProjectionOperation { Table = "Orders", Name = "prj_daily", SelectSql = "SELECT ..." },
            new CreateIndexOperation { Table = "Orders", Name = "IX_Orders_Date", Columns = ["Date"] },
            new AddColumnOperation { Table = "Orders", Name = "Status", ClrType = typeof(string) },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = splitter.Split(operations);

        // Table must come first
        Assert.Equal(5, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);

        // All others depend on table
        var remainingTypes = steps.Skip(1).Select(s => s.Operation.GetType()).ToList();
        Assert.Contains(typeof(AddColumnOperation), remainingTypes);
        Assert.Contains(typeof(CreateIndexOperation), remainingTypes);
        Assert.Contains(typeof(AddProjectionOperation), remainingTypes);
        Assert.Contains(typeof(MaterializeProjectionOperation), remainingTypes);
    }

    #endregion

    #region Execution Order Integration Tests

    [Fact]
    public async Task TableBeforeIndex_ExecutesSuccessfully()
    {
        await using var context = CreateContext();

        // Step 1: Create table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Orders" (
                "Id" UUID,
                "OrderDate" DateTime64(3)
            )
            ENGINE = MergeTree()
            ORDER BY ("OrderDate", "Id")
            """);

        // Step 2: Add index (depends on table)
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Orders" ADD INDEX IF NOT EXISTS "IX_OrderDate" ("OrderDate") TYPE minmax GRANULARITY 3
            """);

        // Verify both exist
        Assert.True(await TableExistsAsync(context, "Orders"));
        Assert.True(await IndexExistsAsync(context, "Orders", "IX_OrderDate"));
    }

    [Fact]
    public async Task TableBeforeProjection_ExecutesSuccessfully()
    {
        await using var context = CreateContext();

        // Step 1: Create table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Events" (
                "Id" UUID,
                "EventType" String,
                "CreatedAt" DateTime64(3)
            )
            ENGINE = MergeTree()
            ORDER BY ("CreatedAt", "Id")
            """);

        // Step 2: Add projection (depends on table)
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Events" ADD PROJECTION IF NOT EXISTS "prj_by_type" (SELECT * ORDER BY ("EventType", "CreatedAt"))
            """);

        // Verify both exist
        Assert.True(await TableExistsAsync(context, "Events"));
        Assert.True(await ProjectionExistsAsync(context, "Events", "prj_by_type"));
    }

    [Fact]
    public async Task SourceTableBeforeMaterializedView_ExecutesSuccessfully()
    {
        await using var context = CreateContext();

        // Step 1: Create source table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "RawEvents" (
                "Id" UUID,
                "EventType" String,
                "Timestamp" DateTime64(3)
            )
            ENGINE = MergeTree()
            ORDER BY ("Timestamp", "Id")
            """);

        // Step 2: Create materialized view (depends on source table)
        await context.Database.ExecuteSqlRawAsync("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS "EventCounts"
            ENGINE = SummingMergeTree()
            ORDER BY ("EventType")
            AS SELECT "EventType", count() AS "Count"
            FROM "RawEvents"
            GROUP BY "EventType"
            """);

        // Verify both exist
        Assert.True(await TableExistsAsync(context, "RawEvents"));
        Assert.True(await TableExistsAsync(context, "EventCounts")); // MV is a table
    }

    [Fact]
    public async Task SourceTableBeforeDictionary_ExecutesSuccessfully()
    {
        await using var context = CreateContext();

        // Step 1: Create source table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Countries" (
                "Id" UInt64,
                "Name" String,
                "Code" String
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        // Insert some data (dictionaries need data to load)
        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "Countries" VALUES (1, 'USA', 'US'), (2, 'UK', 'GB')
            """);

        // Step 2: Create dictionary (depends on source table)
        await context.Database.ExecuteSqlRawAsync("""
            CREATE DICTIONARY IF NOT EXISTS "country_lookup"
            (
                "Id" UInt64,
                "Name" String,
                "Code" String
            )
            PRIMARY KEY "Id"
            SOURCE(CLICKHOUSE(TABLE 'Countries'))
            LAYOUT(HASHED())
            LIFETIME(300)
            """);

        // Verify both exist
        Assert.True(await TableExistsAsync(context, "Countries"));
        Assert.True(await DictionaryExistsAsync(context, "country_lookup"));
    }

    [Fact]
    public async Task ComplexMigration_AllDependenciesRespected()
    {
        await using var context = CreateContext();

        // This simulates a real migration with multiple dependent objects
        // The order here is what the splitter would produce

        // Step 1: Create table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Sales" (
                "Id" UUID,
                "ProductId" UUID,
                "CustomerId" UUID,
                "Amount" Decimal(18, 4),
                "SaleDate" DateTime64(3),
                "Region" String
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM("SaleDate")
            ORDER BY ("SaleDate", "Id")
            """);

        // Step 2: Add index on Region
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Sales" ADD INDEX IF NOT EXISTS "IX_Region" ("Region") TYPE set(100) GRANULARITY 2
            """);

        // Step 3: Add index on ProductId
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Sales" ADD INDEX IF NOT EXISTS "IX_ProductId" ("ProductId") TYPE bloom_filter(0.025) GRANULARITY 3
            """);

        // Step 4: Add projection for queries by customer
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Sales" ADD PROJECTION IF NOT EXISTS "prj_by_customer" (SELECT * ORDER BY ("CustomerId", "SaleDate"))
            """);

        // Step 5: Add aggregate projection for daily totals
        await context.Database.ExecuteSqlRawAsync("""
            ALTER TABLE "Sales" ADD PROJECTION IF NOT EXISTS "prj_daily_totals" (
                SELECT toDate("SaleDate") AS "Date", "Region", sum("Amount") AS "Total"
                GROUP BY "Date", "Region"
            )
            """);

        // Verify all objects exist
        Assert.True(await TableExistsAsync(context, "Sales"));
        Assert.True(await IndexExistsAsync(context, "Sales", "IX_Region"));
        Assert.True(await IndexExistsAsync(context, "Sales", "IX_ProductId"));
        Assert.True(await ProjectionExistsAsync(context, "Sales", "prj_by_customer"));
        Assert.True(await ProjectionExistsAsync(context, "Sales", "prj_daily_totals"));
    }

    #endregion

    #region Wrong Order Would Fail Tests

    [Fact]
    public async Task IndexBeforeTable_WouldFail()
    {
        await using var context = CreateContext();

        // Attempting to create index on non-existent table should fail
        var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await context.Database.ExecuteSqlRawAsync("""
                ALTER TABLE "NonExistentTable" ADD INDEX "IX_Test" ("Col") TYPE minmax GRANULARITY 3
                """);
        });

        // ClickHouse returns error about missing table
        Assert.Contains("UNKNOWN_TABLE", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ProjectionBeforeTable_WouldFail()
    {
        await using var context = CreateContext();

        var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await context.Database.ExecuteSqlRawAsync("""
                ALTER TABLE "NonExistentTable" ADD PROJECTION "prj_test" (SELECT * ORDER BY Col)
                """);
        });

        Assert.Contains("UNKNOWN_TABLE", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task MaterializedViewBeforeSourceTable_WouldFail()
    {
        await using var context = CreateContext();

        var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await context.Database.ExecuteSqlRawAsync("""
                CREATE MATERIALIZED VIEW "TestView"
                ENGINE = MergeTree() ORDER BY tuple()
                AS SELECT * FROM "NonExistentSource"
                """);
        });

        Assert.Contains("UNKNOWN_TABLE", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    #endregion

    #region EnsureCreated Integration

    [Fact]
    public async Task EnsureCreated_WithTableAndIndex_CreatesInCorrectOrder()
    {
        await using var context = CreateContextWithModel();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Verify table exists
        Assert.True(await TableExistsAsync(context, "Products"));

        // Insert data to verify table works
        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "Products" ("Id", "Name", "Price", "CreatedAt")
            VALUES (generateUUIDv4(), 'Test Product', 99.99, now64())
            """);

        var count = await context.Database.SqlQueryRaw<ulong>(
            "SELECT count() AS Value FROM \"Products\""
        ).FirstOrDefaultAsync();

        Assert.Equal(1UL, count);
    }

    [Fact]
    public async Task EnsureCreated_ComplexModel_AllObjectsCreated()
    {
        await using var context = CreateContextWithComplexModel();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Table created
        Assert.True(await TableExistsAsync(context, "Transactions"));

        // Can insert and query
        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "Transactions" ("Id", "AccountId", "Amount", "TransactionDate", "Status")
            VALUES (generateUUIDv4(), generateUUIDv4(), 100.00, now64(), 'completed')
            """);

        var result = await context.Database.SqlQueryRaw<string>(
            "SELECT \"Status\" AS Value FROM \"Transactions\" LIMIT 1"
        ).FirstOrDefaultAsync();

        Assert.Equal("completed", result);
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

    private ProductDbContext CreateContextWithModel()
    {
        var options = new DbContextOptionsBuilder<ProductDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new ProductDbContext(options);
    }

    private TransactionDbContext CreateContextWithComplexModel()
    {
        var options = new DbContextOptionsBuilder<TransactionDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new TransactionDbContext(options);
    }

    private static async Task<bool> TableExistsAsync(DbContext context, string tableName)
    {
        var result = await context.Database.SqlQueryRaw<string>(
            $"SELECT name AS Value FROM system.tables WHERE database = currentDatabase() AND name = '{tableName}'"
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

    #endregion

    #region Test DbContexts

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options) : base(options) { }
    }

    private class ProductDbContext : DbContext
    {
        public ProductDbContext(DbContextOptions<ProductDbContext> options) : base(options) { }

        public DbSet<Product> Products => Set<Product>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Product>(entity =>
            {
                entity.ToTable("Products");
                entity.HasKey(e => e.Id);
                entity.UseMergeTree(x => new { x.CreatedAt, x.Id });
            });
        }
    }

    private class Product
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    private class TransactionDbContext : DbContext
    {
        public TransactionDbContext(DbContextOptions<TransactionDbContext> options) : base(options) { }

        public DbSet<Transaction> Transactions => Set<Transaction>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Transaction>(entity =>
            {
                entity.ToTable("Transactions");
                entity.HasKey(e => e.Id);
                entity.UseMergeTree(x => new { x.TransactionDate, x.Id });
                entity.HasPartitionByMonth(x => x.TransactionDate);
            });
        }
    }

    private class Transaction
    {
        public Guid Id { get; set; }
        public Guid AccountId { get; set; }
        public decimal Amount { get; set; }
        public DateTime TransactionDate { get; set; }
        public string Status { get; set; } = string.Empty;
    }

    #endregion
}
