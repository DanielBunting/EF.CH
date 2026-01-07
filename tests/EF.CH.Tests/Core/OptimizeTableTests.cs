using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Core;

public class OptimizeTableTests : IAsyncLifetime
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

    #region Integration Tests

    [Fact]
    public async Task OptimizeTableAsync_ExecutesWithoutError()
    {
        await using var context = CreateContext();
        await CreateTestTable(context);

        // Should execute without error
        await context.Database.OptimizeTableAsync<OptimizeTestEvent>();
    }

    [Fact]
    public async Task OptimizeTableFinalAsync_ExecutesWithoutError()
    {
        await using var context = CreateContext();
        await CreateTestTable(context);

        // Should execute without error
        await context.Database.OptimizeTableFinalAsync<OptimizeTestEvent>();
    }

    [Fact]
    public async Task OptimizeTableFinalAsync_ForcesDeduplication()
    {
        await using var context = CreateContext();

        // Create ReplacingMergeTree table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""OptimizeTestEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = ReplacingMergeTree(""UpdatedAt"")
            ORDER BY ""Id""
        ");

        var id = Guid.NewGuid();

        // Insert duplicate rows with same Id but different UpdatedAt
        await context.Database.ExecuteSqlRawAsync($@"
            INSERT INTO ""OptimizeTestEvents"" VALUES
            ('{id}', 'Version 1', '2024-01-01 00:00:00'),
            ('{id}', 'Version 2', '2024-01-02 00:00:00')
        ");

        // Optimize to force merge
        await context.Database.OptimizeTableFinalAsync("OptimizeTestEvents");

        // Verify deduplication occurred - should only have latest version
        var count = await context.Database
            .SqlQueryRaw<ulong>(@"SELECT count() as ""Value"" FROM ""OptimizeTestEvents""")
            .FirstAsync();

        Assert.Equal(1UL, count);
    }

    [Fact]
    public async Task OptimizeTablePartitionAsync_ExecutesWithoutError()
    {
        await using var context = CreateContext();

        // Create partitioned table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""PartitionedEvents"" (
                ""Id"" UUID,
                ""Timestamp"" DateTime64(3)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(""Timestamp"")
            ORDER BY ""Id""
        ");

        // Insert data into a specific partition
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""PartitionedEvents"" VALUES
            (generateUUIDv4(), '2024-01-15 00:00:00')
        ");

        // Should execute without error
        await context.Database.OptimizeTablePartitionAsync("PartitionedEvents", "202401");
    }

    [Fact]
    public async Task OptimizeTablePartitionFinalAsync_ExecutesWithoutError()
    {
        await using var context = CreateContext();

        // Create partitioned table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""PartitionedEvents2"" (
                ""Id"" UUID,
                ""Timestamp"" DateTime64(3)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(""Timestamp"")
            ORDER BY ""Id""
        ");

        // Insert data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""PartitionedEvents2"" VALUES
            (generateUUIDv4(), '2024-01-15 00:00:00')
        ");

        // Should execute without error
        await context.Database.OptimizeTablePartitionFinalAsync("PartitionedEvents2", "202401");
    }

    [Fact]
    public async Task OptimizeTableAsync_WithOptions_ExecutesWithoutError()
    {
        await using var context = CreateContext();

        // Create partitioned ReplacingMergeTree table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""OptionsTestEvents"" (
                ""Id"" UUID,
                ""Timestamp"" DateTime64(3),
                ""Name"" String
            ) ENGINE = ReplacingMergeTree()
            PARTITION BY toYYYYMM(""Timestamp"")
            ORDER BY ""Id""
        ");

        // Insert data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""OptionsTestEvents"" VALUES
            (generateUUIDv4(), '2024-01-15 00:00:00', 'Test')
        ");

        // Should execute with combined options
        await context.Database.OptimizeTableAsync("OptionsTestEvents", o => o
            .WithPartition("202401")
            .WithFinal());
    }

    [Fact]
    public async Task OptimizeTableAsync_WithDeduplicate_ExecutesWithoutError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""DedupeTestEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Value"" Int32
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        // Insert data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""DedupeTestEvents"" VALUES
            (generateUUIDv4(), 'Test', 1)
        ");

        // Should execute with DEDUPLICATE
        await context.Database.OptimizeTableAsync("DedupeTestEvents", o => o
            .WithFinal()
            .WithDeduplicate());
    }

    [Fact]
    public async Task OptimizeTableAsync_WithDeduplicateByColumns_ExecutesWithoutError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""DedupeByTestEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Value"" Int32
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");

        // Insert data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""DedupeByTestEvents"" VALUES
            (generateUUIDv4(), 'Test', 1)
        ");

        // Should execute with DEDUPLICATE BY columns
        await context.Database.OptimizeTableAsync("DedupeByTestEvents", o => o
            .WithFinal()
            .WithDeduplicate("Id", "Name"));
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task OptimizeTableAsync_InvalidEntity_ThrowsException()
    {
        await using var context = CreateContext();

        // NonExistentEntity is not in the model
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            context.Database.OptimizeTableFinalAsync<NonExistentEntity>());
    }

    [Fact]
    public async Task OptimizeTableAsync_NullTableName_ThrowsException()
    {
        await using var context = CreateContext();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            context.Database.OptimizeTableAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task OptimizeTableAsync_EmptyTableName_ThrowsException()
    {
        await using var context = CreateContext();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            context.Database.OptimizeTableAsync("", CancellationToken.None));
    }

    #endregion

    #region Helpers

    private OptimizeTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<OptimizeTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new OptimizeTestContext(options);
    }

    private static async Task CreateTestTable(OptimizeTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""OptimizeTestEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");
    }

    #endregion
}

#region Test Entities and Context

public class OptimizeTestEvent
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }
}

public class NonExistentEntity
{
    public Guid Id { get; set; }
}

public class OptimizeTestContext : DbContext
{
    public OptimizeTestContext(DbContextOptions<OptimizeTestContext> options)
        : base(options) { }

    public DbSet<OptimizeTestEvent> Events => Set<OptimizeTestEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OptimizeTestEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("OptimizeTestEvents");
            entity.UseMergeTree(x => x.Id);
        });

        // Note: NonExistentEntity is intentionally NOT registered
    }
}

#endregion
