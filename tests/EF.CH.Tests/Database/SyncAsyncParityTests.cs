using EF.CH.BulkInsert;
using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Database;

/// <summary>
/// Smoke coverage that every sync forwarder produces the same observable
/// state change as its async sibling. Each pair runs both shapes back-to-back
/// against the same schema and asserts mutual success.
/// </summary>
public class SyncAsyncParityTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string Conn => _container.GetConnectionString();

    private ParityContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ParityContext>()
            .UseClickHouse(Conn)
            .Options;
        return new ParityContext(options);
    }

    private static async Task CreateTableAsync(ParityContext ctx)
    {
        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""ParityOrders"" (
                ""Id"" UUID,
                ""Name"" String,
                ""UpdatedAt"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");
    }

    [Fact]
    public async Task OptimizeTable_Generic_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var asyncResult = await ctx.Database.OptimizeTableAsync<ParityOrder>();
        var syncResult = ctx.Database.OptimizeTable<ParityOrder>();

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task OptimizeTable_GenericWithOptions_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var asyncResult = await ctx.Database.OptimizeTableAsync<ParityOrder>(o => o.WithFinal());
        var syncResult = ctx.Database.OptimizeTable<ParityOrder>(o => o.WithFinal());

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task OptimizeTable_StringTableName_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var asyncResult = await ctx.Database.OptimizeTableAsync("ParityOrders");
        var syncResult = ctx.Database.OptimizeTable("ParityOrders");

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task OptimizeTable_StringTableNameWithOptions_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var asyncResult = await ctx.Database.OptimizeTableAsync("ParityOrders", o => o.WithFinal());
        var syncResult = ctx.Database.OptimizeTable("ParityOrders", o => o.WithFinal());

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task TruncateTable_Generic_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        // Two truncates on an empty table are idempotent.
        var asyncResult = await ctx.Database.TruncateTableAsync<ParityOrder>();
        var syncResult = ctx.Database.TruncateTable<ParityOrder>();

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task TruncateTable_StringTableName_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var asyncResult = await ctx.Database.TruncateTableAsync("ParityOrders");
        var syncResult = ctx.Database.TruncateTable("ParityOrders");

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task FlushLogs_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var asyncResult = await ctx.Database.FlushLogsAsync();
        var syncResult = ctx.Database.FlushLogs();

        Assert.Equal(asyncResult, syncResult);
    }

    [Fact]
    public async Task EnsureViews_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        // No views in this minimal model, but both shapes should return 0 cleanly.
        var asyncResult = await ctx.Database.EnsureViewsAsync();
        var syncResult = ctx.Database.EnsureViews();

        Assert.Equal(0, asyncResult);
        Assert.Equal(0, syncResult);
    }

    [Fact]
    public async Task EnsureParameterizedViews_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        // Likewise — no parameterized views, both return 0.
        var asyncResult = await ctx.Database.EnsureParameterizedViewsAsync();
        var syncResult = ctx.Database.EnsureParameterizedViews();

        Assert.Equal(0, asyncResult);
        Assert.Equal(0, syncResult);
    }

    [Fact]
    public async Task BulkInsert_DbContext_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var batchA = Enumerable.Range(1, 5)
            .Select(_ => new ParityOrder { Id = Guid.NewGuid(), Name = "async", UpdatedAt = DateTime.UtcNow })
            .ToList();
        var batchB = Enumerable.Range(1, 5)
            .Select(_ => new ParityOrder { Id = Guid.NewGuid(), Name = "sync", UpdatedAt = DateTime.UtcNow })
            .ToList();

        var asyncResult = await ctx.BulkInsertAsync(batchA);
        var syncResult = ctx.BulkInsert(batchB);

        Assert.Equal(5L, asyncResult.RowsInserted);
        Assert.Equal(5L, syncResult.RowsInserted);
    }

    [Fact]
    public async Task BulkInsert_DbSet_Sync_And_Async_Both_Succeed()
    {
        await using var ctx = CreateContext();
        await CreateTableAsync(ctx);

        var batchA = Enumerable.Range(1, 3)
            .Select(_ => new ParityOrder { Id = Guid.NewGuid(), Name = "async", UpdatedAt = DateTime.UtcNow })
            .ToList();
        var batchB = Enumerable.Range(1, 3)
            .Select(_ => new ParityOrder { Id = Guid.NewGuid(), Name = "sync", UpdatedAt = DateTime.UtcNow })
            .ToList();

        var asyncResult = await ctx.Orders.BulkInsertAsync(batchA);
        var syncResult = ctx.Orders.BulkInsert(batchB);

        Assert.Equal(3L, asyncResult.RowsInserted);
        Assert.Equal(3L, syncResult.RowsInserted);
    }
}

#region Test Entities

public class ParityOrder
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateTime UpdatedAt { get; set; }
}

public class ParityContext : DbContext
{
    public ParityContext(DbContextOptions<ParityContext> options) : base(options) { }

    public DbSet<ParityOrder> Orders => Set<ParityOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ParityOrder>(e =>
        {
            e.HasKey(o => o.Id);
            e.ToTable("ParityOrders");
            e.UseMergeTree(o => o.Id);
        });
    }
}

#endregion
