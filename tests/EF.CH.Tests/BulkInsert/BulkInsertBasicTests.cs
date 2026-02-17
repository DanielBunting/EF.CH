using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

public class BulkInsertBasicTests : IAsyncLifetime
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

    [Fact]
    public async Task BulkInsert_InsertsAllEntities()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = Enumerable.Range(0, 1000)
            .Select(i => new BulkEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                EventType = $"Event_{i}",
                Data = $"Data_{i}"
            })
            .ToList();

        var result = await context.BulkInsertAsync(events);

        Assert.Equal(1000, result.RowsInserted);
        Assert.True(result.BatchesExecuted >= 1);
        Assert.True(result.Elapsed > TimeSpan.Zero);
        Assert.True(result.RowsPerSecond > 0);

        var count = await context.BulkEvents.LongCountAsync();
        Assert.Equal(1000, count);
    }

    [Fact]
    public async Task BulkInsert_WithEmptyCollection_ReturnsEmptyResult()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var result = await context.BulkInsertAsync(Array.Empty<BulkEvent>());

        Assert.Equal(0, result.RowsInserted);
        Assert.Equal(0, result.BatchesExecuted);
    }

    [Fact]
    public async Task BulkInsert_WithBatchSize_CreatesBatches()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = Enumerable.Range(0, 250)
            .Select(i => new BulkEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                EventType = "test",
                Data = null
            })
            .ToList();

        var result = await context.BulkInsertAsync(events, opts => opts.WithBatchSize(100));

        Assert.Equal(250, result.RowsInserted);
        Assert.Equal(3, result.BatchesExecuted); // 100 + 100 + 50

        var count = await context.BulkEvents.LongCountAsync();
        Assert.Equal(250, count);
    }

    [Fact]
    public async Task BulkInsert_WithProgressCallback_InvokesCallback()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = Enumerable.Range(0, 150)
            .Select(i => new BulkEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                EventType = "test",
                Data = null
            })
            .ToList();

        var progressValues = new List<long>();

        var result = await context.BulkInsertAsync(events, opts => opts
            .WithBatchSize(50)
            .WithProgressCallback(count => progressValues.Add(count)));

        Assert.Equal(150, result.RowsInserted);
        Assert.Equal(3, result.BatchesExecuted);
        Assert.Equal(3, progressValues.Count);
        Assert.Equal(50, progressValues[0]);
        Assert.Equal(100, progressValues[1]);
        Assert.Equal(150, progressValues[2]);
    }

    [Fact]
    public async Task BulkInsert_ViaDbSet_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = Enumerable.Range(0, 100)
            .Select(i => new BulkEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                EventType = "dbset_test",
                Data = null
            })
            .ToList();

        var result = await context.BulkEvents.BulkInsertAsync(events);

        Assert.Equal(100, result.RowsInserted);

        var count = await context.BulkEvents.LongCountAsync();
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task BulkInsert_WithNullableProperties_HandlesNulls()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = new[]
        {
            new BulkEvent { Id = Guid.NewGuid(), EventTime = DateTime.UtcNow, EventType = "test", Data = "has_data" },
            new BulkEvent { Id = Guid.NewGuid(), EventTime = DateTime.UtcNow, EventType = "test", Data = null }
        };

        var result = await context.BulkInsertAsync(events);

        Assert.Equal(2, result.RowsInserted);

        var withData = await context.BulkEvents.Where(e => e.Data != null).CountAsync();
        var withoutData = await context.BulkEvents.Where(e => e.Data == null).CountAsync();

        Assert.Equal(1, withData);
        Assert.Equal(1, withoutData);
    }

    [Fact]
    public async Task BulkInsert_WithDifferentDataTypes_SerializesCorrectly()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var now = DateTime.UtcNow;
        var id = Guid.NewGuid();

        var events = new[]
        {
            new BulkEvent
            {
                Id = id,
                EventTime = now,
                EventType = "special'chars\"test",
                Data = "line1\nline2\ttab"
            }
        };

        var result = await context.BulkInsertAsync(events);

        Assert.Equal(1, result.RowsInserted);

        var loaded = await context.BulkEvents.FirstAsync();
        Assert.Equal(id, loaded.Id);
        Assert.Equal("special'chars\"test", loaded.EventType);
        Assert.Equal("line1\nline2\ttab", loaded.Data);
    }

    private BulkTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<BulkTestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new BulkTestDbContext(options);
    }
}

public class BulkTestDbContext : DbContext
{
    public BulkTestDbContext(DbContextOptions<BulkTestDbContext> options) : base(options) { }

    public DbSet<BulkEvent> BulkEvents => Set<BulkEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BulkEvent>(entity =>
        {
            entity.ToTable("BulkEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.EventTime, x.Id });
        });
    }
}

public class BulkEvent
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string? Data { get; set; }
}
