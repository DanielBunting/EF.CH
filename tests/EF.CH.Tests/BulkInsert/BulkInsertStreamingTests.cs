using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

public class BulkInsertStreamingTests : IAsyncLifetime
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
    public async Task BulkInsertStreaming_InsertsAllEntities()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<StreamEvent> GenerateEvents()
        {
            for (var i = 0; i < 500; i++)
            {
                yield return new StreamEvent
                {
                    Id = Guid.NewGuid(),
                    EventTime = DateTime.UtcNow,
                    Name = $"Event_{i}"
                };
            }
        }

        var result = await context.BulkInsertStreamingAsync(GenerateEvents());

        Assert.Equal(500, result.RowsInserted);

        var count = await context.StreamEvents.LongCountAsync();
        Assert.Equal(500, count);
    }

    [Fact]
    public async Task BulkInsertStreaming_WithBatchSize_CreatesBatches()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<StreamEvent> GenerateEvents()
        {
            for (var i = 0; i < 250; i++)
            {
                yield return new StreamEvent
                {
                    Id = Guid.NewGuid(),
                    EventTime = DateTime.UtcNow,
                    Name = $"Event_{i}"
                };
            }
        }

        var result = await context.BulkInsertStreamingAsync(GenerateEvents(), opts => opts
            .WithBatchSize(100));

        Assert.Equal(250, result.RowsInserted);
        Assert.Equal(3, result.BatchesExecuted); // 100 + 100 + 50

        var count = await context.StreamEvents.LongCountAsync();
        Assert.Equal(250, count);
    }

    [Fact]
    public async Task BulkInsertStreaming_WithProgressCallback_InvokesCallback()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<StreamEvent> GenerateEvents()
        {
            for (var i = 0; i < 150; i++)
            {
                yield return new StreamEvent
                {
                    Id = Guid.NewGuid(),
                    EventTime = DateTime.UtcNow,
                    Name = $"Event_{i}"
                };
            }
        }

        var progressValues = new List<long>();

        var result = await context.BulkInsertStreamingAsync(GenerateEvents(), opts => opts
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
    public async Task BulkInsertStreaming_ViaDbSet_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<StreamEvent> GenerateEvents()
        {
            for (var i = 0; i < 100; i++)
            {
                yield return new StreamEvent
                {
                    Id = Guid.NewGuid(),
                    EventTime = DateTime.UtcNow,
                    Name = $"Event_{i}"
                };
            }
        }

        var result = await context.StreamEvents.BulkInsertStreamingAsync(GenerateEvents());

        Assert.Equal(100, result.RowsInserted);

        var count = await context.StreamEvents.LongCountAsync();
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task BulkInsertStreaming_WithEmptyStream_ReturnsEmptyResult()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<StreamEvent> GenerateEvents()
        {
            await Task.CompletedTask;
            yield break;
        }

        var result = await context.BulkInsertStreamingAsync(GenerateEvents());

        Assert.Equal(0, result.RowsInserted);
        Assert.Equal(0, result.BatchesExecuted);
    }

    [Fact]
    public async Task BulkInsertStreaming_WithDelayedProduction_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<StreamEvent> GenerateEventsWithDelay()
        {
            for (var i = 0; i < 10; i++)
            {
                await Task.Delay(10); // Simulate slow production
                yield return new StreamEvent
                {
                    Id = Guid.NewGuid(),
                    EventTime = DateTime.UtcNow,
                    Name = $"Event_{i}"
                };
            }
        }

        var result = await context.BulkInsertStreamingAsync(GenerateEventsWithDelay());

        Assert.Equal(10, result.RowsInserted);

        var count = await context.StreamEvents.LongCountAsync();
        Assert.Equal(10, count);
    }

    private StreamTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<StreamTestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new StreamTestDbContext(options);
    }
}

public class StreamTestDbContext : DbContext
{
    public StreamTestDbContext(DbContextOptions<StreamTestDbContext> options) : base(options) { }

    public DbSet<StreamEvent> StreamEvents => Set<StreamEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StreamEvent>(entity =>
        {
            entity.ToTable("StreamEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.EventTime, x.Id });
        });
    }
}

public class StreamEvent
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string Name { get; set; } = string.Empty;
}
