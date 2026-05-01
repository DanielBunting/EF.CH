using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

/// <summary>
/// Pins the bulk-inserter behaviour when a user-supplied <c>OnBatchCompleted</c>
/// callback throws. Previously the callback was invoked unguarded — its exception
/// propagated through <c>Task.WhenAll</c> in parallel mode, terminated other
/// in-flight batches, and produced a silent partial insert with the result object
/// reporting the in-progress count as if everything had succeeded.
/// </summary>
public class BulkInsertCallbackResilienceTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task BulkInsert_Sequential_KeepsInsertingWhenCallbackThrows()
    {
        await using var ctx = Create();
        await ctx.Database.EnsureCreatedAsync();

        const int total = 600;
        var events = Enumerable.Range(0, total)
            .Select(i => new BulkEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                EventType = "seq",
                Data = $"r{i}",
            })
            .ToList();

        var callbackCalls = 0;
        var result = await ctx.BulkInsertAsync(events, opts =>
        {
            opts.BatchSize = 100;
            opts.MaxDegreeOfParallelism = 1;
            opts.OnBatchCompleted = _ =>
            {
                callbackCalls++;
                throw new InvalidOperationException("user callback bug");
            };
        });

        Assert.Equal(total, result.RowsInserted);
        Assert.Equal(total / 100, result.BatchesExecuted);
        Assert.Equal(total / 100, callbackCalls);

        var actualOnServer = await ctx.BulkEvents.Where(e => e.EventType == "seq").LongCountAsync();
        Assert.Equal(total, actualOnServer);
    }

    [Fact]
    public async Task BulkInsert_Parallel_KeepsInsertingWhenCallbackThrows()
    {
        await using var ctx = Create();
        await ctx.Database.EnsureCreatedAsync();

        const int total = 600;
        var events = Enumerable.Range(0, total)
            .Select(i => new BulkEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                EventType = "par",
                Data = $"r{i}",
            })
            .ToList();

        var result = await ctx.BulkInsertAsync(events, opts =>
        {
            opts.BatchSize = 100;
            opts.MaxDegreeOfParallelism = 4;
            opts.OnBatchCompleted = _ => throw new InvalidOperationException("user callback bug");
        });

        Assert.Equal(total, result.RowsInserted);
        Assert.Equal(total / 100, result.BatchesExecuted);

        var actualOnServer = await ctx.BulkEvents.Where(e => e.EventType == "par").LongCountAsync();
        Assert.Equal(total, actualOnServer);
    }

    private BulkTestDbContext Create()
    {
        var options = new DbContextOptionsBuilder<BulkTestDbContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;
        return new BulkTestDbContext(options);
    }
}
