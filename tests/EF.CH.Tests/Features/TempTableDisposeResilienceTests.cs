using EF.CH.Extensions;
using EF.CH.TempTable;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Pins <see cref="TempTableHandle{T}.DisposeAsync"/>'s exception-handling
/// contract. The DROP TABLE inside DisposeAsync is best-effort cleanup —
/// connectivity-class failures should not propagate, otherwise a user code
/// path like <c>try { ... } finally { await handle.DisposeAsync(); }</c>
/// silently replaces the original (interesting) exception with a downstream
/// (boring) "connection closed" error.
/// </summary>
public class TempTableDisposeResilienceTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task DisposeAsync_WhenServerUnreachable_DoesNotPropagateException()
    {
        // Stand up a temp table, then stop the ClickHouse container so the
        // DROP TABLE that DisposeAsync emits has no server to talk to. Without
        // the swallow, the connectivity exception propagates and shadows
        // whatever the caller was already handling in their `finally` block.
        await using var ctx = Create();
        await ctx.Database.EnsureCreatedAsync();
        var manager = ctx.GetService<IClickHouseTempTableManager>();

        var handle = await manager.CreateAsync<DisposeResilienceRow>();

        // Kill the connection before dispose. ClickHouse driver throws on
        // ExecuteNonQuery against a stopped server.
        await _container.StopAsync();

        // DisposeAsync must NOT propagate the connectivity failure.
        await handle.DisposeAsync();
    }

    private DisposeResilienceCtx Create()
    {
        var options = new DbContextOptionsBuilder<DisposeResilienceCtx>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;
        return new DisposeResilienceCtx(options);
    }

    public sealed class DisposeResilienceRow
    {
        public uint Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class DisposeResilienceCtx(DbContextOptions<DisposeResilienceCtx> o) : DbContext(o)
    {
        public DbSet<DisposeResilienceRow> Rows => Set<DisposeResilienceRow>();
        protected override void OnModelCreating(ModelBuilder mb)
            => mb.Entity<DisposeResilienceRow>(e =>
            {
                e.ToTable("dispose_resilience_rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
