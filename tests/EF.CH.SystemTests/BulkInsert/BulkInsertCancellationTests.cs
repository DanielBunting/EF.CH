using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Cancellation propagation through <c>BulkInsertAsync</c> /
/// <c>BulkInsertStreamingAsync</c>. The streaming path is most prone to
/// hang-on-cancel since it pulls from <c>IAsyncEnumerable</c>; cancelling
/// mid-stream must yield <see cref="OperationCanceledException"/> rather
/// than draining the source.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BulkInsertCancellationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BulkInsertCancellationTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task BulkInsertStreaming_CancellationToken_StopsEnumeration()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        using var cts = new CancellationTokenSource();
        var enumerated = 0;

        async IAsyncEnumerable<Row> Source(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            for (uint i = 1; i <= 1_000_000; i++)
            {
                ct.ThrowIfCancellationRequested();
                Interlocked.Increment(ref enumerated);
                yield return new Row { Id = i, Tag = "x" };
                if (i == 2_500) cts.Cancel();
                if (i % 500 == 0) await Task.Yield();
            }
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await ctx.BulkInsertStreamingAsync(
                Source(cts.Token),
                o => o.WithBatchSize(1_000),
                cts.Token);
        });

        // The enumeration stopped soon after cancel — not after pulling the full source.
        Assert.True(enumerated < 100_000,
            $"streaming enumerator should have stopped soon after cancel; pulled {enumerated} entities");
    }

    [Fact]
    public async Task BulkInsert_CancelledBeforeStart_ThrowsImmediately()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var rows = Enumerable.Range(1, 100).Select(i => new Row { Id = (uint)i, Tag = "x" }).ToList();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ctx.BulkInsertAsync(rows, _ => { }, cts.Token));
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Tag { get; set; } = "";
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("BulkCancel_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
