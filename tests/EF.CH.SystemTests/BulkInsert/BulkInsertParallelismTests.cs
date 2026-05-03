using EF.CH.BulkInsert;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Coverage of <c>MaxDegreeOfParallelism &gt; 1</c> on <see cref="ClickHouseBulkInsertOptions"/>.
/// Parallel execution opens an independent <c>ClickHouseConnection</c> per batch
/// (see <c>ClickHouseBulkInserter.ExecuteParallelAsync</c>) — a regression that
/// silently serialised batches or lost rows would slip through the existing
/// happy-path tests, which all run sequentially.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BulkInsertParallelismTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BulkInsertParallelismTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task BulkInsert_Parallel_AllBatchesLand(int parallelism)
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        const int rowCount = 8_000;
        const int batchSize = 500;

        var rows = Enumerable.Range(1, rowCount)
            .Select(i => new Row { Id = (uint)i, Tag = $"t-{i % 17}" })
            .ToList();

        var result = await ctx.BulkInsertAsync(rows, o =>
        {
            o.WithBatchSize(batchSize);
            o.WithParallelism(parallelism);
        });

        Assert.Equal(rowCount, result.RowsInserted);
        Assert.Equal(rowCount / batchSize, result.BatchesExecuted);

        var actual = await RawClickHouse.RowCountAsync(Conn, "BulkParallel_Rows");
        Assert.Equal((ulong)rowCount, actual);
    }

    [Fact]
    public async Task BulkInsert_Parallel_ProgressCallback_SeesEveryBatch()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 5_000)
            .Select(i => new Row { Id = (uint)i, Tag = "x" })
            .ToList();

        var callbackCounts = new System.Collections.Concurrent.ConcurrentBag<long>();
        var result = await ctx.BulkInsertAsync(rows, o =>
        {
            o.WithBatchSize(1_000);
            o.WithParallelism(3);
            o.WithProgressCallback(total => callbackCounts.Add(total));
        });

        Assert.Equal(5L, result.BatchesExecuted);
        Assert.Equal(5, callbackCounts.Count);
        // Cumulative monotonic — the largest reported value equals the final total.
        Assert.Equal(5_000L, callbackCounts.Max());
    }

    [Fact]
    public async Task BulkInsert_Parallel_ThrowingProgressCallback_DoesNotAbort()
    {
        // Defensive: a buggy user callback should not corrupt an in-flight parallel
        // bulk insert. Inserter swallows the throw and routes it to OnCallbackException.
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 3_000)
            .Select(i => new Row { Id = (uint)i, Tag = "x" })
            .ToList();

        var observed = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        var result = await ctx.BulkInsertAsync(rows, o =>
        {
            o.WithBatchSize(500);
            o.WithParallelism(4);
            o.OnBatchCompleted = _ => throw new InvalidOperationException("boom");
            o.OnCallbackException = ex => observed.Add(ex);
        });

        // All rows still landed despite every callback throwing.
        Assert.Equal(3_000L, result.RowsInserted);
        var actual = await RawClickHouse.RowCountAsync(Conn, "BulkParallel_Rows");
        Assert.Equal(3_000ul, actual);

        // Exception sink saw all the throws.
        Assert.Equal(6, observed.Count);
        Assert.All(observed, ex => Assert.Equal("boom", ex.Message));
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
                e.ToTable("BulkParallel_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
