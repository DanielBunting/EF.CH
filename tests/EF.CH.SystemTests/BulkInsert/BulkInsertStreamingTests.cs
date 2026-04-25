using EF.CH.BulkInsert;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Coverage of <c>BulkInsertStreamingAsync</c> over an <c>IAsyncEnumerable</c>.
/// We feed the streaming variant a batch large enough to exercise batching.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BulkInsertStreamingTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BulkInsertStreamingTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task BulkInsertStreaming_HonoursBatchSize_AndReportsBatches()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        async IAsyncEnumerable<Row> Source()
        {
            for (uint i = 1; i <= 25_000; i++)
            {
                yield return new Row { Id = i, Tag = "x" };
                if (i % 5_000 == 0) await Task.Yield();
            }
        }

        var result = await ctx.BulkInsertStreamingAsync(Source(), o => o.WithBatchSize(5_000));
        Assert.Equal(25_000L, result.RowsInserted);
        Assert.True(result.BatchesExecuted >= 5, $"expected ≥5 batches, got {result.BatchesExecuted}");

        var n = await RawClickHouse.RowCountAsync(Conn, "BulkInsertStream_Rows");
        Assert.Equal(25_000ul, n);
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
                e.ToTable("BulkInsertStream_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
