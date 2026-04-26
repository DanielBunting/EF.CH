using EF.CH.BulkInsert;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Coverage of <c>ClickHouseBulkInsertResult</c>: <c>RowsInserted</c>, <c>BatchesExecuted</c>,
/// <c>Elapsed</c>, <c>RowsPerSecond</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BulkInsertResultStatsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BulkInsertResultStatsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task BulkInsertResult_PopulatesAllStats()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 1_000).Select(i => new Row { Id = (uint)i, V = i }).ToList();
        var result = await ctx.BulkInsertAsync(rows, o => o.WithBatchSize(250));

        Assert.Equal(1_000L, result.RowsInserted);
        Assert.True(result.BatchesExecuted >= 4);
        Assert.True(result.Elapsed > TimeSpan.Zero);
        Assert.True(result.RowsPerSecond > 0);

        // The reported stats must agree with the actual landed row count.
        var landed = await RawClickHouse.RowCountAsync(Conn, "BulkInsertResult_Rows");
        Assert.Equal(1_000ul, landed);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("BulkInsertResult_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
