using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Bulk insert into a keyless (<c>HasNoKey</c>) source table such as a
/// Null-engine MV source. <c>DbSet.AddRange</c> can't accept keyless entities
/// through EF's change-tracker, but <c>BulkInsertAsync</c> bypasses it.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class KeylessBulkInsertTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public KeylessBulkInsertTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public async Task BulkInsertAsync_OnKeylessNullEngineSource_FlowsThroughToTargetMv()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(0, 50).Select(i => new Row
        {
            Key = i % 5, UserId = i, Amount = i * 1.5,
        }).ToList();

        var result = await ctx.Incoming.BulkInsertAsync(rows);
        Assert.Equal(rows.Count, (int)result.RowsInserted);

        // Null engine discards rows; the MV target holds the aggregate.
        await RawClickHouse.SettleMaterializationAsync(_fixture.ConnectionString, "KeylessAggTarget");
        var count = await RawClickHouse.ScalarAsync<ulong>(_fixture.ConnectionString,
            "SELECT count() FROM \"KeylessAggTarget\"");
        Assert.True(count > 0);
    }

    public class Row
    {
        public long Key { get; set; }
        public long UserId { get; set; }
        public double Amount { get; set; }
    }

    public class Target
    {
        public long Key { get; set; }
        public byte[] AmountTotal { get; set; } = Array.Empty<byte>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Incoming => Set<Row>();
        public DbSet<Target> Target => Set<Target>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e =>
            {
                e.ToTable("Incoming"); e.HasNoKey(); e.UseNullEngine();
            });
            mb.Entity<Target>(e =>
            {
                e.ToTable("KeylessAggTarget"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Key);
                e.Property(x => x.AmountTotal).HasAggregateFunction("sum", typeof(double));

            });
            mb.MaterializedView<Target>().From<Row>().DefinedAs(src => src
                    .GroupBy(r => r.Key)
                    .Select(g => new Target
                    {
                        Key = g.Key,
                        AmountTotal = g.SumState(r => r.Amount),
                    }));
        }
    }
}
