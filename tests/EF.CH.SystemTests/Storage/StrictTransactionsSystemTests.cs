using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Storage;

/// <summary>
/// End-to-end coverage of <c>UseStrictTransactions()</c> against a real
/// ClickHouse server. The default mode silently substitutes a no-op
/// transaction (since ClickHouse does not support real transactions); strict
/// mode throws on any explicit <c>BeginTransaction</c> attempt.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class StrictTransactionsSystemTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public StrictTransactionsSystemTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task DefaultMode_BeginTransaction_ReturnsNoOp_AndCommitDoesNotRollback()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await using var tx = await ctx.Database.BeginTransactionAsync();

        ctx.Rows.AddRange(
            new Row { Id = 1, Tag = "a" },
            new Row { Id = 2, Tag = "b" });
        await ctx.SaveChangesAsync();

        // No-op transaction: rollback does not undo the writes.
        await tx.RollbackAsync();

        var n = await RawClickHouse.RowCountAsync(Conn, "StrictTx_Rows");
        Assert.Equal(2ul, n);
    }

    [Fact]
    public async Task DefaultMode_SaveChangesAcrossEntities_AllRowsLand()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // EF normally wraps SaveChanges in a transaction. With the no-op transaction,
        // the writes are flushed in arbitrary order — the assertion is just that all
        // rows land, not that they're transactionally atomic.
        ctx.Rows.AddRange(Enumerable.Range(1, 50)
            .Select(i => new Row { Id = (uint)i, Tag = "t" }));
        await ctx.SaveChangesAsync();

        var n = await RawClickHouse.RowCountAsync(Conn, "StrictTx_Rows");
        Assert.Equal(50ul, n);
    }

    [Fact]
    public async Task StrictMode_BeginTransaction_Throws()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn,
            o => o.UseStrictTransactions());
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.Throws<ClickHouseUnsupportedOperationException>(() => ctx.Database.BeginTransaction());
        await Assert.ThrowsAsync<ClickHouseUnsupportedOperationException>(() => ctx.Database.BeginTransactionAsync());
    }

    [Fact]
    public async Task StrictMode_SaveChanges_StillSucceeds_WhenNoExplicitTransaction()
    {
        // EF uses ExecutionStrategy + an internal RelationalTransaction during SaveChanges;
        // strict mode would throw if EF hit the BeginTransaction codepath. The provider
        // works around this by suppressing transaction creation in the EF pipeline. This
        // test is a regression-pin: SaveChanges in strict mode should still succeed.
        await using var ctx = TestContextFactory.Create<Ctx>(Conn,
            o => o.UseStrictTransactions());
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(Enumerable.Range(1, 5)
            .Select(i => new Row { Id = (uint)i, Tag = "t" }));
        await ctx.SaveChangesAsync();

        var n = await RawClickHouse.RowCountAsync(Conn, "StrictTx_Rows");
        Assert.Equal(5ul, n);
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
                e.ToTable("StrictTx_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
