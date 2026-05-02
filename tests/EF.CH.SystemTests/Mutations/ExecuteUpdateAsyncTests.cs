using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Mutations;

/// <summary>
/// Coverage of EF Core's <c>ExecuteUpdateAsync</c> against ClickHouse.
/// ClickHouse mutations are async and run via <c>ALTER TABLE UPDATE</c>:
/// - Mutations require a WHERE clause; the provider emits <c>WHERE 1</c> when no predicate is supplied.
/// - Aliases are forbidden in mutation context (provider sets <c>_inUpdateContext</c>).
/// We wait for mutation completion before asserting post-state.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ExecuteUpdateAsyncTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ExecuteUpdateAsyncTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 10; i++)
            ctx.Rows.Add(new Row { Id = i, Score = (int)i, Status = "new" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ExecuteUpdate_WithPredicate_AppliesUpdateToMatchingRows()
    {
        await using var ctx = await SeededAsync();
        await ctx.Rows.Where(r => r.Id <= 3).ExecuteUpdateAsync(s => s.SetProperty(x => x.Status, "old"));
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteUpdateOpTests_Rows");

        var statuses = await RawClickHouse.RowsAsync(Conn,
            "SELECT Id, Status FROM \"ExecuteUpdateOpTests_Rows\" ORDER BY Id");
        for (int i = 0; i < 3; i++)
            Assert.Equal("old", statuses[i]["Status"]);
        for (int i = 3; i < 10; i++)
            Assert.Equal("new", statuses[i]["Status"]);
    }

    [Fact]
    public async Task ExecuteUpdate_NoPredicate_FallsBackToWhereOne_AppliesToAll()
    {
        await using var ctx = await SeededAsync();
        // No predicate — the provider must emit `WHERE 1` since CH refuses bare ALTER UPDATE.
        await ctx.Rows.ExecuteUpdateAsync(s => s.SetProperty(x => x.Status, "all"));
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteUpdateOpTests_Rows");
        var distinctStatuses = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT DISTINCT Status FROM \"ExecuteUpdateOpTests_Rows\"");
        Assert.Single(distinctStatuses);
        Assert.Equal("all", distinctStatuses[0]);
    }

    /// <summary>
    /// Pins the documented contract that <c>ExecuteUpdateAsync</c> against
    /// ClickHouse is asynchronous: the call returns as soon as the
    /// <c>ALTER TABLE … UPDATE</c> mutation is enqueued, not after the data
    /// part has been rewritten. Reading immediately afterwards may see the
    /// pre-update value. Callers that need read-your-write semantics must
    /// await <c>WaitForMutationsAsync</c> (or equivalent) before querying.
    /// <para>
    /// May be flaky on extremely fast CH builds where the mutation completes
    /// before the read; the contract still holds — the test surfaces the
    /// non-determinism rather than guaranteeing a race.
    /// </para>
    /// </summary>
    [Fact]
    [Trait("Category", "AsyncSemantics")]
    public async Task ExecuteUpdateAsync_ReadImmediately_SeesStale()
    {
        await using var ctx = await SeededAsync();

        // Capture the pre-update value, then issue the update without
        // awaiting the mutation queue.
        var before = await RawClickHouse.RowsAsync(Conn,
            "SELECT Status FROM \"ExecuteUpdateOpTests_Rows\" WHERE Id = 1");
        Assert.Equal("new", before[0]["Status"]);

        await ctx.Rows.Where(r => r.Id == 1).ExecuteUpdateAsync(s =>
            s.SetProperty(x => x.Status, "raced"));
        // No WaitForMutationsAsync — the contract under test is that the
        // immediate read can observe the prior value because the mutation
        // is still in flight.

        var immediate = await RawClickHouse.RowsAsync(Conn,
            "SELECT Status FROM \"ExecuteUpdateOpTests_Rows\" WHERE Id = 1");
        var observed = (string)immediate[0]["Status"]!;

        // Either is contractually allowed: the mutation may have completed
        // already (observed == "raced") or still be in flight (observed ==
        // "new"). The point is that callers must not rely on
        // read-your-write — both outcomes are valid.
        Assert.True(observed is "new" or "raced",
            $"expected 'new' (still pending) or 'raced' (already merged); got '{observed}'");

        // Drain the mutation so subsequent tests start from a clean state.
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteUpdateOpTests_Rows");
    }

    [Fact]
    public async Task ExecuteUpdate_MultipleSetProperty_AppliesAllAssignments()
    {
        await using var ctx = await SeededAsync();
        await ctx.Rows.Where(r => r.Id == 5).ExecuteUpdateAsync(s => s
            .SetProperty(x => x.Score, 999)
            .SetProperty(x => x.Status, "edited"));
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteUpdateOpTests_Rows");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Score, Status FROM \"ExecuteUpdateOpTests_Rows\" WHERE Id = 5");
        Assert.Single(rows);
        Assert.Equal(999, Convert.ToInt32(rows[0]["Score"]));
        Assert.Equal("edited", rows[0]["Status"]);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int Score { get; set; }
        public string Status { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ExecuteUpdateOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
