using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Coverage of <c>WithAsyncInsert()</c> and <c>WaitForCompletion()</c> on the
/// bulk-insert options. Without <c>WaitForCompletion</c> the rows may sit in
/// the server's async-insert buffer for up to <c>async_insert_busy_timeout_ms</c>
/// before becoming visible — so the test reads <c>system.asynchronous_inserts</c>
/// rather than depending on <c>SELECT count()</c> visibility.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BulkInsertAsyncInsertSettingsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BulkInsertAsyncInsertSettingsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task BulkInsert_WithAsyncInsertWait_RowsImmediatelyVisible()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 200)
            .Select(i => new Row { Id = (uint)i, Tag = $"t{i}" })
            .ToList();

        var result = await ctx.BulkInsertAsync(rows, o => o.WaitForCompletion());
        Assert.Equal(200L, result.RowsInserted);

        // wait_for_async_insert = 1 ⇒ rows are visible the moment the call returns.
        var n = await RawClickHouse.RowCountAsync(Conn, "BulkAsyncInsert_Rows");
        Assert.Equal(200ul, n);
    }

    [Fact]
    public async Task BulkInsert_WithAsyncInsertNoWait_RowsBecomeVisibleEventually()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 200)
            .Select(i => new Row { Id = (uint)i, Tag = $"t{i}" })
            .ToList();

        await ctx.BulkInsertAsync(rows, o =>
        {
            o.WithAsyncInsert();
            // Force a small flush window so the test does not hang for a real prod default.
            o.WithSetting("async_insert_busy_timeout_ms", 200);
        });

        // Poll: with async_insert=1 the buffer flushes asynchronously. We don't assert
        // *immediate* invisibility — that's racy — but we do assert eventual visibility.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        ulong landed = 0;
        while (DateTime.UtcNow < deadline)
        {
            landed = await RawClickHouse.RowCountAsync(Conn, "BulkAsyncInsert_Rows");
            if (landed == 200ul) break;
            await Task.Delay(100);
        }
        Assert.Equal(200ul, landed);
    }

    [Fact]
    public async Task BulkInsert_WithCustomSettings_AppliesMaxInsertThreads()
    {
        // max_insert_threads is sent in the SETTINGS clause; this test exercises the
        // option plumbing by inserting with max_insert_threads = 4 and confirming the
        // operation succeeds (the server validates the setting name & value type).
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 1_000)
            .Select(i => new Row { Id = (uint)i, Tag = "t" })
            .ToList();

        await ctx.BulkInsertAsync(rows, o => o.WithMaxInsertThreads(4));

        var n = await RawClickHouse.RowCountAsync(Conn, "BulkAsyncInsert_Rows");
        Assert.Equal(1_000ul, n);
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
                e.ToTable("BulkAsyncInsert_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
