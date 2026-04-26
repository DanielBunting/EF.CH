using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Mutations;

/// <summary>
/// Coverage of EF Core's <c>ExecuteDeleteAsync</c> against ClickHouse.
/// Behind the scenes this maps to <c>ALTER TABLE … DELETE WHERE …</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ExecuteDeleteAsyncTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ExecuteDeleteAsyncTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 10; i++) ctx.Rows.Add(new Row { Id = i, Tag = i % 2 == 0 ? "even" : "odd" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ExecuteDelete_WithPredicate_RemovesMatchingRows()
    {
        await using var ctx = await SeededAsync();
        await ctx.Rows.Where(r => r.Tag == "odd").ExecuteDeleteAsync();
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteDeleteOpTests_Rows");
        var remaining = await RawClickHouse.ColumnAsync<uint>(Conn,
            "SELECT Id FROM \"ExecuteDeleteOpTests_Rows\" ORDER BY Id");
        Assert.Equal(new uint[] { 2, 4, 6, 8, 10 }, remaining);
    }

    [Fact]
    public async Task ExecuteDelete_NoPredicate_RemovesAllRows()
    {
        await using var ctx = await SeededAsync();
        await ctx.Rows.ExecuteDeleteAsync();
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteDeleteOpTests_Rows");
        var n = await RawClickHouse.RowCountAsync(Conn, "ExecuteDeleteOpTests_Rows");
        Assert.Equal(0ul, n);
    }

    [Fact]
    public async Task ExecuteDelete_RangePredicate_HonoredOnIdColumn()
    {
        await using var ctx = await SeededAsync();
        await ctx.Rows.Where(r => r.Id >= 4 && r.Id <= 7).ExecuteDeleteAsync();
        await RawClickHouse.WaitForMutationsAsync(Conn, "ExecuteDeleteOpTests_Rows");
        var remaining = await RawClickHouse.ColumnAsync<uint>(Conn,
            "SELECT Id FROM \"ExecuteDeleteOpTests_Rows\" ORDER BY Id");
        Assert.Equal(new uint[] { 1, 2, 3, 8, 9, 10 }, remaining);
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
                e.ToTable("ExecuteDeleteOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
