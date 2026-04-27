using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Throughput-parity smoke for the synchronous <c>BulkInsert</c> forwarder.
/// No perf assertion — just confirms the sync overload pushes the same row
/// count through the bulk path as the async sibling.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class SyncBulkInsertTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public SyncBulkInsertTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task BulkInsert_Sync_DbContext_RowsLandIntact()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 100)
            .Select(i => new Row { Id = (uint)i, Name = $"name-{i}" })
            .ToList();

        var result = ctx.BulkInsert(rows);
        Assert.Equal(100L, result.RowsInserted);

        var n = await RawClickHouse.RowCountAsync(Conn, "SyncBulkInsert_Rows");
        Assert.Equal(100ul, n);
    }

    [Fact]
    public async Task BulkInsert_Sync_DbSet_RowsLandIntact()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(1, 50)
            .Select(i => new Row { Id = (uint)i, Name = $"name-{i}" })
            .ToList();

        var result = ctx.Rows.BulkInsert(rows);
        Assert.Equal(50L, result.RowsInserted);

        var n = await RawClickHouse.RowCountAsync(Conn, "SyncBulkInsert_Rows");
        Assert.Equal(50ul, n);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("SyncBulkInsert_Rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
