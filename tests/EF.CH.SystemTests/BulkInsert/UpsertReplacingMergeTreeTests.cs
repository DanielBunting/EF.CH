using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.BulkInsert;

/// <summary>
/// Coverage of <c>UpsertRangeAsync</c> against ReplacingMergeTree. After OPTIMIZE FINAL
/// the latest version per key wins.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class UpsertReplacingMergeTreeTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public UpsertReplacingMergeTreeTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task UpsertRange_AgainstReplacing_LatestVersionWins()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await ctx.Items.UpsertRangeAsync(new[]
        {
            new Item { Id = 1, Version = 1, Score = 10 },
            new Item { Id = 2, Version = 1, Score = 20 },
        });
        await ctx.Items.UpsertRangeAsync(new[]
        {
            new Item { Id = 1, Version = 2, Score = 100 },  // newer version wins
        });

        await RawClickHouse.SettleMaterializationAsync(Conn, "UpsertReplacing_Items");
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Id, Version, Score FROM \"UpsertReplacing_Items\" FINAL ORDER BY Id");
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, Convert.ToInt32(rows[0]["Version"]));
        Assert.Equal(100, Convert.ToInt32(rows[0]["Score"]));
    }

    public sealed class Item
    {
        public uint Id { get; set; }
        public uint Version { get; set; }
        public int Score { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Item>(e =>
            {
                e.ToTable("UpsertReplacing_Items");
                e.HasKey(x => new { x.Id, x.Version });
                e.UseReplacingMergeTree(x => x.Id).WithVersion(x => x.Version);
            });
    }
}
