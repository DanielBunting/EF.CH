using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <see cref="ClickHouseKeeperDbFunctionsExtensions"/>. The Keeper
/// surface depends on a Keeper-enabled cluster; tests that need a live Keeper
/// run against the single-node fixture (which in CH 25+ embeds a Keeper) and
/// will fail loudly if Keeper isn't reachable, exposing the gap.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class KeeperDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public KeeperDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ZooKeeperPath_ResolvesAgainstKeeper()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x =>
            EfClass.Functions.ZooKeeperPath("/clickhouse/tables/{shard}/test")).FirstAsync();
        // The exact resolved path depends on the server's macros; assert the
        // path is non-empty and starts with `/clickhouse` after macro expansion.
        Assert.False(string.IsNullOrEmpty(s));
        Assert.StartsWith("/clickhouse", s);
    }

    [Fact]
    public async Task HasZooKeeperConfig_ReturnsTrueOnReplicatedFixture()
    {
        await using var ctx = await SeededAsync();
        var has = await ctx.Rows.Select(x =>
            EfClass.Functions.HasZooKeeperConfig()).FirstAsync();
        Assert.True(has);
    }

    public sealed class Row { public uint Id { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("KeeperFn_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
