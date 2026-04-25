using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// VersionedCollapsingMergeTree(sign, version) collapses paired +1/-1 rows where the version
/// also matches. After OPTIMIZE FINAL only the most-recent surviving row remains.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class VersionedCollapsingMergeTreeTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public VersionedCollapsingMergeTreeTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task VersionedCollapsing_PairsCollapse_LeavingLatestSurvivor()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Initial state.
        ctx.Rows.Add(new Row { Id = 1, Sign = 1,  Version = 1, Score = 100 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        // Cancel + new state.
        ctx.Rows.AddRange(
            new Row { Id = 1, Sign = -1, Version = 1, Score = 100 },
            new Row { Id = 1, Sign =  1, Version = 2, Score = 200 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        await RawClickHouse.SettleMaterializationAsync(Conn, "VerCollapsing_Rows");

        var summed = await RawClickHouse.ScalarAsync<long>(Conn,
            "SELECT sum(Score * Sign) FROM \"VerCollapsing_Rows\" FINAL");
        Assert.Equal(200L, summed);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public sbyte Sign { get; set; }
        public uint Version { get; set; }
        public int Score { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("VerCollapsing_Rows");
                e.HasKey(x => new { x.Id, x.Sign, x.Version });
                e.UseVersionedCollapsingMergeTree(x => x.Sign, x => x.Version, x => x.Id);
            });
    }
}
