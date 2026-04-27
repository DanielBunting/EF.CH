using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// KeeperMap's same-key overwrite semantic expressed through
/// <c>DbSet&lt;T&gt;.BulkInsertAsync</c>. Bypasses EF's change-tracker so
/// successive writes with the same key collapse into a single row — the
/// KeeperMap engine does the replace server-side.
/// </summary>
// Use the replicated cluster fixture — KeeperMap requires ClickHouse Keeper to be configured,
// which the single-node fixture doesn't provide.
[Collection(ReplicatedClusterCollection.Name)]
public class KeeperMapUpsertTests
{
    private readonly ReplicatedClusterFixture _fixture;
    public KeeperMapUpsertTests(ReplicatedClusterFixture fx) => _fixture = fx;

    [Fact]
    public async Task BulkInsertAsync_SameKey_CollapsesToLatestValue()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.Node1ConnectionString);
        // Use a unique keeper path per test run so re-runs don't collide.
        var cs = _fixture.Node1ConnectionString;
        await RawClickHouse.ExecuteAsync(cs, "DROP TABLE IF EXISTS \"FeatureFlags\"");
        await ctx.Database.EnsureCreatedAsync();

        await ctx.Flags.BulkInsertAsync([new Flag { Name = "beta-search", Enabled = false, RolloutPct = 0 }]);
        await ctx.Flags.BulkInsertAsync([new Flag { Name = "beta-search", Enabled = true, RolloutPct = 100 }]);

        var rowCount = await RawClickHouse.ScalarAsync<ulong>(
            cs, "SELECT count() FROM \"FeatureFlags\"");
        Assert.Equal(1UL, rowCount);

        var row = await RawClickHouse.RowsAsync(cs,
            "SELECT Enabled, RolloutPct FROM \"FeatureFlags\" WHERE Name = 'beta-search'");
        Assert.Single(row);
        Assert.Equal(1, Convert.ToInt32(row[0]["Enabled"]));
        Assert.Equal(100u, Convert.ToUInt32(row[0]["RolloutPct"]));
    }

    public class Flag
    {
        public string Name { get; set; } = "";
        public bool Enabled { get; set; }
        public uint RolloutPct { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Flag> Flags => Set<Flag>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Flag>(e =>
            {
                e.ToTable("FeatureFlags");
                e.HasKey(x => x.Name);
                e.UseKeeperMapEngine("/clickhouse/efch-test/feature-flags", x => x.Name);
            });
    }
}
