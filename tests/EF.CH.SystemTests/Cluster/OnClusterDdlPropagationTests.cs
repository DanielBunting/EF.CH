using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

[Collection(ReplicatedClusterCollection.Name)]
public class OnClusterDdlPropagationTests
{
    private readonly ReplicatedClusterFixture _fx;
    public OnClusterDdlPropagationTests(ReplicatedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task DeployOnNode1_CreatesTableOnAllReplicas()
    {
        await using var node1 = TestContextFactory.CreateWithCluster<Ctx>(_fx.Node1ConnectionString, _fx.ClusterName);
        await node1.Database.EnsureDeletedAsync();
        await node1.Database.EnsureCreatedAsync();

        foreach (var conn in _fx.AllConnectionStrings)
            Assert.True(await RawClickHouse.TableExistsAsync(conn, "Widgets"),
                $"Expected Widgets table to have propagated to replica at {conn}");
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Widget> Widgets => Set<Widget>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Widget>(e =>
            {
                e.ToTable("Widgets"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }

    public class Widget { public long Id { get; set; } public string Name { get; set; } = ""; }
}
