using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

/// <summary>
/// End-to-end coverage of <c>UseTableGroup</c> + <c>AddTableGroup</c> against
/// a real replicated cluster. Unit tests in <c>EF.CH.Tests/Cluster/TableGroupTests</c>
/// pin the wiring and DDL-rendering logic; this fixture proves a table-group
/// definition with <c>UseCluster</c> + <c>Replicated = true</c> actually
/// produces a ReplicatedMergeTree on every node.
/// </summary>
[Collection(ReplicatedClusterCollection.Name)]
public class TableGroupDdlPropagationTests
{
    private readonly ReplicatedClusterFixture _fx;
    public TableGroupDdlPropagationTests(ReplicatedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task TableGroupWithCluster_PropagatesDdlToAllReplicas()
    {
        // The table-group annotation supplies the cluster name so EnsureCreated
        // emits ON CLUSTER. Replication of the engine itself is configured on
        // the engine builder (`WithReplication`); the table-group scaffolding is
        // orthogonal and only handles cluster targeting.
        var options = new DbContextOptionsBuilder<TgCtx>()
            .UseClickHouse(_fx.Node1ConnectionString, o =>
            {
                o.AddTableGroup("Core", g => g
                    .UseCluster(_fx.ClusterName)
                    .Replicated());
            })
            .Options;
        await using var ctx = new TgCtx(options);

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        foreach (var conn in _fx.AllConnectionStrings)
        {
            Assert.True(await RawClickHouse.TableExistsAsync(conn, "TgCore_Items"),
                $"TgCore_Items expected on {conn}");
            var engine = await RawClickHouse.EngineFullAsync(conn, "TgCore_Items");
            Assert.Contains("ReplicatedMergeTree", engine);
        }

        // Insert on node 1 — replication carries it to nodes 2 & 3 because the
        // engine itself is ReplicatedMergeTree.
        ctx.Items.AddRange(Enumerable.Range(1, 5).Select(i => new TgItem { Id = i, Tag = $"t-{i}" }));
        await ctx.SaveChangesAsync();

        foreach (var conn in _fx.AllConnectionStrings)
        {
            await RawClickHouse.WaitForReplicationAsync(conn, "TgCore_Items");
            Assert.Equal(5ul, await RawClickHouse.RowCountAsync(conn, "TgCore_Items"));
        }
    }

    [Fact]
    public async Task TableGroupWithoutCluster_DoesNotEmitOnCluster_OrReplicate()
    {
        // Local-only group: no UseCluster, no Replicated → plain MergeTree on the connected node only.
        var options = new DbContextOptionsBuilder<TgLocalCtx>()
            .UseClickHouse(_fx.Node1ConnectionString, o =>
            {
                o.AddTableGroup("LocalOnly", g => g.NotReplicated());
            })
            .Options;
        await using var ctx = new TgLocalCtx(options);

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(_fx.Node1ConnectionString, "TgLocal_Items"));
        var engine = await RawClickHouse.EngineFullAsync(_fx.Node1ConnectionString, "TgLocal_Items");
        Assert.DoesNotContain("Replicated", engine);
        // Other nodes should NOT have the table — no ON CLUSTER was emitted.
        Assert.False(await RawClickHouse.TableExistsAsync(_fx.Node2ConnectionString, "TgLocal_Items"));
    }

    public sealed class TgItem { public long Id { get; set; } public string Tag { get; set; } = ""; }
    public sealed class TgCtx(DbContextOptions<TgCtx> o) : DbContext(o)
    {
        public DbSet<TgItem> Items => Set<TgItem>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<TgItem>(e =>
            {
                e.ToTable("TgCore_Items"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id).WithReplication("/clickhouse/tables/{uuid}");
                e.UseTableGroup("Core");
            });
    }

    public sealed class TgLocalCtx(DbContextOptions<TgLocalCtx> o) : DbContext(o)
    {
        public DbSet<TgItem> Items => Set<TgItem>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<TgItem>(e =>
            {
                e.ToTable("TgLocal_Items"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.UseTableGroup("LocalOnly");
            });
    }
}
