using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

/// <summary>
/// Verifies that <c>ON CLUSTER '{cluster}'</c> — emitted when the cluster name
/// is the <c>{cluster}</c> macro — is expanded by the server at DDL time so the
/// same CREATE TABLE propagates to every replica.
/// </summary>
[Collection(ReplicatedClusterCollection.Name)]
public class ClusterMacroDdlTests
{
    private readonly ReplicatedClusterFixture _fx;
    public ClusterMacroDdlTests(ReplicatedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task EntityLevelMacroCluster_PropagatesDdlToAllReplicas()
    {
        var options = new DbContextOptionsBuilder<MacroEntityCtx>()
            .UseClickHouse(_fx.Node1ConnectionString)
            .Options;
        await using var ctx = new MacroEntityCtx(options);

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        foreach (var conn in _fx.AllConnectionStrings)
            Assert.True(await RawClickHouse.TableExistsAsync(conn, "MacroGadgets"),
                $"Expected MacroGadgets to propagate via ON CLUSTER '{{cluster}}' to {conn}");
    }

    [Fact]
    public async Task OptionsLevelMacroCluster_PropagatesDdlToAllReplicas()
    {
        var options = new DbContextOptionsBuilder<PlainCtx>()
            .UseClickHouse(_fx.Node1ConnectionString, o => o.UseCluster())
            .Options;
        await using var ctx = new PlainCtx(options);

        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        foreach (var conn in _fx.AllConnectionStrings)
            Assert.True(await RawClickHouse.TableExistsAsync(conn, "MacroPlain"),
                $"Expected MacroPlain to propagate via options-level UseCluster() to {conn}");
    }

    public sealed class MacroEntityCtx(DbContextOptions<MacroEntityCtx> o) : DbContext(o)
    {
        public DbSet<Gadget> Widgets => Set<Gadget>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Gadget>(e =>
            {
                e.ToTable("MacroGadgets");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                // Parameterless overload — defers cluster resolution to the
                // server-side {cluster} macro.
                e.UseCluster();
            });
    }

    public sealed class PlainCtx(DbContextOptions<PlainCtx> o) : DbContext(o)
    {
        public DbSet<Plain> Rows => Set<Plain>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Plain>(e =>
            {
                e.ToTable("MacroPlain");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }

    public class Gadget { public long Id { get; set; } public string Name { get; set; } = ""; }
    public class Plain { public long Id { get; set; } public string Value { get; set; } = ""; }
}
