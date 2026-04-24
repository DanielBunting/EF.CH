using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

[Collection(ReplicatedClusterCollection.Name)]
public class ReplicatedClusterSetupTests
{
    private readonly ReplicatedClusterFixture _fx;
    public ReplicatedClusterSetupTests(ReplicatedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task SystemClusters_ReportsThreeReplicasOnOneShard()
    {
        var rows = await RawClickHouse.RowsAsync(_fx.Node1ConnectionString,
            $"SELECT shard_num, replica_num, host_name FROM system.clusters WHERE cluster = '{_fx.ClusterName}' ORDER BY shard_num, replica_num");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.Equal(1UL, Convert.ToUInt64(r["shard_num"])));
        var hosts = rows.Select(r => (string)r["host_name"]!).OrderBy(h => h).ToArray();
        Assert.Equal(_fx.ReplicaHostnames.OrderBy(h => h), hosts);
    }

    [Fact]
    public async Task Macros_ReportShardAndReplicaPerNode()
    {
        foreach (var (conn, expectedReplica) in _fx.AllConnectionStrings.Zip(_fx.ReplicaHostnames))
        {
            var shard = await RawClickHouse.ScalarAsync<string>(conn, "SELECT getMacro('shard')");
            var replica = await RawClickHouse.ScalarAsync<string>(conn, "SELECT getMacro('replica')");
            Assert.Equal("1", shard);
            Assert.Equal(expectedReplica, replica);
        }
    }
}

[Collection(ShardedClusterCollection.Name)]
public class ShardedClusterSetupTests
{
    private readonly ShardedClusterFixture _fx;
    public ShardedClusterSetupTests(ShardedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task SystemClusters_ReportsThreeShardsOneReplicaEach()
    {
        var rows = await RawClickHouse.RowsAsync(_fx.Shard1ConnectionString,
            $"SELECT shard_num, replica_num FROM system.clusters WHERE cluster = '{_fx.ClusterName}' ORDER BY shard_num");

        Assert.Equal(3, rows.Count);
        Assert.Equal(new[] { 1UL, 2UL, 3UL },
            rows.Select(r => Convert.ToUInt64(r["shard_num"])).OrderBy(s => s).ToArray());
        Assert.All(rows, r => Assert.Equal(1UL, Convert.ToUInt64(r["replica_num"])));
    }

    [Fact]
    public async Task ShardMacro_DiffersAcrossNodes()
    {
        var shards = new List<string>();
        foreach (var conn in _fx.AllConnectionStrings)
            shards.Add(await RawClickHouse.ScalarAsync<string>(conn, "SELECT getMacro('shard')"));
        Assert.Equal(new[] { "1", "2", "3" }, shards.OrderBy(s => s).ToArray());
    }
}
