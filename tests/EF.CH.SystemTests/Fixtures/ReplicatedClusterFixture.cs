using ClickHouse.Driver.ADO;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// 3-node ClickHouse cluster with one shard and three replicas, backed by an
/// integrated Keeper quorum across the same three nodes. Entirely provisioned
/// through Testcontainers — no docker-compose or host-side config files.
/// </summary>
public sealed class ReplicatedClusterFixture : IAsyncLifetime
{
    private const int NodeCount = 3;

    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private readonly List<ClickHouseContainer> _nodes = new();
    private INetwork? _network;

    public IReadOnlyList<string> ReplicaHostnames { get; private set; } = Array.Empty<string>();
    public string ClusterName => ClusterConfigTemplates.ReplicatedClusterName;

    public string Node1ConnectionString => _nodes[0].GetConnectionString();
    public string Node2ConnectionString => _nodes[1].GetConnectionString();
    public string Node3ConnectionString => _nodes[2].GetConnectionString();

    public IReadOnlyList<string> AllConnectionStrings => _nodes.Select(c => c.GetConnectionString()).ToArray();

    /// <summary>
    /// Stops the underlying container for a node. Used by live-failover tests that
    /// need to exercise the connection pool's recovery path against a real outage.
    /// Indices are 1-based to match Node1/Node2/Node3 connection-string accessors.
    /// </summary>
    public Task StopNodeAsync(int nodeIndex)
    {
        if (nodeIndex < 1 || nodeIndex > _nodes.Count)
            throw new ArgumentOutOfRangeException(nameof(nodeIndex));
        return _nodes[nodeIndex - 1].StopAsync();
    }

    /// <summary>Starts a previously-stopped node.</summary>
    public Task StartNodeAsync(int nodeIndex)
    {
        if (nodeIndex < 1 || nodeIndex > _nodes.Count)
            throw new ArgumentOutOfRangeException(nameof(nodeIndex));
        return _nodes[nodeIndex - 1].StartAsync();
    }

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-repl-{_token}").Build();
        await _network.CreateAsync();

        var aliases = Enumerable.Range(1, NodeCount)
            .Select(i => $"ch-repl-{_token}-{i}")
            .ToArray();
        ReplicaHostnames = aliases;

        for (int i = 0; i < NodeCount; i++)
        {
            int serverId = i + 1;
            var alias = aliases[i];

            var clusterXml = ClusterConfigTemplates.BuildReplicatedClusterConfig(serverId, aliases);
            var macrosXml = ClusterConfigTemplates.BuildMacros(ClusterName, shardNum: 1, replicaName: alias);
            var usersXml = ClusterConfigTemplates.BuildAccessManagementGrant("clickhouse");

            var container = new ClickHouseBuilder()
                .WithImage(ClusterConfigTemplates.ClickHouseImage)
                .WithNetwork(_network)
                .WithNetworkAliases(alias)
                .WithHostname(alias)
                .WithResourceMapping(clusterXml, "/etc/clickhouse-server/config.d/cluster.xml")
                .WithResourceMapping(macrosXml, "/etc/clickhouse-server/config.d/macros.xml")
                .WithResourceMapping(usersXml, "/etc/clickhouse-server/users.d/grants.xml")
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(r => r.ForPort(8123).ForPath("/ping")))
                .Build();

            _nodes.Add(container);
        }

        await Task.WhenAll(_nodes.Select(c => c.StartAsync()));
        await WaitForClusterReadyAsync();
    }

    public async Task DisposeAsync()
    {
        foreach (var node in _nodes)
        {
            try { await node.DisposeAsync(); } catch { /* best effort */ }
        }

        if (_network is not null)
        {
            try { await _network.DeleteAsync(); } catch { /* best effort */ }
        }
    }

    private async Task WaitForClusterReadyAsync()
    {
        var deadline = DateTime.UtcNow.AddSeconds(45);
        Exception? last = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                foreach (var conn in AllConnectionStrings)
                {
                    var count = await QueryScalarAsync(conn,
                        $"SELECT count() FROM system.clusters WHERE cluster = '{ClusterName}'");
                    if (count < NodeCount)
                        throw new InvalidOperationException(
                            $"Cluster '{ClusterName}' has {count} entries, expected {NodeCount}.");

                    // Keeper must be reachable — any ZK path query exercises the client.
                    await QueryScalarAsync(conn, "SELECT count() FROM system.zookeeper WHERE path = '/'");
                }
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(500);
            }
        }

        throw new InvalidOperationException(
            "Cluster did not become ready within 45s.", last);
    }

    private static async Task<long> QueryScalarAsync(string connectionString, string sql)
    {
        await using var conn = new ClickHouseConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }
}
