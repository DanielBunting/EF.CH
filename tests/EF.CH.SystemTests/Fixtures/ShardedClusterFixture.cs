using ClickHouse.Driver.ADO;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// 3-node ClickHouse cluster with three shards (one replica each), backed by
/// an integrated Keeper quorum across the same three nodes. Entirely
/// provisioned through Testcontainers.
/// </summary>
public sealed class ShardedClusterFixture : IAsyncLifetime
{
    private const int NodeCount = 3;

    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private readonly List<ClickHouseContainer> _nodes = new();
    private INetwork? _network;

    public IReadOnlyList<string> ShardHostnames { get; private set; } = Array.Empty<string>();
    public string ClusterName => ClusterConfigTemplates.ShardedClusterName;

    public string Shard1ConnectionString => _nodes[0].GetConnectionString();
    public string Shard2ConnectionString => _nodes[1].GetConnectionString();
    public string Shard3ConnectionString => _nodes[2].GetConnectionString();
    public IReadOnlyList<string> AllConnectionStrings => _nodes.Select(c => c.GetConnectionString()).ToArray();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-shard-{_token}").Build();
        await _network.CreateAsync();

        var aliases = Enumerable.Range(1, NodeCount)
            .Select(i => $"ch-shard-{_token}-{i}")
            .ToArray();
        ShardHostnames = aliases;

        for (int i = 0; i < NodeCount; i++)
        {
            int serverId = i + 1;
            int shardNum = i + 1;
            var alias = aliases[i];

            var clusterXml = ClusterConfigTemplates.BuildShardedClusterConfig(serverId, aliases);
            var macrosXml = ClusterConfigTemplates.BuildMacros(ClusterName, shardNum, alias);
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
            "Sharded cluster did not become ready within 45s.", last);
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
