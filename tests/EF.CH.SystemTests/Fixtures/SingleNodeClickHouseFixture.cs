using ClickHouse.Driver.ADO;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

public sealed class SingleNodeClickHouseFixture : IAsyncLifetime
{
    private readonly string _hostname = $"ch-single-{Guid.NewGuid().ToString("N")[..8]}";
    private readonly ClickHouseContainer _container;

    public SingleNodeClickHouseFixture()
    {
        var keeperConfig = ClusterConfigTemplates.BuildSingleNodeKeeperConfig(_hostname);
        // Macros let zooKeeperPath('/clickhouse/tables/{shard}/test') and similar
        // path-templates resolve. Use a fixed cluster name and shard 1 so single-node
        // queries that exercise the macro surface produce stable output.
        var macrosConfig = ClusterConfigTemplates.BuildMacros(
            clusterName: "single", shardNum: 1, replicaName: _hostname);

        _container = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithHostname(_hostname)
            .WithResourceMapping(keeperConfig, "/etc/clickhouse-server/config.d/keeper.xml")
            .WithResourceMapping(macrosConfig, "/etc/clickhouse-server/config.d/macros.xml")
            .Build();
    }

    /// <summary>Native-protocol connection string (mapped 9000 port). Used by EF queries.</summary>
    public string ConnectionString => _container.GetConnectionString();

    /// <summary>The mapped host port for the ClickHouse HTTP interface (8123).</summary>
    public int HttpPort => _container.GetMappedPublicPort(8123);

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        await WaitForKeeperReadyAsync();
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    private async Task WaitForKeeperReadyAsync()
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        Exception? last = null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await using var conn = new ClickHouseConnection(ConnectionString);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT count() FROM system.zookeeper WHERE path = '/'";
                await cmd.ExecuteScalarAsync();
                return;
            }
            catch (Exception ex)
            {
                last = ex;
                await Task.Delay(500);
            }
        }
        throw new InvalidOperationException(
            "Embedded Keeper did not become ready within 30s on the single-node fixture.", last);
    }
}
