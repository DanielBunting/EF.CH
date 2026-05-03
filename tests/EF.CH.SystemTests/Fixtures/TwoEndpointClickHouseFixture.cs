using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// Two independent ClickHouse containers (write-node and read-node) on a shared Docker
/// network. Used by connection-routing tests; tests seed each node with distinguishable
/// data so routing decisions can be observed via query results.
/// </summary>
public sealed class TwoEndpointClickHouseFixture : IAsyncLifetime
{
    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private INetwork? _network;

    public ClickHouseContainer WriteNode { get; private set; } = null!;
    public ClickHouseContainer ReadNode { get; private set; } = null!;

    public string WriteNodeAlias { get; private set; } = "";
    public string ReadNodeAlias { get; private set; } = "";

    public string WriteConnectionString => WriteNode.GetConnectionString();
    public string ReadConnectionString => ReadNode.GetConnectionString();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-route-{_token}").Build();
        await _network.CreateAsync();

        WriteNodeAlias = $"ch-write-{_token}";
        ReadNodeAlias = $"ch-read-{_token}";

        WriteNode = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .WithNetworkAliases(WriteNodeAlias)
            .WithHostname(WriteNodeAlias)
            .Build();

        ReadNode = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .WithNetworkAliases(ReadNodeAlias)
            .WithHostname(ReadNodeAlias)
            .Build();

        await Task.WhenAll(WriteNode.StartAsync(), ReadNode.StartAsync());
    }

    public async Task DisposeAsync()
    {
        try { await WriteNode.DisposeAsync(); } catch { }
        try { await ReadNode.DisposeAsync(); } catch { }
        if (_network is not null) try { await _network.DeleteAsync(); } catch { }
    }
}
