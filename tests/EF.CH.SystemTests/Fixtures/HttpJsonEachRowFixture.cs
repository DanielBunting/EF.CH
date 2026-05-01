using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// nginx sidecar serving a small <c>JSONEachRow</c> payload at <c>/data.jsonl</c>, paired
/// with a ClickHouse container on the same Docker network. Used by <c>FromUrl</c> tests so
/// ClickHouse can fetch via Docker DNS without depending on <c>host.docker.internal</c>
/// (unreliable on Linux CI).
/// </summary>
public sealed class HttpJsonEachRowFixture : IAsyncLifetime
{
    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private INetwork? _network;
    private IContainer? _nginx;
    private ClickHouseContainer? _clickhouse;

    public string NginxHostAlias { get; private set; } = "";
    public int NginxInternalPort => 80;

    /// <summary>The payload nginx serves at <c>/data.jsonl</c>. Three rows.</summary>
    public string JsonEachRowPayload { get; } =
        "{\"id\":1,\"name\":\"alpha\",\"score\":10.5}\n" +
        "{\"id\":2,\"name\":\"bravo\",\"score\":20.25}\n" +
        "{\"id\":3,\"name\":\"charlie\",\"score\":30.0}\n";

    public string ClickHouseConnectionString => _clickhouse!.GetConnectionString();

    /// <summary>URL the ClickHouse container should use to reach nginx.</summary>
    public string InternalUrl => $"http://{NginxHostAlias}:{NginxInternalPort}/data.jsonl";

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-http-{_token}").Build();
        await _network.CreateAsync();

        NginxHostAlias = $"nginx-{_token}";

        _nginx = new ContainerBuilder()
            .WithImage("nginx:alpine")
            .WithNetwork(_network)
            .WithNetworkAliases(NginxHostAlias)
            .WithHostname(NginxHostAlias)
            // Map nginx port to a random host port so the health check (runs from host) can reach it.
            .WithPortBinding(NginxInternalPort, true)
            .WithResourceMapping(
                Encoding.UTF8.GetBytes(JsonEachRowPayload),
                "/usr/share/nginx/html/data.jsonl")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort((ushort)NginxInternalPort).ForPath("/")))
            .Build();

        _clickhouse = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .Build();

        await Task.WhenAll(_nginx.StartAsync(), _clickhouse.StartAsync());
    }

    public async Task DisposeAsync()
    {
        if (_clickhouse is not null) try { await _clickhouse.DisposeAsync(); } catch { }
        if (_nginx is not null) try { await _nginx.DisposeAsync(); } catch { }
        if (_network is not null) try { await _network.DeleteAsync(); } catch { }
    }
}
