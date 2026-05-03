using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Testcontainers.Redis;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// Co-located ClickHouse + Redis pair for external Redis entity tests.
/// </summary>
public sealed class RedisFixture : IAsyncLifetime
{
    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private INetwork? _network;
    private RedisContainer? _redis;
    private ClickHouseContainer? _clickhouse;

    public string RedisHostAlias { get; private set; } = "";
    public int RedisInternalPort => 6379;

    public string RedisConnectionString => _redis!.GetConnectionString();
    public string ClickHouseConnectionString => _clickhouse!.GetConnectionString();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-redis-{_token}").Build();
        await _network.CreateAsync();

        RedisHostAlias = $"redis-{_token}";

        _redis = new RedisBuilder()
            .WithNetwork(_network)
            .WithNetworkAliases(RedisHostAlias)
            .WithHostname(RedisHostAlias)
            .Build();

        _clickhouse = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .Build();

        await Task.WhenAll(_redis.StartAsync(), _clickhouse.StartAsync());
    }

    public async Task DisposeAsync()
    {
        if (_clickhouse is not null) try { await _clickhouse.DisposeAsync(); } catch { }
        if (_redis is not null) try { await _redis.DisposeAsync(); } catch { }
        if (_network is not null) try { await _network.DeleteAsync(); } catch { }
    }
}
