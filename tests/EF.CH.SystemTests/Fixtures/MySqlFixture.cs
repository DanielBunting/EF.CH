using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Testcontainers.MySql;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// Co-located ClickHouse + MySQL pair on a private Docker network for external MySQL
/// entity tests.
/// </summary>
public sealed class MySqlFixture : IAsyncLifetime
{
    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private INetwork? _network;
    private MySqlContainer? _mysql;
    private ClickHouseContainer? _clickhouse;

    public string MySqlHostAlias { get; private set; } = "";
    public string MySqlUser => "myuser";
    public string MySqlPassword => "mypass";
    public string MySqlDatabase => "mydb";

    public string MySqlConnectionString => _mysql!.GetConnectionString();
    public string ClickHouseConnectionString => _clickhouse!.GetConnectionString();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-mysql-{_token}").Build();
        await _network.CreateAsync();

        MySqlHostAlias = $"mysql-{_token}";

        _mysql = new MySqlBuilder()
            .WithNetwork(_network)
            .WithNetworkAliases(MySqlHostAlias)
            .WithHostname(MySqlHostAlias)
            .WithUsername(MySqlUser)
            .WithPassword(MySqlPassword)
            .WithDatabase(MySqlDatabase)
            .Build();

        _clickhouse = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .Build();

        await Task.WhenAll(_mysql.StartAsync(), _clickhouse.StartAsync());
    }

    public async Task DisposeAsync()
    {
        if (_clickhouse is not null) try { await _clickhouse.DisposeAsync(); } catch { }
        if (_mysql is not null) try { await _mysql.DisposeAsync(); } catch { }
        if (_network is not null) try { await _network.DeleteAsync(); } catch { }
    }
}
