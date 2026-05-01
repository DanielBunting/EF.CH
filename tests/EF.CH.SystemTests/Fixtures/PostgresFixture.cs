using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Testcontainers.PostgreSql;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// Co-located ClickHouse + PostgreSQL pair on a private Docker network so ClickHouse can
/// reach PostgreSQL by container hostname. Used by external Postgres entity tests and the
/// Postgres-source dictionary path (sidesteps the dictGet loopback-auth limitation that
/// blocks the ClickHouse-source dictionary tests in <see cref="MigrateDictionariesTests"/>).
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private INetwork? _network;
    private PostgreSqlContainer? _postgres;
    private ClickHouseContainer? _clickhouse;

    public string PostgresHostAlias { get; private set; } = "";
    public string PostgresUser => "pguser";
    public string PostgresPassword => "pgpass";
    public string PostgresDatabase => "pgdb";

    /// <summary>Connection string from the test process to PostgreSQL (mapped port).</summary>
    public string PostgresConnectionString => _postgres!.GetConnectionString();

    /// <summary>Connection string from the test process to ClickHouse (mapped port).</summary>
    public string ClickHouseConnectionString => _clickhouse!.GetConnectionString();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-pg-{_token}").Build();
        await _network.CreateAsync();

        PostgresHostAlias = $"pg-{_token}";

        _postgres = new PostgreSqlBuilder()
            .WithNetwork(_network)
            .WithNetworkAliases(PostgresHostAlias)
            .WithHostname(PostgresHostAlias)
            .WithUsername(PostgresUser)
            .WithPassword(PostgresPassword)
            .WithDatabase(PostgresDatabase)
            .Build();

        _clickhouse = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .Build();

        await Task.WhenAll(_postgres.StartAsync(), _clickhouse.StartAsync());
    }

    public async Task DisposeAsync()
    {
        if (_clickhouse is not null) try { await _clickhouse.DisposeAsync(); } catch { }
        if (_postgres is not null) try { await _postgres.DisposeAsync(); } catch { }
        if (_network is not null) try { await _network.DeleteAsync(); } catch { }
    }
}
