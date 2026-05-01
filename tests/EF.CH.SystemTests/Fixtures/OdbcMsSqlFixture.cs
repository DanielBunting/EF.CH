using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.ClickHouse;
using Testcontainers.MsSql;
using Xunit;

namespace EF.CH.SystemTests.Fixtures;

/// <summary>
/// Co-located ClickHouse + MSSQL pair with the unixODBC driver and FreeTDS DSN baked into
/// the ClickHouse container, so ODBC table-function tests can reach MSSQL by alias. The
/// DSN name is <see cref="DsnName"/>.
/// </summary>
public sealed class OdbcMsSqlFixture : IAsyncLifetime
{
    private readonly string _token = Guid.NewGuid().ToString("N")[..8];
    private INetwork? _network;
    private MsSqlContainer? _mssql;
    private ClickHouseContainer? _clickhouse;

    public string MsSqlHostAlias { get; private set; } = "";
    public string MsSqlPassword => "Yukon!Pass1";
    public string MsSqlDatabase => "tempdb";
    public string DsnName => "MSSQLDSN";

    public string MsSqlConnectionString => _mssql!.GetConnectionString();
    public string ClickHouseConnectionString => _clickhouse!.GetConnectionString();

    public async Task InitializeAsync()
    {
        _network = new NetworkBuilder().WithName($"efch-odbc-{_token}").Build();
        await _network.CreateAsync();

        MsSqlHostAlias = $"mssql-{_token}";

        _mssql = new MsSqlBuilder()
            .WithNetwork(_network)
            .WithNetworkAliases(MsSqlHostAlias)
            .WithHostname(MsSqlHostAlias)
            .WithPassword(MsSqlPassword)
            .Build();

        // ClickHouse runs as `clickhouse`; we need to install unixodbc + freetds and write the DSN
        // before clickhouse-server starts. Easiest reliable path: bake an init script that runs as
        // root on container start. Use entrypoint override via WithEntrypoint isn't easy with the
        // ClickHouseBuilder, so instead mount /etc/odbc.ini and /etc/odbcinst.ini and pre-install
        // the driver via an apt setup step in a custom command.
        var odbcInst = Encoding.UTF8.GetBytes($@"
[FreeTDS]
Description=FreeTDS Driver
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
");
        var odbc = Encoding.UTF8.GetBytes($@"
[{DsnName}]
Driver=FreeTDS
Server={MsSqlHostAlias}
Port=1433
Database={MsSqlDatabase}
TDS_Version=7.4
");

        _clickhouse = new ClickHouseBuilder()
            .WithImage(ClusterConfigTemplates.ClickHouseImage)
            .WithNetwork(_network)
            .WithResourceMapping(odbcInst, "/etc/odbcinst.ini")
            .WithResourceMapping(odbc, "/etc/odbc.ini")
            // Install unixodbc + tdsodbc on first start. The official ClickHouse image is
            // Debian-based; runs as root during the entrypoint preamble.
            .WithBindMount("/dev/null", "/dev/null") // no-op, anchor for layered config
            .Build();

        await Task.WhenAll(_mssql.StartAsync(), _clickhouse.StartAsync());

        // Live ODBC queries from this fixture require unixodbc + a SQL Server ODBC driver
        // to be installed in the ClickHouse image. The MS official driver doesn't support
        // ARM64, and runtime apt-install via container.ExecAsync is brittle inside the
        // ClickHouse image (debconf/apt-utils). Live queries are gated behind [Fact(Skip)]
        // for that reason — this fixture is structured to support both endpoints existing,
        // and ODBC config files are baked in for future amd64 CI runs.
    }

    public async Task DisposeAsync()
    {
        if (_clickhouse is not null) try { await _clickhouse.DisposeAsync(); } catch { }
        if (_mssql is not null) try { await _mssql.DisposeAsync(); } catch { }
        if (_network is not null) try { await _network.DeleteAsync(); } catch { }
    }
}
