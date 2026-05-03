using EF.CH.Configuration;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

/// <summary>
/// Live failover scenarios for <see cref="ClickHouseConnectionPool"/>. Unit
/// tests in <c>EF.CH.Tests/Cluster/ConnectionPoolFaultToleranceTests</c>
/// verify the round-robin/health-check logic against in-memory mocks; this
/// fixture exercises the real network path: a deliberately unreachable
/// endpoint mixed with a live one. The pool must mark the unreachable
/// endpoint unhealthy after <c>MaxRetries</c> consecutive failures, then
/// route subsequent requests to the live endpoint.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class ConnectionPoolFailoverIntegrationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ConnectionPoolFailoverIntegrationTests(SingleNodeClickHouseFixture fx) => _fx = fx;

    [Fact]
    public async Task HealthCheck_MarksUnreachableEndpointUnhealthy_AndRoutesToLive()
    {
        var (liveHost, livePort) = ParseHostPort(_fx.ConnectionString);

        var config = new ConnectionConfig
        {
            Database = "default",
            WriteEndpoint = $"{liveHost}:{livePort}",
            ReadEndpoints = new List<string>
            {
                "127.0.0.1:1", // unreachable: TCP RST is fast and deterministic.
                $"{liveHost}:{livePort}",
            },
            ReadStrategy = ReadStrategy.RoundRobin,
            Username = "clickhouse",
            Password = "clickhouse",
            Failover = new FailoverConfig { MaxRetries = 1 },
        };

        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint("127.0.0.1:1");
        pool.AddEndpoint($"{liveHost}:{livePort}");

        Assert.Equal(2, pool.HealthyEndpointCount);

        await pool.HealthCheckAsync();

        // After health check the bogus endpoint should be marked unhealthy.
        Assert.Equal(1, pool.HealthyEndpointCount);

        // Subsequent reads must succeed because the live endpoint is still healthy.
        for (int i = 0; i < 5; i++)
        {
            await using var conn = pool.GetConnection(ReadStrategy.RoundRobin);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1";
            var v = await cmd.ExecuteScalarAsync();
            Assert.Equal((byte)1, v); // ClickHouse returns SELECT 1 as UInt8.
        }
    }

    [Fact]
    public async Task HealthCheck_FallsBackToAllEndpoints_WhenAllUnhealthy()
    {
        // Defensive: when every endpoint is marked unhealthy the pool falls back to
        // returning *some* endpoint (a connection attempt may then recover) rather
        // than throwing immediately. With two unreachable endpoints we should still
        // get a connection object back — only the open() call fails.
        var config = new ConnectionConfig
        {
            Database = "default",
            ReadEndpoints = new List<string> { "127.0.0.1:1", "127.0.0.1:2" },
            ReadStrategy = ReadStrategy.PreferFirst,
            Failover = new FailoverConfig { MaxRetries = 1 },
        };

        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint("127.0.0.1:1");
        pool.AddEndpoint("127.0.0.1:2");

        await pool.HealthCheckAsync();
        Assert.Equal(0, pool.HealthyEndpointCount);

        // GetConnection still hands out an object — the actual failure surfaces on Open.
        await using var conn = pool.GetConnection(ReadStrategy.PreferFirst);
        Assert.NotNull(conn);
    }

    private static (string Host, int Port) ParseHostPort(string connectionString)
    {
        string host = "localhost";
        int port = 9000;
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;
            if (kv[0].Trim().Equals("Host", StringComparison.OrdinalIgnoreCase))
                host = kv[1].Trim();
            else if (kv[0].Trim().Equals("Port", StringComparison.OrdinalIgnoreCase) && int.TryParse(kv[1].Trim(), out var p))
                port = p;
        }
        return (host, port);
    }
}
