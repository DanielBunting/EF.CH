using EF.CH.Configuration;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

/// <summary>
/// Live failover scenario: with three real ClickHouse replicas in a pool, kill one
/// node mid-test and verify reads continue to succeed via the surviving replicas.
/// This is the gap that <c>ConnectionPoolFailoverIntegrationTests</c> deliberately
/// avoided (it only kills a synthetic 127.0.0.1:1 endpoint). The fixture's
/// <see cref="ReplicatedClusterFixture.StopNodeAsync"/> primitive makes a real
/// node outage reachable from a test.
/// </summary>
[Collection(ReplicatedClusterCollection.Name)]
public sealed class LiveNodeFailoverTests
{
    private readonly ReplicatedClusterFixture _fx;
    public LiveNodeFailoverTests(ReplicatedClusterFixture fx) => _fx = fx;

    [Fact]
    public async Task PoolReadsContinue_AfterOneNodeStops()
    {
        // Seed each surviving replica with a sentinel row so we can prove the
        // pool actually answered from one of them after node 1 went down.
        foreach (var conn in _fx.AllConnectionStrings)
        {
            await RawClickHouse.ExecuteAsync(conn, "DROP TABLE IF EXISTS sentinel SYNC");
        }
        // Use ON CLUSTER so the table exists on all three replicas.
        await RawClickHouse.ExecuteAsync(_fx.Node1ConnectionString,
            $"CREATE TABLE sentinel ON CLUSTER '{_fx.ClusterName}' (name String) " +
            $"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{uuid}}', '{{replica}}') ORDER BY name");
        await RawClickHouse.ExecuteAsync(_fx.Node1ConnectionString,
            "INSERT INTO sentinel VALUES ('alive')");
        foreach (var conn in _fx.AllConnectionStrings)
            await RawClickHouse.WaitForReplicationAsync(conn, "sentinel");

        var (h1, p1) = ParseHostPort(_fx.Node1ConnectionString);
        var (h2, p2) = ParseHostPort(_fx.Node2ConnectionString);
        var (h3, p3) = ParseHostPort(_fx.Node3ConnectionString);

        var config = new ConnectionConfig
        {
            Database = "default",
            ReadEndpoints = new List<string> { $"{h1}:{p1}", $"{h2}:{p2}", $"{h3}:{p3}" },
            ReadStrategy = ReadStrategy.RoundRobin,
            Username = "clickhouse",
            Password = "clickhouse",
            Failover = new FailoverConfig { MaxRetries = 1 },
        };
        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint($"{h1}:{p1}");
        pool.AddEndpoint($"{h2}:{p2}");
        pool.AddEndpoint($"{h3}:{p3}");

        await pool.HealthCheckAsync();
        Assert.Equal(3, pool.HealthyEndpointCount);

        // Take node 1 down. The pool's next health check should mark it unhealthy;
        // subsequent reads must succeed via nodes 2 or 3.
        await _fx.StopNodeAsync(1);
        try
        {
            await pool.HealthCheckAsync();
            Assert.Equal(2, pool.HealthyEndpointCount);

            for (int i = 0; i < 10; i++)
            {
                await using var conn = pool.GetConnection(ReadStrategy.RoundRobin);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT name FROM sentinel LIMIT 1";
                var v = await cmd.ExecuteScalarAsync();
                Assert.Equal("alive", v);
            }
        }
        finally
        {
            // Restore the node so the rest of the suite sees a 3-node cluster.
            await _fx.StartNodeAsync(1);
            // Wait for the node to rejoin Keeper before health-checking — without this
            // a parallel test class that runs immediately could see a transient outage.
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    await RawClickHouse.ExecuteAsync(_fx.Node1ConnectionString, "SELECT 1");
                    break;
                }
                catch { await Task.Delay(500); }
            }
        }
    }

    private static (string Host, int Port) ParseHostPort(string connectionString)
    {
        string host = "localhost";
        int port = 9000;
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = part.Split('=', 2);
            if (kv.Length != 2) continue;
            if (kv[0].Trim().Equals("Host", StringComparison.OrdinalIgnoreCase)) host = kv[1].Trim();
            else if (kv[0].Trim().Equals("Port", StringComparison.OrdinalIgnoreCase) && int.TryParse(kv[1].Trim(), out var p)) port = p;
        }
        return (host, port);
    }
}
