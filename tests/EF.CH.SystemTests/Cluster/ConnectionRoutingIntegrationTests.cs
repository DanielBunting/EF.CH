using EF.CH.Configuration;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

/// <summary>
/// End-to-end coverage for <see cref="ClickHouseRoutingConnection"/> against two real
/// ClickHouse containers. Each node is seeded with a sentinel row identifying which side
/// answered the query, so endpoint switching is observable.
///
/// Note: the EF DbContext layer doesn't currently expose a one-line wire-up that binds a
/// named connection (from <c>AddConnection</c>) to the active connection; the routing
/// connection is therefore exercised directly here. The interceptor's read/write
/// classification logic (<c>ClickHouseCommandInterceptor.IsReadOperation</c>) is covered by
/// unit tests in <c>tests/EF.CH.Tests/Cluster/ConnectionRoutingTests.cs</c>.
/// </summary>
[Collection(TwoEndpointCollection.Name)]
public sealed class ConnectionRoutingIntegrationTests
{
    private readonly TwoEndpointClickHouseFixture _fx;
    public ConnectionRoutingIntegrationTests(TwoEndpointClickHouseFixture fx) => _fx = fx;

    [Fact]
    public async Task RoutingConnection_SwitchActiveEndpoint_ChangesUnderlyingConnection()
    {
        // Seed each node with a different sentinel.
        await SeedAsync(_fx.WriteConnectionString, "WRITE-NODE");
        await SeedAsync(_fx.ReadConnectionString, "READ-NODE");

        // Map the test-process connection strings to the format the routing config expects
        // (host:port). The container fixture exposes mapped native ports.
        var (writeHost, writePort) = ParseHostPort(_fx.WriteConnectionString);
        var (readHost, readPort) = ParseHostPort(_fx.ReadConnectionString);

        var config = new ConnectionConfig
        {
            Database = "default",
            WriteEndpoint = $"{writeHost}:{writePort}",
            ReadEndpoints = new List<string> { $"{readHost}:{readPort}" },
            ReadStrategy = ReadStrategy.PreferFirst,
            Username = "clickhouse",
            Password = "clickhouse",
        };

        await using var routing = new ClickHouseRoutingConnection(config);

        // Default is Write.
        Assert.Equal(EndpointType.Write, routing.ActiveEndpoint);
        Assert.Equal("WRITE-NODE", await ReadSentinelAsync(routing));

        // Switch to Read.
        routing.ActiveEndpoint = EndpointType.Read;
        Assert.Equal("READ-NODE", await ReadSentinelAsync(routing));

        // Back to Write.
        routing.ActiveEndpoint = EndpointType.Write;
        Assert.Equal("WRITE-NODE", await ReadSentinelAsync(routing));
    }

    [Fact]
    public async Task ConnectionPool_PreferFirstStrategy_AlwaysHitsFirstEndpoint()
    {
        await SeedAsync(_fx.ReadConnectionString, "READ-NODE-FIRST");

        var (readHost, readPort) = ParseHostPort(_fx.ReadConnectionString);
        var config = new ConnectionConfig
        {
            Database = "default",
            WriteEndpoint = "ignored:9999",
            ReadEndpoints = new List<string> { $"{readHost}:{readPort}" },
            ReadStrategy = ReadStrategy.PreferFirst,
            Username = "clickhouse",
            Password = "clickhouse",
        };

        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint($"{readHost}:{readPort}");

        for (int i = 0; i < 3; i++)
        {
            var conn = pool.GetConnection(ReadStrategy.PreferFirst);
            Assert.NotNull(conn);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT name FROM sentinel LIMIT 1";
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal("READ-NODE-FIRST", result);
            await conn.CloseAsync();
        }
    }

    private static async Task SeedAsync(string connectionString, string sentinel)
    {
        await RawClickHouse.ExecuteAsync(connectionString, "DROP TABLE IF EXISTS sentinel SYNC");
        await RawClickHouse.ExecuteAsync(connectionString,
            "CREATE TABLE sentinel (name String) ENGINE = MergeTree() ORDER BY name");
        await RawClickHouse.ExecuteAsync(connectionString,
            $"INSERT INTO sentinel VALUES ('{sentinel}')");
    }

    private static async Task<string?> ReadSentinelAsync(System.Data.Common.DbConnection conn)
    {
        if (conn.State != System.Data.ConnectionState.Open)
            await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sentinel LIMIT 1";
        return (await cmd.ExecuteScalarAsync()) as string;
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
