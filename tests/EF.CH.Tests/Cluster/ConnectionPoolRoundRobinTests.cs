using System.Reflection;
using EF.CH.Configuration;
using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// Pins the round-robin counter against signed-int overflow. After
/// <c>int.MaxValue</c> calls, an unguarded <c>_roundRobinIndex++ % count</c>
/// produces a negative index and throws <see cref="ArgumentOutOfRangeException"/>
/// when used to index into the endpoint list. Using <c>uint</c> arithmetic
/// keeps the modulo non-negative across the full range of values.
/// </summary>
public class ConnectionPoolRoundRobinTests
{
    [Fact]
    public void RoundRobin_AfterIntMaxValueCalls_StillReturnsValidEndpoint()
    {
        var pool = new ClickHouseConnectionPool(new ConnectionConfig());
        pool.AddEndpoint("a:8123");
        pool.AddEndpoint("b:8123");
        pool.AddEndpoint("c:8123");

        // Force the internal index to a value just below overflow so the next
        // increment trips the wrap. Using reflection because the field is
        // intentionally private.
        var indexField = typeof(ClickHouseConnectionPool)
            .GetField("_roundRobinIndex", BindingFlags.Instance | BindingFlags.NonPublic)!;
        indexField.SetValue(pool, int.MaxValue - 1);

        // Two calls span the overflow boundary. Both must return a connection
        // without throwing — that's the whole regression.
        using var conn1 = pool.GetConnection(ReadStrategy.RoundRobin);
        using var conn2 = pool.GetConnection(ReadStrategy.RoundRobin);
        using var conn3 = pool.GetConnection(ReadStrategy.RoundRobin);

        Assert.NotNull(conn1);
        Assert.NotNull(conn2);
        Assert.NotNull(conn3);
    }

    [Fact]
    public void RoundRobin_DistributesAcrossAllEndpointsOverManyCalls()
    {
        var pool = new ClickHouseConnectionPool(new ConnectionConfig());
        pool.AddEndpoint("a:8123");
        pool.AddEndpoint("b:8123");
        pool.AddEndpoint("c:8123");

        // The connection's ConnectionString carries the host:port of the picked
        // endpoint, so we can confirm round-robin actually rotates without ever
        // needing to talk to a real ClickHouse server.
        var seenHosts = new HashSet<string>();
        for (var i = 0; i < 30; i++)
        {
            using var conn = pool.GetConnection(ReadStrategy.RoundRobin);
            seenHosts.Add(conn.ConnectionString);
        }

        Assert.True(seenHosts.Count >= 3, $"expected to see all 3 endpoints, only saw {seenHosts.Count}");
    }
}
