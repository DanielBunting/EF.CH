using System.Reflection;
using EF.CH.Configuration;
using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// Pins fault-tolerance behaviours of <see cref="ClickHouseConnectionPool"/>:
///   - Concurrent round-robin contention does not skip endpoints
///   - When all endpoints are unhealthy, GetConnection falls back rather than
///     returning null
///   - Unhealthy endpoints are excluded from selection
///
/// These are unit pins (no Docker). Live failover behaviour is covered by
/// <c>EF.CH.SystemTests/Cluster/ConnectionRoutingIntegrationTests</c>.
/// </summary>
public class ConnectionPoolFaultToleranceTests
{
    [Fact]
    public async Task RoundRobin_ConcurrentBorrowers_HitAllEndpoints()
    {
        var pool = new ClickHouseConnectionPool(new ConnectionConfig());
        pool.AddEndpoint("a:8123");
        pool.AddEndpoint("b:8123");
        pool.AddEndpoint("c:8123");

        var seen = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, 600),
            new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount * 4 },
            (_, _) =>
            {
                using var conn = pool.GetConnection(ReadStrategy.RoundRobin);
                seen.AddOrUpdate(conn.ConnectionString, 1, (_, v) => v + 1);
                return ValueTask.CompletedTask;
            });

        // All 3 endpoints must show up; with 600 calls and 3 endpoints we
        // expect ~200 each, but allow plenty of slack for scheduler effects.
        Assert.Equal(3, seen.Count);
        Assert.All(seen.Values, v => Assert.InRange(v, 100, 400));
    }

    [Fact]
    public void GetConnection_AllEndpointsUnhealthy_FallsBackInsteadOfFailing()
    {
        var pool = new ClickHouseConnectionPool(new ConnectionConfig());
        pool.AddEndpoint("a:8123");
        pool.AddEndpoint("b:8123");
        ForceAllUnhealthy(pool);

        // The pool falls back to all endpoints when none are healthy
        // (a recovery channel — better to return *something* and let the
        // caller's Open() fail explicitly than to throw on a stale flag).
        using var conn = pool.GetConnection(ReadStrategy.RoundRobin);
        Assert.NotNull(conn);
    }

    [Fact]
    public void GetConnection_OneUnhealthy_NeverReturnsTheUnhealthyEndpoint()
    {
        var pool = new ClickHouseConnectionPool(new ConnectionConfig());
        pool.AddEndpoint("alive:8123");
        pool.AddEndpoint("dead:8123");
        ForceUnhealthy(pool, "dead:8123");

        // Hammer round-robin and confirm the unhealthy endpoint is never picked.
        for (var i = 0; i < 50; i++)
        {
            using var conn = pool.GetConnection(ReadStrategy.RoundRobin);
            Assert.DoesNotContain("dead", conn.ConnectionString);
        }
    }

    [Fact]
    public void GetConnection_NoEndpoints_ThrowsClearError()
    {
        var pool = new ClickHouseConnectionPool(new ConnectionConfig());

        var ex = Assert.Throws<ClickHouseConnectionException>(() =>
            pool.GetConnection(ReadStrategy.RoundRobin));

        Assert.Contains("endpoint", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // -- Reflection helpers ---------------------------------------------------
    // EndpointState is private; we reach in to flip health for tests rather
    // than running real failed connections (which would need Docker and a
    // real broken endpoint, doubling test cost for no extra signal).

    private static void ForceAllUnhealthy(ClickHouseConnectionPool pool)
    {
        var endpoints = GetEndpointStates(pool);
        foreach (var ep in endpoints) MarkUnhealthyEnoughTimes(ep);
    }

    private static void ForceUnhealthy(ClickHouseConnectionPool pool, string endpoint)
    {
        var endpoints = GetEndpointStates(pool);
        foreach (var ep in endpoints)
        {
            var endpointField = ep.GetType()
                .GetField("_endpoint", BindingFlags.Instance | BindingFlags.NonPublic)!;
            if ((string?)endpointField.GetValue(ep) == endpoint)
                MarkUnhealthyEnoughTimes(ep);
        }
    }

    private static IEnumerable<object> GetEndpointStates(ClickHouseConnectionPool pool)
    {
        var dictField = typeof(ClickHouseConnectionPool)
            .GetField("_endpoints", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var dict = (System.Collections.IEnumerable)dictField.GetValue(pool)!;
        foreach (var entry in dict)
        {
            var valueProp = entry.GetType().GetProperty("Value")!;
            yield return valueProp.GetValue(entry)!;
        }
    }

    private static void MarkUnhealthyEnoughTimes(object endpointState)
    {
        // The state stays healthy until consecutive failures hit MaxRetries.
        // Defaults to a small number; call MarkUnhealthy enough times to clear
        // any threshold (and a few extra for safety).
        var method = endpointState.GetType().GetMethod("MarkUnhealthy")!;
        for (var i = 0; i < 100; i++)
            method.Invoke(endpointState, null);
    }
}
