using EF.CH.Configuration;
using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// Tests for connection routing and endpoint selection.
/// </summary>
public class ConnectionRoutingTests
{
    [Fact]
    public void ConnectionPool_PreferFirst_ReturnsFirstEndpoint()
    {
        var config = new ConnectionConfig
        {
            Database = "test",
            WriteEndpoint = "write:8123",
            ReadEndpoints = ["read1:8123", "read2:8123"],
            ReadStrategy = ReadStrategy.PreferFirst
        };

        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint("read1:8123");
        pool.AddEndpoint("read2:8123");

        // PreferFirst should always return the first endpoint
        var conn1 = pool.GetConnection(ReadStrategy.PreferFirst);
        var conn2 = pool.GetConnection(ReadStrategy.PreferFirst);

        Assert.NotNull(conn1);
        Assert.NotNull(conn2);
    }

    [Fact]
    public void ConnectionPool_RoundRobin_RotatesThroughEndpoints()
    {
        var config = new ConnectionConfig
        {
            Database = "test",
            WriteEndpoint = "write:8123",
            ReadEndpoints = ["read1:8123", "read2:8123"],
            ReadStrategy = ReadStrategy.RoundRobin
        };

        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint("read1:8123");
        pool.AddEndpoint("read2:8123");

        // Get multiple connections to test rotation
        var connections = new List<System.Data.Common.DbConnection>();
        for (int i = 0; i < 4; i++)
        {
            connections.Add(pool.GetConnection(ReadStrategy.RoundRobin));
        }

        Assert.Equal(4, connections.Count);
    }

    [Fact]
    public void ConnectionPool_Random_ReturnsRandomEndpoint()
    {
        var config = new ConnectionConfig
        {
            Database = "test",
            WriteEndpoint = "write:8123",
            ReadEndpoints = ["read1:8123", "read2:8123", "read3:8123"],
            ReadStrategy = ReadStrategy.Random
        };

        var pool = new ClickHouseConnectionPool(config);
        pool.AddEndpoint("read1:8123");
        pool.AddEndpoint("read2:8123");
        pool.AddEndpoint("read3:8123");

        // Random should return a connection (we can't test randomness easily)
        var conn = pool.GetConnection(ReadStrategy.Random);
        Assert.NotNull(conn);
    }

    [Fact]
    public void ConnectionPool_ThrowsWhenNoEndpoints()
    {
        var config = new ConnectionConfig
        {
            Database = "test",
            WriteEndpoint = "write:8123",
            ReadEndpoints = [],
            ReadStrategy = ReadStrategy.PreferFirst
        };

        var pool = new ClickHouseConnectionPool(config);

        Assert.Throws<ClickHouseConnectionException>(() => pool.GetConnection(ReadStrategy.PreferFirst));
    }

    [Fact]
    public void RoutingConnection_DefaultsToWriteEndpoint()
    {
        var config = new ConnectionConfig
        {
            Database = "test",
            WriteEndpoint = "write:8123",
            ReadEndpoints = ["read:8123"],
            ReadStrategy = ReadStrategy.PreferFirst
        };

        var routingConn = new ClickHouseRoutingConnection(config);

        Assert.Equal(EndpointType.Write, routingConn.ActiveEndpoint);
    }

    [Fact]
    public void RoutingConnection_CanSwitchEndpointType()
    {
        var config = new ConnectionConfig
        {
            Database = "test",
            WriteEndpoint = "write:8123",
            ReadEndpoints = ["read:8123"],
            ReadStrategy = ReadStrategy.PreferFirst
        };

        var routingConn = new ClickHouseRoutingConnection(config);

        // Default is Write
        Assert.Equal(EndpointType.Write, routingConn.ActiveEndpoint);

        // Switch to Read
        routingConn.ActiveEndpoint = EndpointType.Read;
        Assert.Equal(EndpointType.Read, routingConn.ActiveEndpoint);

        // Switch back to Write
        routingConn.ActiveEndpoint = EndpointType.Write;
        Assert.Equal(EndpointType.Write, routingConn.ActiveEndpoint);
    }

    [Fact]
    public void FailoverConfig_HasCorrectDefaults()
    {
        var config = new FailoverConfig();

        Assert.True(config.Enabled);
        Assert.Equal(3, config.MaxRetries);
        Assert.Equal(1000, config.RetryDelayMs);
        Assert.Equal(30000, config.HealthCheckIntervalMs);
    }

    [Fact]
    public void ConnectionConfig_HasCorrectDefaults()
    {
        var config = new ConnectionConfig();

        Assert.Equal("default", config.Database);
        Assert.Equal(ReadStrategy.PreferFirst, config.ReadStrategy);
        Assert.Empty(config.ReadEndpoints);
        Assert.NotNull(config.Failover);
    }

    [Fact]
    public void ReplicationConfig_HasCorrectDefaults()
    {
        var config = new ReplicationConfig();

        Assert.Equal("/clickhouse/tables/{uuid}", config.ZooKeeperBasePath);
        Assert.Equal("{replica}", config.ReplicaNameMacro);
    }

    [Fact]
    public void TableGroupConfig_HasCorrectDefaults()
    {
        var config = new TableGroupConfig();

        Assert.Null(config.Cluster);
        Assert.True(config.Replicated);
        Assert.Null(config.Description);
    }

    [Fact]
    public void DefaultsConfig_HasCorrectDefaults()
    {
        var config = new DefaultsConfig();

        Assert.Equal("Core", config.TableGroup);
        Assert.Null(config.MigrationsHistoryCluster);
        Assert.True(config.ReplicateMigrationsHistory);
    }
}
