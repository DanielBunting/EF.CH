using System.Reflection;
using EF.CH.Configuration;
using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// Pins the connection-string rebuilder used by <see cref="ClickHouseRoutingConnection"/>.
/// On every reconnect it constructs a fresh string from the active endpoint plus
/// the <see cref="ConnectionConfig"/>; the previous implementation copied
/// <c>Host</c>, <c>Port</c>, and <c>Database</c> but silently dropped
/// <c>Username</c> and <c>Password</c>. Result: routed deployments with auth
/// reconnect as the default user (or fail "authentication required" against
/// locked-down clusters).
/// </summary>
public class RoutingConnectionStringTests
{
    [Fact]
    public void GetConnectionString_IncludesUsernameAndPassword_WhenConfigured()
    {
        var config = new ConnectionConfig
        {
            WriteEndpoint = "writer.example:9000",
            Database = "analytics",
            Username = "ef_user",
            Password = "ef_pass",
        };
        var conn = new ClickHouseRoutingConnection(config);

        var rebuilt = (string)typeof(ClickHouseRoutingConnection)
            .GetMethod("GetConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(conn, [])!;

        Assert.Contains("Host=writer.example", rebuilt);
        Assert.Contains("Port=9000", rebuilt);
        Assert.Contains("Database=analytics", rebuilt);
        Assert.Contains("ef_user", rebuilt);
        Assert.Contains("ef_pass", rebuilt);
    }

    [Fact]
    public void GetConnectionString_OmitsUsernamePassword_WhenNotConfigured()
    {
        var config = new ConnectionConfig
        {
            WriteEndpoint = "writer.example:9000",
            Database = "analytics",
        };
        var conn = new ClickHouseRoutingConnection(config);

        var rebuilt = (string)typeof(ClickHouseRoutingConnection)
            .GetMethod("GetConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(conn, [])!;

        Assert.DoesNotContain("User=", rebuilt, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Password=", rebuilt, StringComparison.OrdinalIgnoreCase);
    }
}
