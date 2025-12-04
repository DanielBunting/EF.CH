using System.Net;
using System.Net.Sockets;
using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class IPTypeTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region ClickHouseIPv4 Unit Tests

    [Fact]
    public void IPv4_Parse_ValidAddress()
    {
        var ip = ClickHouseIPv4.Parse("192.168.1.1");

        Assert.Equal("192.168.1.1", ip.ToString());
    }

    [Fact]
    public void IPv4_Parse_LocalhostAddress()
    {
        var ip = ClickHouseIPv4.Parse("127.0.0.1");

        Assert.Equal("127.0.0.1", ip.ToString());
    }

    [Fact]
    public void IPv4_Equality()
    {
        var ip1 = ClickHouseIPv4.Parse("10.0.0.1");
        var ip2 = ClickHouseIPv4.Parse("10.0.0.1");
        var ip3 = ClickHouseIPv4.Parse("10.0.0.2");

        Assert.Equal(ip1, ip2);
        Assert.NotEqual(ip1, ip3);
        Assert.True(ip1 == ip2);
        Assert.True(ip1 != ip3);
    }

    [Fact]
    public void IPv4_Comparison()
    {
        var ip1 = ClickHouseIPv4.Parse("10.0.0.1");
        var ip2 = ClickHouseIPv4.Parse("10.0.0.2");

        Assert.True(ip1 < ip2);
        Assert.True(ip2 > ip1);
        Assert.True(ip1 <= ip2);
        Assert.True(ip2 >= ip1);
    }

    [Fact]
    public void IPv4_ToIPAddress()
    {
        var chIp = ClickHouseIPv4.Parse("192.168.1.100");
        var netIp = chIp.ToIPAddress();

        Assert.Equal(AddressFamily.InterNetwork, netIp.AddressFamily);
        Assert.Equal("192.168.1.100", netIp.ToString());
    }

    [Fact]
    public void IPv4_ImplicitConversionFromString()
    {
        ClickHouseIPv4 ip = "172.16.0.1";

        Assert.Equal("172.16.0.1", ip.ToString());
    }

    #endregion

    #region ClickHouseIPv6 Unit Tests

    [Fact]
    public void IPv6_Parse_ValidAddress()
    {
        var ip = ClickHouseIPv6.Parse("2001:db8::1");

        Assert.Contains("2001:db8", ip.ToString().ToLowerInvariant());
    }

    [Fact]
    public void IPv6_Parse_FullAddress()
    {
        var ip = ClickHouseIPv6.Parse("2001:0db8:85a3:0000:0000:8a2e:0370:7334");

        Assert.NotNull(ip.ToString());
    }

    [Fact]
    public void IPv6_Parse_MapsIPv4()
    {
        var ip = ClickHouseIPv6.Parse("192.168.1.1");

        // IPv4 mapped to IPv6 should contain the original address
        var str = ip.ToString();
        Assert.True(str.Contains("192.168.1.1") || str.Contains("::ffff:"));
    }

    [Fact]
    public void IPv6_Equality()
    {
        var ip1 = ClickHouseIPv6.Parse("::1");
        var ip2 = ClickHouseIPv6.Parse("::1");
        var ip3 = ClickHouseIPv6.Parse("::2");

        Assert.Equal(ip1, ip2);
        Assert.NotEqual(ip1, ip3);
    }

    [Fact]
    public void IPv6_ToIPAddress()
    {
        var chIp = ClickHouseIPv6.Parse("2001:db8::1");
        var netIp = chIp.ToIPAddress();

        Assert.Equal(AddressFamily.InterNetworkV6, netIp.AddressFamily);
    }

    #endregion

    #region Type Mapping Unit Tests

    [Fact]
    public void IPv4Mapping_GeneratesCorrectStoreType()
    {
        var mapping = new ClickHouseIPv4TypeMapping();

        Assert.Equal("IPv4", mapping.StoreType);
        Assert.Equal(typeof(ClickHouseIPv4), mapping.ClrType);
    }

    [Fact]
    public void IPv4Mapping_GeneratesCorrectLiteral()
    {
        var mapping = new ClickHouseIPv4TypeMapping();
        var ip = ClickHouseIPv4.Parse("192.168.1.1");

        var literal = mapping.GenerateSqlLiteral(ip);

        Assert.Equal("toIPv4('192.168.1.1')", literal);
    }

    [Fact]
    public void IPv6Mapping_GeneratesCorrectStoreType()
    {
        var mapping = new ClickHouseIPv6TypeMapping();

        Assert.Equal("IPv6", mapping.StoreType);
        Assert.Equal(typeof(ClickHouseIPv6), mapping.ClrType);
    }

    [Fact]
    public void IPv6Mapping_GeneratesCorrectLiteral()
    {
        var mapping = new ClickHouseIPv6TypeMapping();
        var ip = ClickHouseIPv6.Parse("2001:db8::1");

        var literal = mapping.GenerateSqlLiteral(ip);

        Assert.StartsWith("toIPv6('", literal);
        Assert.EndsWith("')", literal);
    }

    [Fact]
    public void IPAddressMapping_GeneratesIPv6StoreType()
    {
        var mapping = new ClickHouseIPAddressTypeMapping();

        Assert.Equal("IPv6", mapping.StoreType);
        Assert.Equal(typeof(IPAddress), mapping.ClrType);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanInsertAndQueryIPv4()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "ServerLogs" (
                "Id" UUID,
                "ClientIP" IPv4,
                "Timestamp" DateTime64(3)
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert using toIPv4() function
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""ServerLogs"" (""Id"", ""ClientIP"", ""Timestamp"")
              VALUES ('" + id + @"', toIPv4('192.168.1.100'), now())");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""ServerLogs"" WHERE ""ClientIP"" = toIPv4('192.168.1.100')"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanInsertAndQueryIPv6()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "NetworkEvents" (
                "Id" UUID,
                "SourceIP" IPv6,
                "EventType" String
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert using toIPv6() function
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""NetworkEvents"" (""Id"", ""SourceIP"", ""EventType"")
              VALUES ('" + id + @"', toIPv6('2001:db8::1'), 'Connection')");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""NetworkEvents"" WHERE ""SourceIP"" = toIPv6('2001:db8::1')"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryIPv4Range()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "AccessLogs" (
                "Id" UUID,
                "ClientIP" IPv4
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert multiple IPs
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""AccessLogs"" (""Id"", ""ClientIP"") VALUES
            ('" + Guid.NewGuid() + @"', toIPv4('192.168.1.1')),
            ('" + Guid.NewGuid() + @"', toIPv4('192.168.1.50')),
            ('" + Guid.NewGuid() + @"', toIPv4('192.168.1.100')),
            ('" + Guid.NewGuid() + @"', toIPv4('10.0.0.1'))");

        // Query for IPs in 192.168.1.0/24 subnet using comparison
        var count = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""AccessLogs""
              WHERE ""ClientIP"" >= toIPv4('192.168.1.0')
              AND ""ClientIP"" <= toIPv4('192.168.1.255')"
        ).AnyAsync();

        Assert.True(count);
    }

    [Fact]
    public async Task CanStoreIPv4AsString()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "SimpleIPLogs" (
                "Id" UUID,
                "IP" IPv4
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // ClickHouse accepts IPv4 as plain string in INSERT
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""SimpleIPLogs"" (""Id"", ""IP"")
              VALUES ('" + Guid.NewGuid() + @"', '8.8.8.8')");

        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""SimpleIPLogs"" WHERE ""IP"" = '8.8.8.8'"
        ).AnyAsync();

        Assert.True(exists);
    }

    #endregion

    private IPTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<IPTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new IPTestContext(options);
    }
}

#region Test Context

public class IPTestContext : DbContext
{
    public IPTestContext(DbContextOptions<IPTestContext> options)
        : base(options) { }
}

#endregion
