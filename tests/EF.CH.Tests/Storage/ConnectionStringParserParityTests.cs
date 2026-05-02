using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EF.CH.Tests.Storage;

/// <summary>
/// EF.CH has two independent connection-string parsers: <c>UseClickHouse</c>
/// (entry point — strips <c>HttpPort=</c>, normalises <c>Server=</c>→<c>Host=</c>)
/// and <c>ClickHouseExportExtensions.ParseConnectionString</c> (used by export
/// HTTP calls — extracts host + port + auth). Past divergences silently routed
/// the export APIs to the wrong port. These tests pin the <c>(host, httpPort)</c>
/// resolution across both parsers for the same input matrix, where the contract
/// is <c>httpPortOverride ?? port ?? 8123</c>.
/// </summary>
public class ConnectionStringParserParityTests
{
    [Theory]
    [InlineData("Host=h;Port=9000",                       "h", 9000)]
    [InlineData("Host=h;Port=9000;HttpPort=8123",         "h", 8123)]
    [InlineData("Server=h;Port=9000",                     "h", 9000)]
    [InlineData("Host=h;HttpPort=8123",                   "h", 8123)]
    [InlineData("Host=h",                                 "h", 8123)]
    public void UseClickHouseAndParseConnectionString_ResolveSameHostAndHttpPort(
        string input, string expectedHost, int expectedHttpPort)
    {
        var (efHost, efHttpPort) = ResolveViaUseClickHouse(input);
        var (exportHost, exportHttpPort) = ResolveViaExportParser(input);

        Assert.Equal(expectedHost, efHost);
        Assert.Equal(expectedHost, exportHost);

        Assert.Equal(expectedHttpPort, efHttpPort);
        Assert.Equal(expectedHttpPort, exportHttpPort);

        // Cross-parity: both parsers agree.
        Assert.Equal(efHost, exportHost);
        Assert.Equal(efHttpPort, exportHttpPort);
    }

    private static (string Host, int HttpPort) ResolveViaUseClickHouse(string connectionString)
    {
        var options = new DbContextOptionsBuilder<EmptyCtx>()
            .UseClickHouse(connectionString)
            .Options;

        using var ctx = new EmptyCtx(options);
        var driverConnString = ctx.GetService<IRelationalConnection>().ConnectionString!;
        var ext = ctx.GetService<IDbContextOptions>().FindExtension<ClickHouseOptionsExtension>()!;

        var host = ExtractField(driverConnString, "Host") ?? "localhost";
        var portFromConn = TryExtractInt(driverConnString, "Port");
        var httpPort = ext.HttpPort ?? portFromConn ?? 8123;

        return (host, httpPort);
    }

    private static (string Host, int HttpPort) ResolveViaExportParser(string connectionString)
    {
        // UseClickHouse strips `HttpPort=` and stores it on the extension; the
        // export parser receives the cleaned conn string + the override. Mirror
        // that wiring so the parity test reflects the runtime call shape.
        var options = new DbContextOptionsBuilder<EmptyCtx>()
            .UseClickHouse(connectionString)
            .Options;

        using var ctx = new EmptyCtx(options);
        var cleanedConnString = ctx.GetService<IRelationalConnection>().ConnectionString!;
        var httpPortOverride = ctx.GetService<IDbContextOptions>()
            .FindExtension<ClickHouseOptionsExtension>()!.HttpPort;

        var parse = typeof(ClickHouseExportExtensions).GetMethod(
            "ParseConnectionString",
            BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("ParseConnectionString not found");

        var result = parse.Invoke(null, new object?[] { cleanedConnString, httpPortOverride })!;
        var baseUrl = (string)result.GetType().GetField("Item1")!.GetValue(result)!;

        var uri = new Uri(baseUrl);
        return (uri.Host, uri.Port);
    }

    private static string? ExtractField(string connectionString, string key)
    {
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = part.Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals(key, StringComparison.OrdinalIgnoreCase))
                return kv[1].Trim();
        }
        return null;
    }

    private static int? TryExtractInt(string connectionString, string key)
        => int.TryParse(ExtractField(connectionString, key), out var v) ? v : null;

    private sealed class EmptyCtx(DbContextOptions<EmptyCtx> o) : DbContext(o);
}
