using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EF.CH.Tests.Storage;

/// <summary>
/// Pins the connection-string parsing for the <c>Server=</c> alias. Users
/// migrating from SQL Server / MySQL providers commonly write
/// <c>Server=hostname;…</c> rather than ClickHouse's preferred <c>Host=…</c>,
/// and the underlying <c>ClickHouse.Driver</c> does not accept <c>Server=</c>
/// so the connection silently points at the default host. <c>UseClickHouse</c>
/// should accept either spelling.
/// </summary>
public class ConnectionStringServerAliasTests
{
    [Fact]
    public void UseClickHouse_AcceptsServerAlias_AsHost()
    {
        var hostStyle = new DbContextOptionsBuilder<EmptyCtx>()
            .UseClickHouse("Host=example.host;Port=8123;Database=db1")
            .Options;
        var serverStyle = new DbContextOptionsBuilder<EmptyCtx>()
            .UseClickHouse("Server=example.host;Port=8123;Database=db1")
            .Options;

        using var hostCtx = new EmptyCtx(hostStyle);
        using var serverCtx = new EmptyCtx(serverStyle);

        var hostConnString = hostCtx.GetService<IRelationalConnection>().ConnectionString!;
        var serverConnString = serverCtx.GetService<IRelationalConnection>().ConnectionString!;

        // After normalization both shapes must produce a connection string the
        // ClickHouse driver understands — i.e., the same parsed host.
        Assert.Contains("Host=example.host", hostConnString, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Host=example.host", serverConnString, StringComparison.OrdinalIgnoreCase);
    }

    public sealed class EmptyCtx(DbContextOptions<EmptyCtx> o) : DbContext(o);
}
