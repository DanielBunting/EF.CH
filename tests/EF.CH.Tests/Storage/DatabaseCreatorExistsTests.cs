using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EF.CH.Tests.Storage;

/// <summary>
/// <c>Exists()</c> previously caught every exception and returned <c>false</c>, so a
/// misconfigured connection string, network failure, or permission error looked
/// identical to "database does not exist" — and <c>EnsureCreated()</c> then attempted
/// <c>CREATE DATABASE</c>, which failed with an unrelated error a long way from the
/// real cause. These tests pin the new behaviour: configuration / connectivity
/// failures must propagate.
/// </summary>
public class DatabaseCreatorExistsTests
{
    [Fact]
    public void Exists_WithUnreachableHost_ThrowsInsteadOfReturningFalse()
    {
        // Use an obviously-unreachable host on a port nothing should be listening on.
        var options = new DbContextOptionsBuilder<ExistsCtx>()
            .UseClickHouse("Host=127.0.0.1;Port=1;Database=any_db")
            .Options;

        using var context = new ExistsCtx(options);
        var creator = context.GetService<IRelationalDatabaseCreator>();

        // Note: "Returns false" is the bug. Catch any exception type — driver-specific.
        Assert.ThrowsAny<Exception>(() => creator.Exists());
    }

    [Fact]
    public async Task ExistsAsync_WithUnreachableHost_ThrowsInsteadOfReturningFalse()
    {
        var options = new DbContextOptionsBuilder<ExistsCtx>()
            .UseClickHouse("Host=127.0.0.1;Port=1;Database=any_db")
            .Options;

        await using var context = new ExistsCtx(options);
        var creator = context.GetService<IRelationalDatabaseCreator>();

        await Assert.ThrowsAnyAsync<Exception>(() => creator.ExistsAsync());
    }

    public sealed class ExistsCtx : DbContext
    {
        public ExistsCtx(DbContextOptions<ExistsCtx> options) : base(options) { }
    }
}
