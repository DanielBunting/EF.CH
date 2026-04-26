using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace EF.CH.SystemTests.Migrations;

[Collection(SingleNodeCollection.Name)]
public class MigrationHistoryTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MigrationHistoryTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task HistoryTable_IsCreated_AndTracksAppliedMigrations()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await RawClickHouse.ExecuteAsync(Conn, "DROP TABLE IF EXISTS \"__EFMigrationsHistory\"");

        var historyRepo = ctx.GetService<IHistoryRepository>();
        await RawClickHouse.ExecuteAsync(Conn, historyRepo.GetCreateIfNotExistsScript());

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "__EFMigrationsHistory"));
        Assert.Contains("MergeTree",
            await RawClickHouse.EngineFullAsync(Conn, "__EFMigrationsHistory"));

        await RawClickHouse.ExecuteAsync(Conn, historyRepo.GetInsertScript(
            new HistoryRow("20260101000000_First", "10.0.0")));
        await RawClickHouse.ExecuteAsync(Conn, historyRepo.GetInsertScript(
            new HistoryRow("20260102000000_Second", "10.0.0")));

        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "__EFMigrationsHistory"));

        // Re-running CreateIfNotExists stays a no-op.
        await RawClickHouse.ExecuteAsync(Conn, historyRepo.GetCreateIfNotExistsScript());
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "__EFMigrationsHistory"));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o);
}
