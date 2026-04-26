using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Admin;

/// <summary>
/// ClickHouse admin commands surfaced as fluent <c>DatabaseFacade</c>
/// extensions. Tests execute each command end-to-end; commands that aren't
/// supported on the Testcontainers single-node image (e.g. RESTART REPLICAS
/// on a non-replicated setup) still succeed as no-ops.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class SystemAdminCommandTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public SystemAdminCommandTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public async Task FlushLogsAsync_Succeeds()
    {
        await using var ctx = TestContextFactory.Create<BareCtx>(_fixture.ConnectionString);
        await ctx.Database.FlushLogsAsync();
    }

    [Fact]
    public async Task DropMarkCacheAsync_Succeeds()
    {
        await using var ctx = TestContextFactory.Create<BareCtx>(_fixture.ConnectionString);
        await ctx.Database.DropMarkCacheAsync();
    }

    [Fact]
    public async Task ReloadDictionaryAsync_OnMissingDictionary_ThrowsWithClickHouseCode()
    {
        await using var ctx = TestContextFactory.Create<BareCtx>(_fixture.ConnectionString);
        // The method sends the command; the server rejects with UNKNOWN_DICTIONARY
        // for a name it doesn't know — confirming the SQL reached the server in
        // the right shape.
        var ex = await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(
            () => ctx.Database.ReloadDictionaryAsync("this_dictionary_does_not_exist"));
        Assert.Contains("Code:", ex.Message);
    }

    [Fact]
    public async Task SyncReplicaAsync_OnMissingTable_ThrowsWithClickHouseCode()
    {
        await using var ctx = TestContextFactory.Create<BareCtx>(_fixture.ConnectionString);
        var ex = await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(
            () => ctx.Database.SyncReplicaAsync("this_table_does_not_exist"));
        Assert.Contains("Code:", ex.Message);
    }

    [Fact]
    public async Task RestartReplicasAsync_Succeeds()
    {
        await using var ctx = TestContextFactory.Create<BareCtx>(_fixture.ConnectionString);
        await ctx.Database.RestartReplicasAsync();
    }

    public sealed class BareCtx(DbContextOptions<BareCtx> o) : DbContext(o);
}
