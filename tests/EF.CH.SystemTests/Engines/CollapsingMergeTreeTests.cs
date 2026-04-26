using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

[Collection(SingleNodeCollection.Name)]
public class CollapsingMergeTreeCancellationTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public CollapsingMergeTreeCancellationTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task SignRows_CancelOnMerge_LeavingNetBalance()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Keyless — use raw SQL inserts.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"Accounts\" (\"AccountId\", \"Balance\", \"Sign\") VALUES (1, 100, 1)");
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"Accounts\" (\"AccountId\", \"Balance\", \"Sign\") VALUES (1, 100, -1), (1, 200, 1)");
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"Accounts\" (\"AccountId\", \"Balance\", \"Sign\") VALUES (2, 50, 1)");

        await RawClickHouse.ExecuteAsync(Conn, "OPTIMIZE TABLE \"Accounts\" FINAL");
        await RawClickHouse.WaitForMutationsAsync(Conn, "Accounts");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT AccountId, toFloat64(Balance) AS Balance FROM \"Accounts\" FINAL ORDER BY AccountId");

        Assert.Equal(2, rows.Count);
        Assert.Equal(1L, Convert.ToInt64(rows[0]["AccountId"]));
        Assert.Equal(200.0, Convert.ToDouble(rows[0]["Balance"]));
        Assert.Equal(2L, Convert.ToInt64(rows[1]["AccountId"]));
        Assert.Equal(50.0, Convert.ToDouble(rows[1]["Balance"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Account> Accounts => Set<Account>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Account>(e =>
            {
                e.ToTable("Accounts"); e.HasNoKey();
                e.UseCollapsingMergeTree("Sign", "AccountId");
            });
    }

    public class Account { public long AccountId { get; set; } public double Balance { get; set; } public sbyte Sign { get; set; } }
}
