using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Verifies that <c>HasProjection(...).OrderBy(...).ThenBy(...).Build()</c>
/// flows through <c>EnsureCreatedAsync</c> (emitting
/// <c>ALTER TABLE ADD PROJECTION</c>) and that a runtime
/// <c>DatabaseFacade.AddProjectionAsync</c> helper exists.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ProjectionFluentFlowTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public ProjectionFluentFlowTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public async Task HasProjection_ShouldBeAppliedByEnsureCreatedAsync()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var count = await RawClickHouse.ScalarAsync<ulong>(
            _fixture.ConnectionString,
            "SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'Orders' AND name = 'by_customer'");
        Assert.Equal(1UL, count);
    }

    [Fact]
    public async Task AddProjectionAsync_EmitsTheAlterTable()
    {
        await using var ctx = TestContextFactory.Create<BareCtx>(_fixture.ConnectionString);
        await RawClickHouse.ExecuteAsync(_fixture.ConnectionString,
            "DROP TABLE IF EXISTS \"Widgets\"");
        await RawClickHouse.ExecuteAsync(_fixture.ConnectionString, """
            CREATE TABLE "Widgets" (Id Int64, Kind String) ENGINE = MergeTree() ORDER BY Id
            """);

        await ctx.Database.AddProjectionAsync(
            "Widgets",
            "by_kind",
            "SELECT * ORDER BY (\"Kind\", \"Id\")");

        var exists = await RawClickHouse.ScalarAsync<ulong>(
            _fixture.ConnectionString,
            "SELECT count() FROM system.projections WHERE table = 'Widgets' AND name = 'by_kind'");
        Assert.Equal(1UL, exists);
    }

    public class Order
    {
        public long Id { get; set; }
        public DateTime OrderDate { get; set; }
        public string CustomerId { get; set; } = "";
        public decimal Amount { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Order> Orders => Set<Order>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Order>(e =>
            {
                e.ToTable("Orders");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.OrderDate, x.Id });
                e.HasProjection("by_customer")
                    .OrderBy(x => x.CustomerId)
                    .ThenBy(x => x.OrderDate)
                    .Build();
            });
    }

    public sealed class BareCtx(DbContextOptions<BareCtx> o) : DbContext(o);
}
