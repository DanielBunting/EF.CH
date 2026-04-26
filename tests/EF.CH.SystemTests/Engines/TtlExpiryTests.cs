using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

[Collection(SingleNodeCollection.Name)]
public class TtlExpiryTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public TtlExpiryTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ExpiredRows_AreRemoved_AfterOptimizeFinal()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.Contains("TTL", await RawClickHouse.EngineFullAsync(Conn, "Events"));

        var now = DateTime.UtcNow;
        ctx.Events.AddRange(
            new Evt { Id = 1, CreatedAt = now.AddDays(-400), Body = "expired-1" },
            new Evt { Id = 2, CreatedAt = now.AddDays(-1),   Body = "fresh-1" },
            new Evt { Id = 3, CreatedAt = now.AddYears(-2),  Body = "expired-2" },
            new Evt { Id = 4, CreatedAt = now,               Body = "fresh-2" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn, "OPTIMIZE TABLE \"Events\" FINAL");
        await RawClickHouse.WaitForMutationsAsync(Conn, "Events");

        var survivors = await RawClickHouse.ColumnAsync<string>(Conn,
            "SELECT Body FROM \"Events\" ORDER BY Id");
        Assert.Equal(new[] { "fresh-1", "fresh-2" }, survivors.ToArray());
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Evt> Events => Set<Evt>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Evt>(e =>
            {
                e.ToTable("Events"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.CreatedAt, x.Id });
                e.HasTtl("toDateTime(CreatedAt) + INTERVAL 90 DAY");
            });
    }

    public class Evt { public long Id { get; set; } public DateTime CreatedAt { get; set; } public string Body { get; set; } = ""; }
}
