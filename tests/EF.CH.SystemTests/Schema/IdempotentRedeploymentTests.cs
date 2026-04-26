using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

[Collection(SingleNodeCollection.Name)]
public class IdempotentRedeploymentTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public IdempotentRedeploymentTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task EnsureCreated_IsIdempotent()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var first = await RawClickHouse.EngineFullAsync(_fixture.ConnectionString, "Widgets");

        await ctx.Database.EnsureCreatedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var second = await RawClickHouse.EngineFullAsync(_fixture.ConnectionString, "Widgets");
        Assert.Equal(first, second);
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Widget> Widgets => Set<Widget>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Widget>(e => { e.ToTable("Widgets"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    public class Widget { public Guid Id { get; set; } public string Name { get; set; } = ""; }
}
