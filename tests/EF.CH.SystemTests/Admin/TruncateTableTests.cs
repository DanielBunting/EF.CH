using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Admin;

/// <summary>
/// <c>TRUNCATE TABLE</c> via the fluent <c>DatabaseFacade.TruncateTableAsync</c>
/// extension — synchronous, atomic part drop, unlike <c>ExecuteDelete</c>
/// which emits an asynchronous <c>ALTER TABLE DELETE</c> mutation.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TruncateTableTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public TruncateTableTests(SingleNodeClickHouseFixture fx) => _fixture = fx;

    [Fact]
    public async Task TruncateTable_Typed_ZerosTheTable()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(Enumerable.Range(1, 100).Select(i => new Row { Id = i, Name = "r" + i }));
        await ctx.SaveChangesAsync();
        Assert.Equal(100UL, await RawClickHouse.RowCountAsync(_fixture.ConnectionString, "Rows"));

        await ctx.Database.TruncateTableAsync<Row>();

        Assert.Equal(0UL, await RawClickHouse.RowCountAsync(_fixture.ConnectionString, "Rows"));
    }

    [Fact]
    public async Task TruncateTable_ByName_ZerosTheTable()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(_fixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(Enumerable.Range(1, 10).Select(i => new Row { Id = i, Name = "n" + i }));
        await ctx.SaveChangesAsync();

        await ctx.Database.TruncateTableAsync("Rows");

        Assert.Equal(0UL, await RawClickHouse.RowCountAsync(_fixture.ConnectionString, "Rows"));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }

    public class Row { public long Id { get; set; } public string Name { get; set; } = ""; }
}
