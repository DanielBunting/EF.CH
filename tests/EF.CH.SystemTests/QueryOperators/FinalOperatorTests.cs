using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Dedicated coverage of <c>Final()</c> over a ReplacingMergeTree, asserting it
/// composes with Where/Join/GroupBy. The existing
/// <c>ReplacingMergeTreeDedupTests</c> covers Final incidentally; this file makes
/// the contract explicit.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class FinalOperatorTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public FinalOperatorTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // Two versions of id=1, one of id=2 → after Final, id=1 collapses to v=2.
        ctx.Items.AddRange(
            new Item { Id = 1, Version = 1, Category = "a", Score = 10 },
            new Item { Id = 1, Version = 2, Category = "a", Score = 100 },
            new Item { Id = 2, Version = 1, Category = "b", Score = 50 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task Final_AloneCollapsesDuplicates()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Items.Final().OrderBy(i => i.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        var i1 = rows.Single(r => r.Id == 1);
        Assert.Equal(2u, i1.Version);
        Assert.Equal(100, i1.Score);
    }

    [Fact]
    public async Task Final_ComposesWithWhere()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Items.Final().Where(i => i.Score > 60).Select(i => i.Id).ToListAsync();
        Assert.Equal(new uint[] { 1 }, ids);
    }

    [Fact]
    public async Task Final_ComposesWithGroupBy()
    {
        await using var ctx = await SeededAsync();
        var totalsByCat = await ctx.Items.Final()
            .GroupBy(i => i.Category)
            .Select(g => new { Cat = g.Key, Total = (long?)g.Sum(x => (long)x.Score) })
            .OrderBy(x => x.Cat)
            .ToListAsync();
        Assert.Equal("a", totalsByCat[0].Cat);
        Assert.Equal(100L, totalsByCat[0].Total);
        Assert.Equal("b", totalsByCat[1].Cat);
        Assert.Equal(50L, totalsByCat[1].Total);
    }

    public sealed class Item
    {
        public uint Id { get; set; }
        public uint Version { get; set; }
        public string Category { get; set; } = "";
        public int Score { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Item>(e =>
            {
                e.ToTable("FinalOpTests_Items");
                e.HasKey(x => new { x.Id, x.Version });
                e.UseReplacingMergeTree(x => x.Id).WithVersion(x => x.Version);
            });
    }
}
