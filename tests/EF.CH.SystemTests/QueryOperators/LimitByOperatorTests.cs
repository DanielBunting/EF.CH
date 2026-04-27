using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of LIMIT BY: <c>LimitBy(key, limit)</c>.
/// The first returns at most N rows per group; the second pages within each group.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class LimitByOperatorTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public LimitByOperatorTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // 3 groups × 5 rows = 15 rows; scores descending within each group.
        var rows = new List<Row>();
        uint id = 1;
        foreach (var grp in new[] { "alpha", "beta", "gamma" })
            for (int s = 5; s >= 1; s--)
                rows.Add(new Row { Id = id++, Group = grp, Score = s });
        ctx.Rows.AddRange(rows);
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task LimitBy_TopTwoPerGroup_ReturnsAtMostTwoPerKey()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .OrderByDescending(r => r.Score)
            .LimitBy(r => r.Group, 2)
            .ToListAsync();
        var perGroup = rows.GroupBy(r => r.Group).ToDictionary(g => g.Key, g => g.Count());
        Assert.All(perGroup.Values, c => Assert.True(c <= 2, $"expected ≤ 2 per group, got {c}"));
        Assert.Equal(6, rows.Count);
    }

    [Fact(Skip = "Per-group LIMIT offset BY semantics no longer exposed via the public API. " +
                 "The composed .LimitBy(key, limit).Skip(offset) translates to global OFFSET, " +
                 "which is intentional but distinct from the legacy per-group offset.")]
    public async Task LimitBy_WithOffset_SkipsLeadingRowsPerGroup()
    {
        await using var ctx = await SeededAsync();
        var rows = await ctx.Rows
            .OrderByDescending(r => r.Score)
            .LimitBy(r => r.Group, 2).Skip(2)
            .ToListAsync();
        var perGroup = rows.GroupBy(r => r.Group).ToDictionary(g => g.Key, g => g.OrderByDescending(x => x.Score).ToList());
        Assert.All(perGroup.Values, list =>
        {
            Assert.True(list.Count <= 2, $"expected ≤ 2 per group, got {list.Count}");
            // Skipped the top 2 (scores 5,4) — leading score should be ≤ 3 after offset.
            if (list.Count > 0) Assert.True(list[0].Score <= 3, $"expected leading score ≤ 3 after offset, got {list[0].Score}");
        });
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Group { get; set; } = "";
        public int Score { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("LimitByOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
