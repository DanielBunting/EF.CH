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

    /// <summary>
    /// <c>.LimitBy(key, n).Skip(m)</c> emits a global <c>LIMIT m, …</c> after
    /// the per-group <c>LIMIT n BY key</c>. ClickHouse evaluates LIMIT BY
    /// before the global LIMIT/OFFSET, so the offset applies across the
    /// already-grouped result set — a global skip, NOT a per-group offset.
    /// </summary>
    [Fact]
    public async Task LimitBy_ComposedWithSkip_EmitsGlobalOffset_NotPerGroup()
    {
        await using var ctx = await SeededAsync();

        var sql = ctx.Rows
            .OrderByDescending(r => r.Score)
            .LimitBy(r => r.Group, 2)
            .Skip(1)
            .ToQueryString();

        Assert.Contains("LIMIT 2 BY", sql);
        // Global skip emerges as `LIMIT <skip>, <max>` after the LIMIT BY.
        var byIdx = sql.IndexOf("LIMIT 2 BY", StringComparison.Ordinal);
        var globalIdx = sql.IndexOf("LIMIT ", byIdx + "LIMIT 2 BY".Length, StringComparison.Ordinal);
        Assert.True(globalIdx > byIdx, $"expected global LIMIT after LIMIT BY; SQL: {sql}");
        Assert.Contains("18446744073709551615", sql); // ClickHouse "no upper bound" sentinel.

        // 3 groups × 2 per group = 6 rows; with .Skip(1) globally, expect 5.
        var rows = await ctx.Rows
            .OrderByDescending(r => r.Score)
            .LimitBy(r => r.Group, 2)
            .Skip(1)
            .ToListAsync();
        Assert.Equal(5, rows.Count);
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
