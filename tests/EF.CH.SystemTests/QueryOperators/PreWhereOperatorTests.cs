using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>PreWhere(predicate)</c> — alone and in combination with a regular
/// Where. We assert the rendered SQL contains a literal <c>PREWHERE</c> clause
/// (otherwise a regression that silently degrades PreWhere to plain WHERE would
/// still pass the result-count assertions) plus the row-count expectation.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class PreWhereOperatorTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public PreWhereOperatorTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 100; i++)
            ctx.Rows.Add(new Row { Id = i, Score = (int)i, Active = i % 2 == 0 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task PreWhere_Alone_RendersPrewhereClause_AndAppliesPredicate()
    {
        await using var ctx = await SeededAsync();
        var query = ctx.Rows.PreWhere(r => r.Score > 80);

        var sql = query.ToQueryString();
        Assert.Contains("PREWHERE", sql, StringComparison.OrdinalIgnoreCase);

        var n = await query.CountAsync();
        Assert.Equal(20, n);
    }

    [Fact]
    public async Task PreWhere_Combined_With_Where_RendersBothClauses_AndBothPredicatesApply()
    {
        await using var ctx = await SeededAsync();
        var query = ctx.Rows.PreWhere(r => r.Score > 80).Where(r => r.Active);

        var sql = query.ToQueryString();
        Assert.Contains("PREWHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);

        var n = await query.CountAsync();
        Assert.Equal(10, n);
    }

    [Fact]
    public async Task PreWhere_OnSelector_RendersPrewhere_AndReturnsCorrectColumnValues()
    {
        await using var ctx = await SeededAsync();
        var query = ctx.Rows.PreWhere(r => r.Score < 50).Select(r => r.Score);

        var sql = query.ToQueryString();
        Assert.Contains("PREWHERE", sql, StringComparison.OrdinalIgnoreCase);

        var max = await query.MaxAsync();
        Assert.Equal(49, max);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int Score { get; set; }
        public bool Active { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("PreWhereOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
