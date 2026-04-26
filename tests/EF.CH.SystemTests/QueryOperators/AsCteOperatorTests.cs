using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>AsCte(name)</c>. Asserts both that the rendered SQL contains
/// the <c>WITH "name" AS (...)</c> clause (so a regression to a no-op operator
/// fails loudly) and that the result matches the equivalent inline query.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class AsCteOperatorTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public AsCteOperatorTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 20; i++) ctx.Rows.Add(new Row { Id = i, V = (int)i });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task AsCte_FilteredQuery_RendersWithClause_AndReturnsExpectedRows()
    {
        await using var ctx = await SeededAsync();
        var query = ctx.Rows
            .Where(r => r.V > 15)
            .AsCte("filtered")
            .OrderBy(r => r.Id)
            .Select(r => r.Id);

        var sql = query.ToQueryString();
        Assert.Contains("WITH", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("filtered", sql, StringComparison.Ordinal);

        var ids = await query.ToListAsync();
        Assert.Equal(new uint[] { 16, 17, 18, 19, 20 }, ids);
    }

    [Fact]
    public async Task AsCte_ComposesWithSum_RendersWithClause_AndReturnsCorrectSum()
    {
        await using var ctx = await SeededAsync();
        var query = ctx.Rows
            .Where(r => r.V > 10)
            .AsCte("upper")
            .Select(r => (long?)r.V);

        var sql = query.ToQueryString();
        Assert.Contains("WITH", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("upper", sql, StringComparison.Ordinal);

        var sum = await query.SumAsync();
        Assert.Equal(11L + 12 + 13 + 14 + 15 + 16 + 17 + 18 + 19 + 20, sum);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("AsCteOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
