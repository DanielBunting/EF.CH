using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>WithRawFilter(rawSqlCondition)</c>. The raw SQL is AND-ed with the
/// LINQ-derived WHERE; we assert it filters correctly and composes with regular Where.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class WithRawFilterTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public WithRawFilterTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // Half the rows carry "foo", half carry "baz" — so an arrayExists(... = 'foo')
        // predicate that *actually* runs should match exactly 10 rows (not all 20).
        for (uint i = 1; i <= 20; i++)
            ctx.Rows.Add(new Row { Id = i, Tags = i % 2 == 1 ? new[] { "foo", "bar" } : new[] { "baz", "qux" } });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task RawFilter_NumericPredicate_AppliesAsExpected()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.WithRawFilter("Id % 5 = 0").OrderBy(r => r.Id).Select(r => r.Id).ToListAsync();
        Assert.Equal(new uint[] { 5, 10, 15, 20 }, ids);
    }

    [Fact]
    public async Task RawFilter_AndedWithLinqWhere_BothApply()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.WithRawFilter("Id % 2 = 0").Where(r => r.Id > 10).Select(r => r.Id).OrderBy(x => x).ToListAsync();
        Assert.Equal(new uint[] { 12, 14, 16, 18, 20 }, ids);
    }

    [Fact]
    public async Task RawFilter_LambdaPredicateOnArrayColumn_TranslatesToClickHouseLambda()
    {
        await using var ctx = await SeededAsync();
        var n = await ctx.Rows.WithRawFilter("arrayExists(x -> x = 'foo', Tags)").CountAsync();
        // Only the 10 odd-Id rows carry the "foo" tag — a no-op raw filter would return 20.
        Assert.Equal(10, n);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string[] Tags { get; set; } = Array.Empty<string>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("WithRawFilterOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
