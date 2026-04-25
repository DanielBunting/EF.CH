using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>UnionAll</c>, <c>UnionDistinct</c>, and the fluent
/// <c>SetOperationBuilder</c> chain (UnionAll/UnionDistinct/Intersect/Except).
/// ClickHouse rejects bare <c>UNION</c>, so the SQL generator must emit explicit
/// <c>UNION ALL</c> / <c>UNION DISTINCT</c> — this is the surface that should
/// loudly regress if that handling breaks.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class SetOperationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public SetOperationTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.A.AddRange(new RowA { Id = 1, V = 10 }, new RowA { Id = 2, V = 20 });
        ctx.B.AddRange(new RowB { Id = 3, V = 20 }, new RowB { Id = 4, V = 30 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task UnionAll_PreservesDuplicates()
    {
        await using var ctx = await SeededAsync();
        var values = await ctx.A.Select(r => r.V).UnionAll(ctx.B.Select(r => r.V)).OrderBy(x => x).ToListAsync();
        Assert.Equal(new[] { 10, 20, 20, 30 }, values);
    }

    [Fact]
    public async Task UnionDistinct_RemovesDuplicates()
    {
        await using var ctx = await SeededAsync();
        var values = await ctx.A.Select(r => r.V).UnionDistinct(ctx.B.Select(r => r.V)).OrderBy(x => x).ToListAsync();
        Assert.Equal(new[] { 10, 20, 30 }, values);
    }

    [Fact]
    public async Task FluentBuilder_ChainsUnionIntersectExcept()
    {
        await using var ctx = await SeededAsync();
        var aValues = ctx.A.Select(r => r.V);
        var bValues = ctx.B.Select(r => r.V);
        var result = await aValues.AsSetOperation()
            .UnionAll(bValues)
            .Build()
            .OrderBy(x => x)
            .ToListAsync();
        Assert.Equal(new[] { 10, 20, 20, 30 }, result);

        var intersect = await aValues.AsSetOperation()
            .Intersect(bValues)
            .Build()
            .ToListAsync();
        Assert.Equal(new[] { 20 }, intersect);

        var except = await aValues.AsSetOperation()
            .Except(bValues)
            .Build()
            .OrderBy(x => x)
            .ToListAsync();
        Assert.Equal(new[] { 10 }, except);
    }

    public sealed class RowA
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }

    public sealed class RowB
    {
        public uint Id { get; set; }
        public int V { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RowA> A => Set<RowA>();
        public DbSet<RowB> B => Set<RowB>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RowA>(e =>
            {
                e.ToTable("SetOpTests_A"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<RowB>(e =>
            {
                e.ToTable("SetOpTests_B"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
        }
    }
}
