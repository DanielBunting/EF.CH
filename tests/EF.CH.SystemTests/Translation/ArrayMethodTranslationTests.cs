using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Exercises every method/member recognized by <c>ClickHouseArrayMethodTranslator</c> /
/// <c>ClickHouseArrayMemberTranslator</c> over an <c>Array(T)</c> column.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ArrayMethodTranslationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ArrayMethodTranslationTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, Tags = new[] { "alpha", "beta", "gamma" }, Numbers = new[] { 1, 2, 3 } },
            new Row { Id = 2, Tags = Array.Empty<string>(), Numbers = Array.Empty<int>() },
            new Row { Id = 3, Tags = new[] { "alpha" }, Numbers = new[] { 99 } });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ListContains_TranslatesToHas()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.Where(r => r.Tags.Contains("alpha")).Select(r => r.Id).OrderBy(x => x).ToListAsync();
        Assert.Equal(new uint[] { 1, 3 }, ids);
    }

    [Fact]
    public async Task EnumerableAny_NoPredicate_TranslatesToNotEmpty()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.Where(r => r.Tags.Any()).Select(r => r.Id).OrderBy(x => x).ToListAsync();
        Assert.Equal(new uint[] { 1, 3 }, ids);
    }

    [Fact]
    public async Task EnumerableCount_NoPredicate_TranslatesToLength()
    {
        await using var ctx = await SeededAsync();
        var counts = await ctx.Rows.Select(r => new { r.Id, N = r.Tags.Count() }).OrderBy(x => x.Id).ToListAsync();
        Assert.Equal(3, counts[0].N);
        Assert.Equal(0, counts[1].N);
        Assert.Equal(1, counts[2].N);
    }

    [Fact]
    public async Task EnumerableFirst_TranslatesToArrayElement1()
    {
        await using var ctx = await SeededAsync();
        var first = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Tags.First()).FirstAsync();
        Assert.Equal("alpha", first);
    }

    [Fact]
    public async Task EnumerableLast_TranslatesToArrayElementMinus1()
    {
        await using var ctx = await SeededAsync();
        var last = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Tags.Last()).FirstAsync();
        Assert.Equal("gamma", last);
    }

    [Fact]
    public async Task FirstOrDefault_LastOrDefault_DoNotThrowForNonEmpty()
    {
        await using var ctx = await SeededAsync();
        var fod = await ctx.Rows.Where(r => r.Id == 3).Select(r => r.Tags.FirstOrDefault()).FirstAsync();
        var lod = await ctx.Rows.Where(r => r.Id == 3).Select(r => r.Tags.LastOrDefault()).FirstAsync();
        Assert.Equal("alpha", fod);
        Assert.Equal("alpha", lod);
    }

    [Fact]
    public async Task ArrayLength_Member_MapsToLength()
    {
        await using var ctx = await SeededAsync();
        var max = await ctx.Rows.MaxAsync(r => r.Numbers.Length);
        Assert.Equal(3, max);
    }

    [Fact]
    public async Task EnumerableContains_OnIntArray_FiltersExpectedRows()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.Where(r => r.Numbers.Contains(99)).Select(r => r.Id).ToListAsync();
        Assert.Equal(new uint[] { 3 }, ids);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string[] Tags { get; set; } = Array.Empty<string>();
        public int[] Numbers { get; set; } = Array.Empty<int>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ArrayFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
