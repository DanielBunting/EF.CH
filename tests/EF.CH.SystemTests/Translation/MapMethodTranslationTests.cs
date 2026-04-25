using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Exercises <c>ClickHouseMapMethodTranslator</c> / <c>ClickHouseMapMemberTranslator</c>
/// over a <c>Map(String, Int32)</c> column. ContainsKey, Keys, Values, Count.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MapMethodTranslationTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MapMethodTranslationTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, Attrs = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 } },
            new Row { Id = 2, Attrs = new Dictionary<string, int> { ["a"] = 100 } },
            new Row { Id = 3, Attrs = new Dictionary<string, int>() });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task ContainsKey_TranslatesToMapContains()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.Where(r => r.Attrs.ContainsKey("a")).Select(r => r.Id).OrderBy(x => x).ToListAsync();
        Assert.Equal(new uint[] { 1, 2 }, ids);
    }

    [Fact]
    public async Task Keys_TranslatesToMapKeys_Length3OnTwoEntries()
    {
        await using var ctx = await SeededAsync();
        var keysLen = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Attrs.Keys.Count).FirstAsync();
        Assert.Equal(2, keysLen);
    }

    [Fact]
    public async Task Values_TranslatesToMapValues()
    {
        await using var ctx = await SeededAsync();
        // Materialize the values array via projection.
        var valsCount = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Attrs.Values.Count).FirstAsync();
        Assert.Equal(2, valsCount);
    }

    [Fact]
    public async Task Count_TranslatesToLengthMapKeys()
    {
        await using var ctx = await SeededAsync();
        var counts = await ctx.Rows.Select(r => new { r.Id, N = r.Attrs.Count }).OrderBy(x => x.Id).ToListAsync();
        Assert.Equal(2, counts[0].N);
        Assert.Equal(1, counts[1].N);
        Assert.Equal(0, counts[2].N);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public Dictionary<string, int> Attrs { get; set; } = new();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("MapFnCoverage_Rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Attrs).HasColumnType("Map(String, Int32)");
            });
    }
}
