using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Exercises every method recognized by <c>ClickHouseStringMethodTranslator</c> /
/// <c>ClickHouseStringMemberTranslator</c>. Behavioral assertions only — no SQL
/// string matching. Anything that fails here is a translator regression.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class StringFunctionCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public StringFunctionCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, S = "Hello, World!", Pad = "  trim me  " },
            new Row { Id = 2, S = "ClickHouse", Pad = "ok" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task Contains_ConstantPattern_FiltersExpectedRows()
    {
        await using var ctx = await SeededAsync();
        var ids = await ctx.Rows.Where(r => r.S.Contains("World")).Select(r => r.Id).ToListAsync();
        Assert.Equal(new uint[] { 1 }, ids);
    }

    [Fact]
    public async Task Contains_NonConstantPattern_FiltersWithConcat()
    {
        await using var ctx = await SeededAsync();
        var needle = "Click";
        var ids = await ctx.Rows.Where(r => r.S.Contains(needle)).Select(r => r.Id).ToListAsync();
        Assert.Equal(new uint[] { 2 }, ids);
    }

    [Fact]
    public async Task StartsWith_AndEndsWith_AreHonored()
    {
        await using var ctx = await SeededAsync();
        var startsHello = await ctx.Rows.Where(r => r.S.StartsWith("Hello")).CountAsync();
        var endsHouse = await ctx.Rows.Where(r => r.S.EndsWith("House")).CountAsync();
        Assert.Equal(1, startsHello);
        Assert.Equal(1, endsHouse);
    }

    [Fact]
    public async Task ToUpper_ToLower_RoundTripUtf8()
    {
        await using var ctx = await SeededAsync();
        var upper = await ctx.Rows.Where(r => r.Id == 2).Select(r => r.S.ToUpper()).FirstAsync();
        var lower = await ctx.Rows.Where(r => r.Id == 2).Select(r => r.S.ToLower()).FirstAsync();
        Assert.Equal("CLICKHOUSE", upper);
        Assert.Equal("clickhouse", lower);
    }

    [Fact]
    public async Task Trim_TrimStart_TrimEnd()
    {
        await using var ctx = await SeededAsync();
        var t = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Pad.Trim()).FirstAsync();
        var ts = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Pad.TrimStart()).FirstAsync();
        var te = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.Pad.TrimEnd()).FirstAsync();
        Assert.Equal("trim me", t);
        Assert.Equal("trim me  ", ts);
        Assert.Equal("  trim me", te);
    }

    [Fact]
    public async Task Substring_OneArg_AndTwoArg_ReturnExpectedSlices()
    {
        await using var ctx = await SeededAsync();
        // "ClickHouse" → Substring(0,5) = "Click", Substring(5) = "House"
        var prefix = await ctx.Rows.Where(r => r.Id == 2).Select(r => r.S.Substring(0, 5)).FirstAsync();
        var suffix = await ctx.Rows.Where(r => r.Id == 2).Select(r => r.S.Substring(5)).FirstAsync();
        Assert.Equal("Click", prefix);
        Assert.Equal("House", suffix);
    }

    [Fact]
    public async Task Replace_SubstitutesAllOccurrences()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Where(x => x.Id == 1).Select(x => x.S.Replace("l", "L")).FirstAsync();
        Assert.Equal("HeLLo, WorLd!", r);
    }

    [Fact]
    public async Task IndexOf_ReturnsZeroBased()
    {
        await using var ctx = await SeededAsync();
        var idx = await ctx.Rows.Where(r => r.Id == 1).Select(r => r.S.IndexOf("World")).FirstAsync();
        Assert.Equal(7, idx);
    }

    [Fact]
    public async Task IsNullOrEmpty_TrueForNullAndEmpty()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, S = "", Pad = "x" },
            new Row { Id = 2, S = "non-empty", Pad = "y" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        var emptyCount = await ctx.Rows.CountAsync(r => string.IsNullOrEmpty(r.S));
        Assert.Equal(1, emptyCount);
    }

    [Fact]
    public async Task IsNullOrWhiteSpace_TrueForWhitespaceOnly()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(
            new Row { Id = 1, S = "   ", Pad = "x" },
            new Row { Id = 2, S = "hello", Pad = "y" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        var n = await ctx.Rows.CountAsync(r => string.IsNullOrWhiteSpace(r.S));
        Assert.Equal(1, n);
    }

    [Fact]
    public async Task Concat_Two_And_Three_Args()
    {
        await using var ctx = await SeededAsync();
        var two = await ctx.Rows.Where(r => r.Id == 2).Select(r => string.Concat(r.S, "!")).FirstAsync();
        var three = await ctx.Rows.Where(r => r.Id == 2).Select(r => string.Concat(r.S, "-", r.S)).FirstAsync();
        Assert.Equal("ClickHouse!", two);
        Assert.Equal("ClickHouse-ClickHouse", three);
    }

    [Fact]
    public async Task Length_MapsToCharLength()
    {
        await using var ctx = await SeededAsync();
        var len = await ctx.Rows.Where(r => r.Id == 2).Select(r => r.S.Length).FirstAsync();
        Assert.Equal(10, len);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string S { get; set; } = "";
        public string Pad { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("StringFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
