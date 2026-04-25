using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <c>ClickHouseStringDistanceDbFunctionsExtensions</c> and
/// <c>ClickHouseTextSearchDbFunctionsExtensions</c>. Where possible, the test pins
/// known-result values (Jaro on MARTHA/MARHTA, exact substring count) so a wrong
/// translator mapping fails — bare bound-checks would still pass for any constant
/// value in [0,1] or any positive integer.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class RegexAndStringDistanceDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public RegexAndStringDistanceDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row
        {
            Id = 1,
            A = "kitten",
            B = "sitting",
            Jaro1 = "MARTHA",
            Jaro2 = "MARHTA",
            Doc = "ClickHouse is a fast columnar OLAP database.",
        });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task Levenshtein_Family_KnownDistanceForKittenSitting()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            L = EfClass.Functions.LevenshteinDistance(x.A, x.B),
            Lu = EfClass.Functions.LevenshteinDistanceUTF8(x.A, x.B),
            Dl = EfClass.Functions.DamerauLevenshteinDistance(x.A, x.B),
        }).FirstAsync();
        Assert.Equal(3ul, r.L);
        Assert.Equal(3ul, r.Lu);
        Assert.Equal(3ul, r.Dl);
    }

    [Fact]
    public async Task Jaro_OnMarthaMarhta_MatchesPublishedValue()
    {
        // Canonical pair: Jaro(MARTHA, MARHTA) = 0.9444…  JaroWinkler = 0.9611…
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            J = EfClass.Functions.JaroSimilarity(x.Jaro1, x.Jaro2),
            Jw = EfClass.Functions.JaroWinklerSimilarity(x.Jaro1, x.Jaro2),
        }).FirstAsync();
        Assert.InRange(r.J, 0.943, 0.946);
        Assert.InRange(r.Jw, 0.960, 0.962);
        Assert.True(r.Jw >= r.J - 1e-6, "JaroWinkler ≥ Jaro");
    }

    [Fact]
    public async Task TextSearch_TokenAndMultiSearch()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            HasTok = EfClass.Functions.HasToken(x.Doc, "ClickHouse"),
            HasTokCi = EfClass.Functions.HasTokenCaseInsensitive(x.Doc, "clickhouse"),
            AnyTok = EfClass.Functions.HasAnyToken(x.Doc, new[] { "missing", "fast" }),
            AllTok = EfClass.Functions.HasAllTokens(x.Doc, new[] { "fast", "columnar" }),
            MsAny = EfClass.Functions.MultiSearchAny(x.Doc, new[] { "OLAP", "missing" }),
            MsFirstIdx = EfClass.Functions.MultiSearchFirstIndex(x.Doc, new[] { "missing", "OLAP" }),
        }).FirstAsync();

        Assert.True(r.HasTok);
        Assert.True(r.HasTokCi);
        Assert.True(r.AnyTok);
        Assert.True(r.AllTok);
        Assert.True(r.MsAny);
        // "missing" not present, "OLAP" is needle 2 — multiSearchFirstIndex is 1-based.
        Assert.Equal(2ul, r.MsFirstIdx);
    }

    [Fact]
    public async Task NgramSearch_HitsMoreOnMatchingNeedle_ThanOnNonMatchingNeedle()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            SHit = EfClass.Functions.NgramSearch(x.Doc, "OLAP"),
            SMiss = EfClass.Functions.NgramSearch(x.Doc, "ZZZZZZ"),
            DHit = EfClass.Functions.NgramDistance(x.Doc, "OLAP"),
            DMiss = EfClass.Functions.NgramDistance(x.Doc, "ZZZZZZ"),
        }).FirstAsync();

        Assert.InRange(r.SHit, 0f, 1f);
        Assert.InRange(r.SMiss, 0f, 1f);
        Assert.True(r.SHit > r.SMiss, $"NgramSearch should rank a present needle higher than an absent one (got hit={r.SHit}, miss={r.SMiss})");
        Assert.True(r.DHit < r.DMiss, $"NgramDistance should be smaller for a present needle than an absent one (got hit={r.DHit}, miss={r.DMiss})");
    }

    [Fact]
    public async Task HasSubsequence_AndCountSubstrings_ExactCounts()
    {
        // "ClickHouse is a fast columnar OLAP database." → 6 lowercase 'a's.
        // (a, fast, columnar, then 3 in 'database')
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Sub = EfClass.Functions.HasSubsequence(x.Doc, "Cck"),
            Count = EfClass.Functions.CountSubstrings(x.Doc, "a"),
        }).FirstAsync();
        Assert.True(r.Sub);
        Assert.Equal(6ul, r.Count);
    }

    [Fact]
    public async Task MultiMatchAny_AndExtractAll_ReturnExactWordCount()
    {
        // 7 words in the seeded doc.
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Hit = EfClass.Functions.MultiMatchAny(x.Doc, new[] { ".*OLAP.*", "nope" }),
            Idx = EfClass.Functions.MultiMatchAnyIndex(x.Doc, new[] { "missing", "OLAP" }),
            Words = EfClass.Functions.ExtractAll(x.Doc, "[A-Za-z]+"),
        }).FirstAsync();
        Assert.True(r.Hit);
        Assert.Equal(2ul, r.Idx);
        Assert.Equal(7, r.Words.Length);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string A { get; set; } = "";
        public string B { get; set; } = "";
        public string Jaro1 { get; set; } = "";
        public string Jaro2 { get; set; } = "";
        public string Doc { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("RegexDistFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
