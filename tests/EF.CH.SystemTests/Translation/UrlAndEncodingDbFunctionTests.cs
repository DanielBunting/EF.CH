using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <c>ClickHouseUrlDbFunctionsExtensions</c> and
/// <c>ClickHouseEncodingDbFunctionsExtensions</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class UrlAndEncodingDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public UrlAndEncodingDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row
        {
            Id = 1,
            Url = "https://www.example.co.uk/path/to/page?utm_source=foo&utm_medium=bar",
            Plain = "ClickHouse",
            Encoded = "Hello%20World",
        });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task UrlParts_ExtractDomainPathProtocolAndParameters()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Domain = EfClass.Functions.Domain(x.Url),
            DomainNoWww = EfClass.Functions.DomainWithoutWWW(x.Url),
            Tld = EfClass.Functions.TopLevelDomain(x.Url),
            Protocol = EfClass.Functions.Protocol(x.Url),
            Path = EfClass.Functions.UrlPath(x.Url),
            Utm = EfClass.Functions.ExtractURLParameter(x.Url, "utm_source"),
            Stripped = EfClass.Functions.CutURLParameter(x.Url, "utm_source"),
        }).FirstAsync();

        Assert.Equal("www.example.co.uk", r.Domain);
        Assert.Equal("example.co.uk", r.DomainNoWww);
        Assert.Equal("uk", r.Tld);
        Assert.Equal("https", r.Protocol);
        Assert.Equal("/path/to/page", r.Path);
        Assert.Equal("foo", r.Utm);
        Assert.DoesNotContain("utm_source", r.Stripped);
    }

    [Fact]
    public async Task ExtractAllParameters_ReturnsBothKVStrings()
    {
        await using var ctx = await SeededAsync();
        var arr = await ctx.Rows.Select(x => EfClass.Functions.ExtractURLParameters(x.Url)).FirstAsync();
        Assert.Contains(arr, p => p.StartsWith("utm_source", StringComparison.Ordinal));
        Assert.Contains(arr, p => p.StartsWith("utm_medium", StringComparison.Ordinal));
    }

    [Fact]
    public async Task UrlEncodeDecode_RoundTrip()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Decoded = EfClass.Functions.DecodeURLComponent(x.Encoded),
            Encoded = EfClass.Functions.EncodeURLComponent("Hello World"),
        }).FirstAsync();
        Assert.Equal("Hello World", r.Decoded);
        Assert.Equal("Hello%20World", r.Encoded);
    }

    [Fact]
    public async Task Base64_AndHex_MatchKnownEncodings_AndDecodeBack()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            B64 = EfClass.Functions.Base64Encode(x.Plain),
            B64Back = EfClass.Functions.Base64Decode(EfClass.Functions.Base64Encode(x.Plain)),
            Hex = EfClass.Functions.Hex(x.Plain),
            UnhexBack = EfClass.Functions.Unhex(EfClass.Functions.Hex(x.Plain)),
        }).FirstAsync();

        // Known encodings of "ClickHouse" — wrong codec mapping would still round-trip
        // but produce a different on-the-wire representation.
        Assert.Equal("Q2xpY2tIb3VzZQ==", r.B64);
        Assert.Equal("ClickHouse", r.B64Back);
        Assert.Equal("436C69636B486F757365", r.Hex);
        Assert.Equal("ClickHouse", r.UnhexBack);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Url { get; set; } = "";
        public string Plain { get; set; } = "";
        public string Encoded { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("UrlEncFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
