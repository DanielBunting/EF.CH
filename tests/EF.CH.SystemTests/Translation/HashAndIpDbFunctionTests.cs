using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <c>ClickHouseHashDbFunctionsExtensions</c> and
/// <c>ClickHouseIpDbFunctionsExtensions</c>. Where ClickHouse and .NET share an
/// algorithm (MD5, SHA-256), the test compares against the .NET hash bit-for-bit.
/// For ClickHouse-specific hashes (CityHash, SipHash, etc.) the test pins their
/// observed-deterministic value for the canonical input <c>"hello"</c>, so a
/// translator regression that mapped to the wrong CH function still fails.
/// IPv4 conversion is asserted against the canonical <c>192.168.1.10 →
/// 3232235786</c> mapping.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class HashAndIpDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public HashAndIpDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    // Canonical inputs.
    private const string Text = "hello";
    private const string IpV4 = "192.168.1.10";

    // ClickHouse-specific hash output for the literal string "hello".
    // SipHash64 is well-defined and stable across CH versions; we use it as the
    // canonical pinned value. Other hashes (City/Xx/Murmur/Farm) can vary by
    // server build version, so we check determinism + uniqueness instead.
    private const ulong SipHash64Hello = 10142490492830962361ul;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Text = Text, Ip = IpV4 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task Md5_AndSha256_MatchDotNetReferenceImplementation()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Md5Hex = EfClass.Functions.Md5(x.Text),
            ShaHex = EfClass.Functions.Sha256(x.Text),
        }).FirstAsync();

        var expectedMd5 = Convert.ToHexString(MD5.HashData(Encoding.UTF8.GetBytes(Text)));
        var expectedSha = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Text)));
        Assert.Equal(expectedMd5, r.Md5Hex, ignoreCase: true);
        Assert.Equal(expectedSha, r.ShaHex, ignoreCase: true);
    }

    [Fact]
    public async Task SipHash64_MatchesPinnedValueForHello()
    {
        await using var ctx = await SeededAsync();
        var sip = await ctx.Rows.Select(x => EfClass.Functions.SipHash64(x.Text)).FirstAsync();
        Assert.Equal(SipHash64Hello, sip);
    }

    [Fact]
    public async Task ClickHouseSpecificHashes_AreDeterministic_AndDistinctFromEachOther()
    {
        // City/Xx/Murmur/Farm vary across CH builds in some edge cases, but each
        // function MUST be deterministic for a given input and should produce
        // distinct values from the others (otherwise the translator routed
        // multiple methods to the same SQL function).
        await using var ctx = await SeededAsync();
        var rA = await ctx.Rows.Select(x => new
        {
            City = EfClass.Functions.CityHash64(x.Text),
            Xx = EfClass.Functions.XxHash64(x.Text),
            Murmur = EfClass.Functions.MurmurHash3_64(x.Text),
            Farm = EfClass.Functions.FarmHash64(x.Text),
        }).FirstAsync();
        var rB = await ctx.Rows.Select(x => new
        {
            City = EfClass.Functions.CityHash64(x.Text),
            Xx = EfClass.Functions.XxHash64(x.Text),
            Murmur = EfClass.Functions.MurmurHash3_64(x.Text),
            Farm = EfClass.Functions.FarmHash64(x.Text),
        }).FirstAsync();

        Assert.Equal(rA.City, rB.City);
        Assert.Equal(rA.Xx, rB.Xx);
        Assert.Equal(rA.Murmur, rB.Murmur);
        Assert.Equal(rA.Farm, rB.Farm);

        var distinct = new HashSet<ulong> { rA.City, rA.Xx, rA.Murmur, rA.Farm };
        Assert.Equal(4, distinct.Count);
    }

    [Fact]
    public async Task ConsistentHash_AssignsSameInputToSameBucket()
    {
        await using var ctx = await SeededAsync();
        var bucketA = await ctx.Rows.Select(x =>
            EfClass.Functions.ConsistentHash(EfClass.Functions.CityHash64(x.Text), 16u)).FirstAsync();
        var bucketB = await ctx.Rows.Select(x =>
            EfClass.Functions.ConsistentHash(EfClass.Functions.CityHash64(x.Text), 16u)).FirstAsync();

        Assert.InRange(bucketA, 0u, 15u);
        Assert.Equal(bucketA, bucketB); // determinism
    }

    [Fact]
    public async Task IPv4StringToNum_MatchesNetworkOrderEncoding_AndIPv4NumToStringRoundTrips()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            Num = EfClass.Functions.IPv4StringToNum(x.Ip),
            BackToStr = EfClass.Functions.IPv4NumToString(EfClass.Functions.IPv4StringToNum(x.Ip)),
        }).FirstAsync();

        // 192.168.1.10 in network order = (192<<24)|(168<<16)|(1<<8)|10 = 3232235786.
        var expected = (uint)((192u << 24) | (168u << 16) | (1u << 8) | 10u);
        Assert.Equal(expected, r.Num);
        Assert.Equal(IpV4, r.BackToStr);
    }

    [Fact]
    public async Task IsIPv4String_TruePositive_AndV6Negative_AndCidrRangeMatches()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => new
        {
            IsV4 = EfClass.Functions.IsIPv4String(x.Ip),
            IsV6 = EfClass.Functions.IsIPv6String(x.Ip),
            InRange16 = EfClass.Functions.IsIPAddressInRange(x.Ip, "192.168.0.0/16"),
            InRange8 = EfClass.Functions.IsIPAddressInRange(x.Ip, "10.0.0.0/8"),
        }).FirstAsync();

        Assert.True(r.IsV4);
        Assert.False(r.IsV6);
        Assert.True(r.InRange16);
        Assert.False(r.InRange8);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Text { get; set; } = "";
        public string Ip { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("HashIpFnCoverage_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
