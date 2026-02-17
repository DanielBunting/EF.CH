using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for ClickHouse utility function SQL generation.
/// These tests verify that LINQ expressions using utility DbFunctions
/// are correctly translated to ClickHouse SQL.
/// </summary>
public class UtilityFunctionTests
{
    #region Null Function Tests

    [Fact]
    public void IfNull_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Value = EfClass.Functions.IfNull(e.NickName, "default") });

        var sql = query.ToQueryString();

        Assert.Contains("ifNull(", sql);
    }

    [Fact]
    public void NullIf_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Value = EfClass.Functions.NullIf(e.Name, "unknown") });

        var sql = query.ToQueryString();

        Assert.Contains("nullIf(", sql);
    }

    [Fact]
    public void AssumeNotNull_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Value = EfClass.Functions.AssumeNotNull(e.NickName) });

        var sql = query.ToQueryString();

        Assert.Contains("assumeNotNull(", sql);
    }

    [Fact]
    public void Coalesce_TwoArgs_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Value = EfClass.Functions.Coalesce(e.NickName, e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("coalesce(", sql);
    }

    [Fact]
    public void Coalesce_ThreeArgs_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Value = EfClass.Functions.Coalesce(e.NickName, e.NickName, e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("coalesce(", sql);
    }

    [Fact]
    public void IsNull_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsNull(e.NickName));

        var sql = query.ToQueryString();

        Assert.Contains("isNull(", sql);
    }

    [Fact]
    public void IsNotNull_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsNotNull(e.NickName));

        var sql = query.ToQueryString();

        Assert.Contains("isNotNull(", sql);
    }

    #endregion

    #region String Distance Function Tests

    [Fact]
    public void LevenshteinDistance_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Dist = EfClass.Functions.LevenshteinDistance(e.Name, "test") });

        var sql = query.ToQueryString();

        Assert.Contains("levenshteinDistance(", sql);
    }

    [Fact]
    public void LevenshteinDistanceUTF8_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Dist = EfClass.Functions.LevenshteinDistanceUTF8(e.Name, "test") });

        var sql = query.ToQueryString();

        Assert.Contains("levenshteinDistanceUTF8(", sql);
    }

    [Fact]
    public void DamerauLevenshteinDistance_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Dist = EfClass.Functions.DamerauLevenshteinDistance(e.Name, "test") });

        var sql = query.ToQueryString();

        Assert.Contains("damerauLevenshteinDistance(", sql);
    }

    [Fact]
    public void JaroSimilarity_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Sim = EfClass.Functions.JaroSimilarity(e.Name, "test") });

        var sql = query.ToQueryString();

        Assert.Contains("jaroSimilarity(", sql);
    }

    [Fact]
    public void JaroWinklerSimilarity_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Sim = EfClass.Functions.JaroWinklerSimilarity(e.Name, "test") });

        var sql = query.ToQueryString();

        Assert.Contains("jaroWinklerSimilarity(", sql);
    }

    [Fact]
    public void JaroWinklerSimilarity_InWhereClause_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.JaroWinklerSimilarity(e.Name, "test") > 0.8);

        var sql = query.ToQueryString();

        Assert.Contains("jaroWinklerSimilarity(", sql);
        Assert.Contains("0.8", sql);
    }

    #endregion

    #region URL Function Tests

    [Fact]
    public void Domain_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, D = EfClass.Functions.Domain(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("domain(", sql);
    }

    [Fact]
    public void DomainWithoutWWW_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, D = EfClass.Functions.DomainWithoutWWW(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("domainWithoutWWW(", sql);
    }

    [Fact]
    public void TopLevelDomain_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Tld = EfClass.Functions.TopLevelDomain(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("topLevelDomain(", sql);
    }

    [Fact]
    public void Protocol_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Proto = EfClass.Functions.Protocol(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("protocol(", sql);
    }

    [Fact]
    public void UrlPath_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, P = EfClass.Functions.UrlPath(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("path(", sql);
    }

    [Fact]
    public void ExtractURLParameter_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Param = EfClass.Functions.ExtractURLParameter(e.Url, "id") });

        var sql = query.ToQueryString();

        Assert.Contains("extractURLParameter(", sql);
    }

    [Fact]
    public void ExtractURLParameters_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Params = EfClass.Functions.ExtractURLParameters(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("extractURLParameters(", sql);
    }

    [Fact]
    public void CutURLParameter_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Cut = EfClass.Functions.CutURLParameter(e.Url, "id") });

        var sql = query.ToQueryString();

        Assert.Contains("cutURLParameter(", sql);
    }

    [Fact]
    public void DecodeURLComponent_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Decoded = EfClass.Functions.DecodeURLComponent(e.Url) });

        var sql = query.ToQueryString();

        Assert.Contains("decodeURLComponent(", sql);
    }

    [Fact]
    public void EncodeURLComponent_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Encoded = EfClass.Functions.EncodeURLComponent(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("encodeURLComponent(", sql);
    }

    #endregion

    #region Hash Function Tests

    [Fact]
    public void CityHash64_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.CityHash64(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("cityHash64(", sql);
    }

    [Fact]
    public void SipHash64_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.SipHash64(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("sipHash64(", sql);
    }

    [Fact]
    public void XxHash64_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.XxHash64(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("xxHash64(", sql);
    }

    [Fact]
    public void MurmurHash3_64_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.MurmurHash3_64(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("murmurHash3_64(", sql);
    }

    [Fact]
    public void FarmHash64_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.FarmHash64(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("farmHash64(", sql);
    }

    [Fact]
    public void Md5_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.Md5(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("hex(MD5(", sql);
    }

    [Fact]
    public void Sha256_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Hash = EfClass.Functions.Sha256(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("hex(SHA256(", sql);
    }

    [Fact]
    public void ConsistentHash_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Bucket = EfClass.Functions.ConsistentHash(EfClass.Functions.CityHash64(e.Name), (uint)10) });

        var sql = query.ToQueryString();

        Assert.Contains("yandexConsistentHash(", sql);
    }

    #endregion

    #region Format Function Tests

    [Fact]
    public void FormatDateTime_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Fmt = EfClass.Functions.FormatDateTime(e.CreatedAt, "%Y-%m-%d") });

        var sql = query.ToQueryString();

        Assert.Contains("formatDateTime(", sql);
    }

    [Fact]
    public void FormatReadableSize_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Size = EfClass.Functions.FormatReadableSize(e.SizeBytes) });

        var sql = query.ToQueryString();

        Assert.Contains("formatReadableSize(", sql);
    }

    [Fact]
    public void FormatReadableDecimalSize_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Size = EfClass.Functions.FormatReadableDecimalSize(e.SizeBytes) });

        var sql = query.ToQueryString();

        Assert.Contains("formatReadableDecimalSize(", sql);
    }

    [Fact]
    public void FormatReadableQuantity_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Qty = EfClass.Functions.FormatReadableQuantity(e.Value) });

        var sql = query.ToQueryString();

        Assert.Contains("formatReadableQuantity(", sql);
    }

    [Fact]
    public void FormatReadableTimeDelta_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Delta = EfClass.Functions.FormatReadableTimeDelta(e.Value) });

        var sql = query.ToQueryString();

        Assert.Contains("formatReadableTimeDelta(", sql);
    }

    [Fact]
    public void ParseDateTime_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Parsed = EfClass.Functions.ParseDateTime(e.Name, "%Y-%m-%d") });

        var sql = query.ToQueryString();

        Assert.Contains("parseDateTime(", sql);
    }

    #endregion

    #region Date Truncation Function Tests

    [Fact]
    public void ToStartOfYear_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfYear(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfYear(", sql);
    }

    [Fact]
    public void ToStartOfQuarter_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfQuarter(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfQuarter(", sql);
    }

    [Fact]
    public void ToStartOfMonth_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfMonth(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfMonth(", sql);
    }

    [Fact]
    public void ToStartOfWeek_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfWeek(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfWeek(", sql);
    }

    [Fact]
    public void ToMonday_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToMonday(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toMonday(", sql);
    }

    [Fact]
    public void ToStartOfDay_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfDay(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfDay(", sql);
    }

    [Fact]
    public void ToStartOfHour_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfHour(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfHour(", sql);
    }

    [Fact]
    public void ToStartOfMinute_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfMinute(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfMinute(", sql);
    }

    [Fact]
    public void ToStartOfFiveMinutes_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfFiveMinutes(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfFiveMinutes(", sql);
    }

    [Fact]
    public void ToStartOfFifteenMinutes_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { Bucket = EfClass.Functions.ToStartOfFifteenMinutes(e.CreatedAt) });

        var sql = query.ToQueryString();

        Assert.Contains("toStartOfFifteenMinutes(", sql);
    }

    [Fact]
    public void DateDiff_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Days = EfClass.Functions.DateDiff("day", e.CreatedAt, DateTime.Now) });

        var sql = query.ToQueryString();

        Assert.Contains("dateDiff(", sql);
    }

    #endregion

    #region IP Address Function Tests

    [Fact]
    public void IPv4NumToString_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Ip = EfClass.Functions.IPv4NumToString(EfClass.Functions.IPv4StringToNum(e.IpAddress)) });

        var sql = query.ToQueryString();

        Assert.Contains("IPv4NumToString(", sql);
    }

    [Fact]
    public void IPv4StringToNum_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Num = EfClass.Functions.IPv4StringToNum(e.IpAddress) });

        var sql = query.ToQueryString();

        Assert.Contains("IPv4StringToNum(", sql);
    }

    [Fact]
    public void IsIPAddressInRange_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsIPAddressInRange(e.IpAddress, "192.168.0.0/16"));

        var sql = query.ToQueryString();

        Assert.Contains("isIPAddressInRange(", sql);
    }

    [Fact]
    public void IsIPv4String_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsIPv4String(e.IpAddress));

        var sql = query.ToQueryString();

        Assert.Contains("isIPv4String(", sql);
    }

    [Fact]
    public void IsIPv6String_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsIPv6String(e.IpAddress));

        var sql = query.ToQueryString();

        Assert.Contains("isIPv6String(", sql);
    }

    #endregion

    #region Encoding Function Tests

    [Fact]
    public void Base64Encode_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Encoded = EfClass.Functions.Base64Encode(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("base64Encode(", sql);
    }

    [Fact]
    public void Base64Decode_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Decoded = EfClass.Functions.Base64Decode(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("base64Decode(", sql);
    }

    [Fact]
    public void Hex_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, H = EfClass.Functions.Hex(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("hex(", sql);
    }

    [Fact]
    public void Unhex_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, U = EfClass.Functions.Unhex(e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("unhex(", sql);
    }

    #endregion

    #region Type Check Function Tests

    [Fact]
    public void IsNaN_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsNaN(e.Value));

        var sql = query.ToQueryString();

        Assert.Contains("isNaN(", sql);
    }

    [Fact]
    public void IsFinite_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsFinite(e.Value));

        var sql = query.ToQueryString();

        Assert.Contains("isFinite(", sql);
    }

    [Fact]
    public void IsInfinite_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Where(e => EfClass.Functions.IsInfinite(e.Value));

        var sql = query.ToQueryString();

        Assert.Contains("isInfinite(", sql);
    }

    #endregion

    #region String Split/Join Function Tests

    [Fact]
    public void SplitByChar_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Parts = EfClass.Functions.SplitByChar(",", e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("splitByChar(", sql);
    }

    [Fact]
    public void SplitByString_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Parts = EfClass.Functions.SplitByString("::", e.Name) });

        var sql = query.ToQueryString();

        Assert.Contains("splitByString(", sql);
    }

    [Fact]
    public void ArrayStringConcat_NoSeparator_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Joined = EfClass.Functions.ArrayStringConcat(e.Tags) });

        var sql = query.ToQueryString();

        Assert.Contains("arrayStringConcat(", sql);
    }

    [Fact]
    public void ArrayStringConcat_WithSeparator_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, Joined = EfClass.Functions.ArrayStringConcat(e.Tags, ", ") });

        var sql = query.ToQueryString();

        Assert.Contains("arrayStringConcat(", sql);
    }

    #endregion

    #region UUID Function Tests

    [Fact]
    public void NewGuidV7_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.Entities
            .Select(e => new { e.Id, V7 = EfClass.Functions.NewGuidV7() });

        var sql = query.ToQueryString();

        Assert.Contains("generateUUIDv7(", sql);
    }

    #endregion

    #region Test Infrastructure

    private static UtilityTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<UtilityTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new UtilityTestContext(options);
    }

    #endregion
}

#region Test Entities

public class UtilityTestEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? NickName { get; set; }
    public string Url { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public long SizeBytes { get; set; }
    public double Value { get; set; }
    public string IpAddress { get; set; } = string.Empty;
    public string[] Tags { get; set; } = [];
}

#endregion

#region Test Context

public class UtilityTestContext : DbContext
{
    public UtilityTestContext(DbContextOptions<UtilityTestContext> options)
        : base(options)
    {
    }

    public DbSet<UtilityTestEntity> Entities => Set<UtilityTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<UtilityTestEntity>(entity =>
        {
            entity.ToTable("utility_test");
            entity.HasKey(e => e.Id);
        });
    }
}

#endregion
