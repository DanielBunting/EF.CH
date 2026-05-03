using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Round-trip integration tests for the Tier 1–3 functions added in the
/// missing-CH-functions rollout. Each test runs the LINQ expression against
/// a live single-node ClickHouse fixture and asserts the materialised value
/// matches expectations. One test per representative function in each new
/// extension class (we don't enumerate every typed-cast variant — the
/// translation tests already pin the per-method emission).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class Tier1ThroughTier3RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public Tier1ThroughTier3RoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row
        {
            Id = 1,
            Name = "Hello World",
            JsonText = "{\"x\":42,\"y\":\"foo\",\"flag\":true,\"nums\":[1,2,3]}",
            IntVal = 42,
            DoubleVal = 0.5,
            CreatedAt = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc),
            IpV4 = "192.168.1.10",
            IpV6 = "2001:db8::1",
            NumericString = "123",
            BadNumericString = "not-a-number",
        });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    // ---- Tier 1a — MultiIf ----

    [Fact]
    public async Task MultiIf_SelectsBranch_BasedOnPredicate()
    {
        await using var ctx = await SeededAsync();
        var bucket = await ctx.Rows.Select(x => EfClass.Functions.MultiIf(
            x.IntVal > 100, "high",
            x.IntVal > 10, "mid",
            "low")).FirstAsync();
        Assert.Equal("mid", bucket);
    }

    // ---- Tier 1b — String pattern matching ----

    [Fact]
    public async Task ILike_MatchesCaseInsensitively()
    {
        await using var ctx = await SeededAsync();
        var match = await ctx.Rows.Select(x => EfClass.Functions.ILike(x.Name, "%HELLO%")).FirstAsync();
        Assert.True(match);
    }

    [Fact]
    public async Task NotLike_NegatesPattern()
    {
        await using var ctx = await SeededAsync();
        var notMatch = await ctx.Rows.Select(x => EfClass.Functions.NotLike(x.Name, "%xyz%")).FirstAsync();
        Assert.True(notMatch);
    }

    [Fact]
    public async Task Match_RegexAnchoredEnd_Hits()
    {
        await using var ctx = await SeededAsync();
        var hit = await ctx.Rows.Select(x => EfClass.Functions.Match(x.Name, "World$")).FirstAsync();
        Assert.True(hit);
    }

    [Fact]
    public async Task ReplaceRegexAll_ReplacesEveryOccurrence()
    {
        await using var ctx = await SeededAsync();
        var replaced = await ctx.Rows.Select(x => EfClass.Functions.ReplaceRegexAll(x.Name, " ", "_")).FirstAsync();
        Assert.Equal("Hello_World", replaced);
    }

    [Fact]
    public async Task Position_ReturnsOneBasedOffset()
    {
        await using var ctx = await SeededAsync();
        var pos = await ctx.Rows.Select(x => EfClass.Functions.Position(x.Name, "World")).FirstAsync();
        Assert.Equal(7, pos);
    }

    [Fact]
    public async Task Position_ReturnsZeroOnMiss()
    {
        await using var ctx = await SeededAsync();
        var pos = await ctx.Rows.Select(x => EfClass.Functions.Position(x.Name, "xyz")).FirstAsync();
        Assert.Equal(0, pos);
    }

    // ---- Tier 1c — Safe type conversion ----

    [Fact]
    public async Task ToInt32OrNull_ReturnsValue_OnValidNumeric()
    {
        await using var ctx = await SeededAsync();
        var n = await ctx.Rows.Select(x => EfClass.Functions.ToInt32OrNull(x.NumericString)).FirstAsync();
        Assert.Equal(123, n);
    }

    [Fact]
    public async Task ToInt32OrNull_ReturnsNull_OnInvalidInput()
    {
        await using var ctx = await SeededAsync();
        var n = await ctx.Rows.Select(x => EfClass.Functions.ToInt32OrNull(x.BadNumericString)).FirstAsync();
        Assert.Null(n);
    }

    [Fact]
    public async Task ToInt32OrZero_ReturnsZero_OnInvalidInput()
    {
        await using var ctx = await SeededAsync();
        var n = await ctx.Rows.Select(x => EfClass.Functions.ToInt32OrZero(x.BadNumericString)).FirstAsync();
        Assert.Equal(0, n);
    }

    [Fact]
    public async Task ParseDateTimeBestEffortOrNull_ReturnsNull_OnGarbage()
    {
        await using var ctx = await SeededAsync();
        var dt = await ctx.Rows.Select(x => EfClass.Functions.ParseDateTimeBestEffortOrNull(x.BadNumericString)).FirstAsync();
        Assert.Null(dt);
    }

    // ---- Tier 1d — DateTime extras ----

    [Fact]
    public async Task DateTrunc_ToDay_StripsTimeComponent()
    {
        await using var ctx = await SeededAsync();
        var d = await ctx.Rows.Select(x => EfClass.Functions.DateTrunc(ClickHouseIntervalUnit.Day, x.CreatedAt)).FirstAsync();
        Assert.Equal(new DateTime(2025, 6, 15, 0, 0, 0), d);
    }

    [Fact]
    public async Task ToStartOfInterval_RoundsToFifteenMinutes()
    {
        await using var ctx = await SeededAsync();
        var d = await ctx.Rows.Select(x => EfClass.Functions.ToStartOfInterval(x.CreatedAt, 15, ClickHouseIntervalUnit.Minute)).FirstAsync();
        Assert.Equal(new DateTime(2025, 6, 15, 12, 30, 0), d);
    }

    [Fact]
    public async Task Now64_ReturnsRecentTimestamp()
    {
        await using var ctx = await SeededAsync();
        var t = await ctx.Rows.Select(x => EfClass.Functions.Now64(3)).FirstAsync();
        // Should be within a generous window of "right now". Don't pin too tight to avoid CI flakes.
        Assert.True((DateTime.UtcNow - t).Duration() < TimeSpan.FromMinutes(5),
            $"now64 returned {t:O}, expected within ~5min of UtcNow {DateTime.UtcNow:O}");
    }

    // ---- Tier 2a — Bit functions ----

    [Fact]
    public async Task BitCount_CountsSetBits()
    {
        await using var ctx = await SeededAsync();
        // 42 in binary is 101010 → 3 set bits.
        var bits = await ctx.Rows.Select(x => EfClass.Functions.BitCount(x.IntVal)).FirstAsync();
        Assert.Equal(3, bits);
    }

    [Fact]
    public async Task BitTest_ReturnsBitAtPosition()
    {
        await using var ctx = await SeededAsync();
        // 42 = …101010 → bit 1 is set, bit 0 is not.
        var b1 = await ctx.Rows.Select(x => EfClass.Functions.BitTest(x.IntVal, 1)).FirstAsync();
        var b0 = await ctx.Rows.Select(x => EfClass.Functions.BitTest(x.IntVal, 0)).FirstAsync();
        Assert.Equal(1, b1);
        Assert.Equal(0, b0);
    }

    [Fact]
    public async Task BitHammingDistance_CountsDifferingBits()
    {
        await using var ctx = await SeededAsync();
        // 42 (101010) vs 0 (000000) → 3 bits differ.
        var d = await ctx.Rows.Select(x => EfClass.Functions.BitHammingDistance(x.IntVal, 0L)).FirstAsync();
        Assert.Equal(3, d);
    }

    // ---- Tier 2b-step-1 — Array helpers ----

    [Fact]
    public async Task ArrayDistinct_RemovesDuplicates()
    {
        await using var ctx = await SeededAsync();
        var arr = new[] { 1, 2, 2, 3, 3, 3 };
        var distinct = await ctx.Rows.Select(x => EfClass.Functions.ArrayDistinct(arr)).FirstAsync();
        Assert.Equal(new[] { 1, 2, 3 }, distinct);
    }

    [Fact]
    public async Task ArrayConcat_CombinesArrays()
    {
        await using var ctx = await SeededAsync();
        var a = new[] { 1, 2 }; var b = new[] { 3, 4 };
        var combined = await ctx.Rows.Select(x => EfClass.Functions.ArrayConcat(a, b)).FirstAsync();
        Assert.Equal(new[] { 1, 2, 3, 4 }, combined);
    }

    [Fact]
    public async Task ArraySlice_TakesSubrange()
    {
        await using var ctx = await SeededAsync();
        var a = new[] { 10, 20, 30, 40, 50 };
        // CH arraySlice(arr, offset, length) is 1-based; arraySlice([10,20,30,40,50], 2, 3) = [20,30,40].
        var slice = await ctx.Rows.Select(x => EfClass.Functions.ArraySlice(a, 2, 3)).FirstAsync();
        Assert.Equal(new[] { 20, 30, 40 }, slice);
    }

    [Fact]
    public async Task IndexOf_ReturnsOneBasedPosition_OrZeroOnMiss()
    {
        await using var ctx = await SeededAsync();
        var a = new[] { 10, 20, 30 };
        var hit = await ctx.Rows.Select(x => EfClass.Functions.IndexOf(a, 20)).FirstAsync();
        var miss = await ctx.Rows.Select(x => EfClass.Functions.IndexOf(a, 99)).FirstAsync();
        Assert.Equal(2, hit);
        Assert.Equal(0, miss);
    }

    [Fact]
    public async Task ArrayProduct_MultipliesAllElements()
    {
        await using var ctx = await SeededAsync();
        var a = new[] { 1.0, 2.0, 3.0, 4.0 };
        var p = await ctx.Rows.Select(x => EfClass.Functions.ArrayProduct(a)).FirstAsync();
        Assert.Equal(24.0, p);
    }

    // ---- Tier 2c — JSON typed extraction ----

    [Fact]
    public async Task JSONExtractInt_ReadsTopLevelInt()
    {
        await using var ctx = await SeededAsync();
        var n = await ctx.Rows.Select(x => EfClass.Functions.JSONExtractInt(x.JsonText, "x")).FirstAsync();
        Assert.Equal(42, n);
    }

    [Fact]
    public async Task JSONExtractString_ReadsTopLevelString()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.JSONExtractString(x.JsonText, "y")).FirstAsync();
        Assert.Equal("foo", s);
    }

    [Fact]
    public async Task JSONExtractBool_ReadsTopLevelBool()
    {
        await using var ctx = await SeededAsync();
        var b = await ctx.Rows.Select(x => EfClass.Functions.JSONExtractBool(x.JsonText, "flag")).FirstAsync();
        Assert.True(b);
    }

    [Fact]
    public async Task JSONHas_DetectsKeyPresence()
    {
        await using var ctx = await SeededAsync();
        var present = await ctx.Rows.Select(x => EfClass.Functions.JSONHas(x.JsonText, "x")).FirstAsync();
        var missing = await ctx.Rows.Select(x => EfClass.Functions.JSONHas(x.JsonText, "missing")).FirstAsync();
        Assert.True(present);
        Assert.False(missing);
    }

    [Fact]
    public async Task JSONLength_ReturnsArrayLength()
    {
        await using var ctx = await SeededAsync();
        var len = await ctx.Rows.Select(x => EfClass.Functions.JSONLength(x.JsonText, "nums")).FirstAsync();
        Assert.Equal(3, len);
    }

    [Fact]
    public async Task IsValidJSON_DistinguishesValidFromGarbage()
    {
        await using var ctx = await SeededAsync();
        var ok = await ctx.Rows.Select(x => EfClass.Functions.IsValidJSON(x.JsonText)).FirstAsync();
        var bad = await ctx.Rows.Select(x => EfClass.Functions.IsValidJSON(x.Name)).FirstAsync();
        Assert.True(ok);
        Assert.False(bad);
    }

    // ---- Tier 2d — Random ----

    [Fact]
    public async Task RandCanonical_ReturnsValueInUnitInterval()
    {
        await using var ctx = await SeededAsync();
        var r = await ctx.Rows.Select(x => EfClass.Functions.RandCanonical()).FirstAsync();
        Assert.InRange(r, 0.0, 1.0);
    }

    [Fact]
    public async Task RandomString_ReturnsNonEmptyString()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.RandomString(8)).FirstAsync();
        // CH's randomString(n) returns n raw bytes — they aren't guaranteed to
        // be valid UTF-8, and what comes back through the driver is a .NET
        // string that may have lossy decoded codepoints. Asserting non-empty
        // is the strongest portable check; pinning length needs randomFixedString.
        Assert.False(string.IsNullOrEmpty(s));
    }

    // ---- Tier 2e — Server metadata ----

    [Fact]
    public async Task Version_ReturnsNonEmptyServerVersion()
    {
        await using var ctx = await SeededAsync();
        var v = await ctx.Rows.Select(x => EfClass.Functions.Version()).FirstAsync();
        Assert.False(string.IsNullOrWhiteSpace(v));
        // Server version follows N.N.N.N convention; first digit must be present.
        Assert.True(char.IsDigit(v[0]), $"expected version to start with a digit; got '{v}'");
    }

    [Fact]
    public async Task CurrentDatabase_ReturnsDefaultName()
    {
        await using var ctx = await SeededAsync();
        var db = await ctx.Rows.Select(x => EfClass.Functions.CurrentDatabase()).FirstAsync();
        Assert.False(string.IsNullOrWhiteSpace(db));
    }

    [Fact]
    public async Task HostName_ReturnsNonEmpty()
    {
        await using var ctx = await SeededAsync();
        var h = await ctx.Rows.Select(x => EfClass.Functions.HostName()).FirstAsync();
        Assert.False(string.IsNullOrWhiteSpace(h));
    }

    // ---- Tier 3a — Tuples (DotProduct on real vectors) ----

    [Fact]
    public async Task DotProduct_ComputesScalarProduct_ViaSql()
    {
        await using var ctx = await SeededAsync();
        // We don't have a tuple column, so compose the tuples server-side via a
        // SqlQuery. dotProduct((1,2,3), (4,5,6)) = 1*4 + 2*5 + 3*6 = 32.
        var dot = await ctx.Database
            .SqlQueryRaw<double>("SELECT dotProduct((1.0, 2.0, 3.0), (4.0, 5.0, 6.0)) AS Value")
            .FirstAsync();
        Assert.Equal(32.0, dot);
    }

    // ---- Tier 3b — IPv6 ----

    [Fact]
    public async Task IPv6StringToNum_RoundTripsViaIPv6NumToString()
    {
        await using var ctx = await SeededAsync();
        // Round-trip the string through IPv6StringToNum → IPv6NumToString
        // server-side. The intermediate FixedString(16) representation comes
        // back from the driver as a .NET string of raw bytes, which is hard
        // to inspect directly — assert the canonical string round-trips.
        var roundTripped = await ctx.Rows.Select(x => EfClass.Functions.IPv6NumToString(EfClass.Functions.IPv6StringToNum(x.IpV6))).FirstAsync();
        Assert.Equal("2001:db8::1", roundTripped, ignoreCase: true);
    }

    // ---- Tier 3c — UUID v7 helpers ----

    [Fact]
    public async Task DateTimeToUUIDv7_ClientSide_RoundTripsThroughUUIDv7ToDateTime()
    {
        await using var ctx = await SeededAsync();
        // DateTimeToUUIDv7 is .NET-implemented (Guid.CreateVersion7) and binds
        // the resulting Guid as a parameter — no `dateTimeToUUIDv7` server
        // function required. Round-trip through the server-side UUIDv7ToDateTime
        // (which IS in CH 25.6) to confirm the embedded ms timestamp survives.
        var ms = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        var roundTripped = await ctx.Rows.Select(x =>
            EfClass.Functions.UUIDv7ToDateTime(EfClass.Functions.DateTimeToUUIDv7(ms))).FirstAsync();
        // CH server runs in UTC; the driver returns Kind=Unspecified for DateTime.
        // Compare wall-clock components — UUIDv7's 48-bit ms timestamp is more
        // than enough resolution for this seconds-precision input.
        Assert.Equal(ms.Year, roundTripped.Year);
        Assert.Equal(ms.Month, roundTripped.Month);
        Assert.Equal(ms.Day, roundTripped.Day);
        Assert.Equal(ms.Hour, roundTripped.Hour);
        Assert.Equal(ms.Minute, roundTripped.Minute);
        Assert.Equal(ms.Second, roundTripped.Second);
    }

    // ---- Tier 3d — Math specials ----

    [Fact]
    public async Task Pi_AndE_ReturnExpectedConstants()
    {
        await using var ctx = await SeededAsync();
        var pi = await ctx.Rows.Select(x => EfClass.Functions.Pi()).FirstAsync();
        var e = await ctx.Rows.Select(x => EfClass.Functions.E()).FirstAsync();
        Assert.InRange(pi, 3.141592, 3.141593);
        Assert.InRange(e, 2.718281, 2.718282);
    }

    [Fact]
    public async Task Degrees_Radians_RoundTrip()
    {
        await using var ctx = await SeededAsync();
        var d = await ctx.Rows.Select(x => EfClass.Functions.Degrees(EfClass.Functions.Radians(180.0))).FirstAsync();
        Assert.InRange(d, 179.99, 180.01);
    }

    [Fact]
    public async Task Factorial_ProducesExpectedValue()
    {
        await using var ctx = await SeededAsync();
        var f = await ctx.Rows.Select(x => EfClass.Functions.Factorial(5)).FirstAsync();
        Assert.Equal(120, f);
    }

    [Fact]
    public async Task Hypot_ComputesPythagorean()
    {
        await using var ctx = await SeededAsync();
        var h = await ctx.Rows.Select(x => EfClass.Functions.Hypot(3.0, 4.0)).FirstAsync();
        Assert.Equal(5.0, h, precision: 6);
    }

    [Fact]
    public async Task RoundBankers_RoundsToEven()
    {
        await using var ctx = await SeededAsync();
        // 0.5 → 0 (round-half-to-even); 1.5 → 2; 2.5 → 2.
        var a = await ctx.Rows.Select(x => EfClass.Functions.RoundBankers(0.5, 0)).FirstAsync();
        var b = await ctx.Rows.Select(x => EfClass.Functions.RoundBankers(2.5, 0)).FirstAsync();
        Assert.Equal(0.0, a);
        Assert.Equal(2.0, b);
    }

    // ---- Tier 3e — String extras ----

    [Fact]
    public async Task Left_TakesFirstNCharacters()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.Left(x.Name, 5)).FirstAsync();
        Assert.Equal("Hello", s);
    }

    [Fact]
    public async Task Right_TakesLastNCharacters()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.Right(x.Name, 5)).FirstAsync();
        Assert.Equal("World", s);
    }

    [Fact]
    public async Task Repeat_DuplicatesString()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.Repeat("ab", 3)).FirstAsync();
        Assert.Equal("ababab", s);
    }

    [Fact]
    public async Task Reverse_ReversesUnicodeString()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.Reverse("Hello")).FirstAsync();
        Assert.Equal("olleH", s);
    }

    [Fact]
    public async Task InitCap_TitleCasesEachWord()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.InitCap("hello world")).FirstAsync();
        Assert.Equal("Hello World", s);
    }

    [Fact]
    public async Task LeftPad_PadsToTargetLength()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.LeftPad("42", 5, "0")).FirstAsync();
        Assert.Equal("00042", s);
    }

    [Fact]
    public async Task Space_ProducesNSpaces()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x => EfClass.Functions.Space(4)).FirstAsync();
        Assert.Equal("    ", s);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
        public string JsonText { get; set; } = "";
        public int IntVal { get; set; }
        public double DoubleVal { get; set; }
        public DateTime CreatedAt { get; set; }
        public string IpV4 { get; set; } = "";
        public string IpV6 { get; set; } = "";
        public string NumericString { get; set; } = "";
        public string BadNumericString { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("Tier1Through3RoundTrip_Rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
