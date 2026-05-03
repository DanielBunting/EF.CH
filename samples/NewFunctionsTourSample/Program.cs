using EF.CH;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using EfClass = Microsoft.EntityFrameworkCore.EF;

// =====================================================================
// New Functions Tour
// =====================================================================
// Exercises every function category added in the missing-CH-functions
// rollout (Tiers 1–3). Each section runs one or two representative queries
// per new extension class and prints the result.
//
//   Tier 1a — Conditional       (MultiIf)
//   Tier 1b — String patterns   (ILike / Match / ReplaceRegex / Position)
//   Tier 1c — Safe casts        (ToInt32OrNull / ParseDateTimeBestEffortOrNull)
//   Tier 1d — DateTime extras   (DateTrunc / ToStartOfInterval / Now64 / ToTimeZone)
//   Tier 2a — Bit functions     (BitCount / BitTest / BitHammingDistance)
//   Tier 2b — Array helpers     (ArrayDistinct / ArrayConcat / ArraySlice / IndexOf)
//   Tier 2c — JSON extraction   (JSONExtract* / JSONHas / JSONLength)
//   Tier 2d — Random            (RandCanonical / RandUniform)
//   Tier 2e — Server metadata   (Version / HostName / CurrentDatabase)
//   Tier 3a — Tuples            (DotProduct — vector similarity)
//   Tier 3b — IPv6              (IPv6StringToNum / IPv6NumToString)
//   Tier 3c — UUID v7 helpers   (DateTimeToUUIDv7 client-side / UUIDv7ToDateTime)
//   Tier 3d — Math specials     (Pi / Factorial / Hypot / RoundBankers)
//   Tier 3e — String extras     (Left / Right / InitCap / Repeat / LeftPad)
// =====================================================================

Console.WriteLine("New Functions Tour");
Console.WriteLine("==================\n");

await using var ctx = new TourDbContext();
await ctx.Database.EnsureDeletedAsync();
await ctx.Database.EnsureCreatedAsync();

// Seed: rich row that covers numeric / text / json / arrays / IPs.
var seed = new[]
{
    new Event
    {
        Id = Guid.NewGuid(),
        UserName = "alice",
        Message = "Hello, World!",
        Payload = """{"sku":"A-100","qty":3,"available":true,"colours":["red","blue","green"]}""",
        Score = 42,
        Latency = 0.5,
        OccurredAt = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc),
        IpV6 = "2001:db8::1",
        NumericString = "123",
        BadNumericString = "not-a-number",
    },
    new Event
    {
        Id = Guid.NewGuid(),
        UserName = "bob",
        Message = "see you later",
        Payload = """{"sku":"B-220","qty":1,"available":false,"colours":["black"]}""",
        Score = 250,
        Latency = 1.2,
        OccurredAt = new DateTime(2025, 6, 15, 13, 15, 0, DateTimeKind.Utc),
        IpV6 = "fe80::1",
        NumericString = "9999",
        BadNumericString = "?",
    },
};
ctx.Events.AddRange(seed);
await ctx.SaveChangesAsync();

void Header(string title) =>
    Console.WriteLine($"\n=== {title} ===\n");

// ---------------------------------------------------------------------
// Tier 1a — Conditional: bucket users by Score with a single multiIf call
// ---------------------------------------------------------------------
Header("Tier 1a — MultiIf");
var buckets = await ctx.Events
    .Select(e => new
    {
        e.UserName,
        Bucket = EfClass.Functions.MultiIf(
            e.Score >= 200, "high",
            e.Score >= 100, "mid",
            "low")
    }).ToListAsync();
foreach (var r in buckets) Console.WriteLine($"  {r.UserName,-10} → {r.Bucket}");

// ---------------------------------------------------------------------
// Tier 1b — Pattern matching
// ---------------------------------------------------------------------
Header("Tier 1b — ILike / Match / ReplaceRegex / Position");
var patterns = await ctx.Events.Select(e => new
{
    e.UserName,
    HelloMatchCi = EfClass.Functions.ILike(e.Message, "%HELLO%"),
    StartsWithGreeting = EfClass.Functions.Match(e.Message, "^(Hello|Hi|Hey)"),
    Sanitised = EfClass.Functions.ReplaceRegexAll(e.Message, @"[!,.]", ""),
    PosOfWorld = EfClass.Functions.Position(e.Message, "World"),
}).ToListAsync();
foreach (var r in patterns)
    Console.WriteLine($"  {r.UserName,-10} ilike-hello={r.HelloMatchCi}, starts-greeting={r.StartsWithGreeting}, sanitised='{r.Sanitised}', pos(World)={r.PosOfWorld}");

// ---------------------------------------------------------------------
// Tier 1c — Safe type conversion
// ---------------------------------------------------------------------
Header("Tier 1c — ToInt32OrNull / ParseDateTimeBestEffortOrNull");
var safeCasts = await ctx.Events.Select(e => new
{
    e.UserName,
    GoodNumber = EfClass.Functions.ToInt32OrNull(e.NumericString),
    BadNumber = EfClass.Functions.ToInt32OrNull(e.BadNumericString),
    BadDate = EfClass.Functions.ParseDateTimeBestEffortOrNull(e.BadNumericString),
}).ToListAsync();
foreach (var r in safeCasts)
    Console.WriteLine($"  {r.UserName,-10} good='{r.GoodNumber?.ToString() ?? "<null>"}', bad='{r.BadNumber?.ToString() ?? "<null>"}', bad-date='{r.BadDate?.ToString("O") ?? "<null>"}'");

// ---------------------------------------------------------------------
// Tier 1d — DateTime extras
// ---------------------------------------------------------------------
Header("Tier 1d — DateTrunc / ToStartOfInterval / Now64 / ToTimeZone");
var dts = await ctx.Events.Select(e => new
{
    e.UserName,
    DayBucket = EfClass.Functions.DateTrunc(ClickHouseIntervalUnit.Day, e.OccurredAt),
    FifteenMinBucket = EfClass.Functions.ToStartOfInterval(e.OccurredAt, 15, ClickHouseIntervalUnit.Minute),
    InLondon = EfClass.Functions.ToTimeZone(e.OccurredAt, "Europe/London"),
    Now6 = EfClass.Functions.Now64(6),
}).ToListAsync();
foreach (var r in dts)
    Console.WriteLine($"  {r.UserName,-10} day={r.DayBucket:yyyy-MM-dd}, 15-min={r.FifteenMinBucket:HH:mm}, london={r.InLondon:yyyy-MM-dd HH:mm}, now64(μs)={r.Now6:HH:mm:ss.ffffff}");

// ---------------------------------------------------------------------
// Tier 2a — Bit functions
// ---------------------------------------------------------------------
Header("Tier 2a — BitCount / BitTest / BitHammingDistance");
var bits = await ctx.Events.Select(e => new
{
    e.UserName,
    e.Score,
    PopCount = EfClass.Functions.BitCount(e.Score),
    Bit3 = EfClass.Functions.BitTest(e.Score, 3),
    HammingFromZero = EfClass.Functions.BitHammingDistance(e.Score, 0L),
}).ToListAsync();
foreach (var r in bits)
    Console.WriteLine($"  {r.UserName,-10} score={r.Score} bin={Convert.ToString(r.Score, 2)} popcount={r.PopCount} bit3={r.Bit3} hamming={r.HammingFromZero}");

// ---------------------------------------------------------------------
// Tier 2b-step-1 — Non-lambda array helpers
// ---------------------------------------------------------------------
Header("Tier 2b — ArrayDistinct / ArrayConcat / ArraySlice / IndexOf");
var arrA = new[] { 1, 2, 2, 3, 4, 4, 5 };
var arrB = new[] { 6, 7, 8 };
var arr = await ctx.Events.Select(e => new
{
    e.UserName,
    Distinct = EfClass.Functions.ArrayDistinct(arrA),
    Concat = EfClass.Functions.ArrayConcat(arrA, arrB),
    Slice = EfClass.Functions.ArraySlice(arrA, 2, 3),
    PosOf3 = EfClass.Functions.IndexOf(arrA, 3),
    PosOf99 = EfClass.Functions.IndexOf(arrA, 99),
}).FirstAsync();
Console.WriteLine($"  arr={string.Join(",", arrA)} arrB={string.Join(",", arrB)}");
Console.WriteLine($"    distinct = [{string.Join(",", arr.Distinct)}]");
Console.WriteLine($"    concat   = [{string.Join(",", arr.Concat)}]");
Console.WriteLine($"    slice(2,3) = [{string.Join(",", arr.Slice)}]");
Console.WriteLine($"    indexOf(3) = {arr.PosOf3}, indexOf(99) = {arr.PosOf99}");

// ---------------------------------------------------------------------
// Tier 2c — JSON typed extraction
// ---------------------------------------------------------------------
Header("Tier 2c — JSONExtract* / JSONHas / JSONLength / IsValidJSON");
var jsonRows = await ctx.Events.Select(e => new
{
    e.UserName,
    Sku = EfClass.Functions.JSONExtractString(e.Payload, "sku"),
    Qty = EfClass.Functions.JSONExtractInt(e.Payload, "qty"),
    Available = EfClass.Functions.JSONExtractBool(e.Payload, "available"),
    HasSku = EfClass.Functions.JSONHas(e.Payload, "sku"),
    NumColours = EfClass.Functions.JSONLength(e.Payload, "colours"),
    PayloadValid = EfClass.Functions.IsValidJSON(e.Payload),
}).ToListAsync();
foreach (var r in jsonRows)
    Console.WriteLine($"  {r.UserName,-10} sku={r.Sku}, qty={r.Qty}, available={r.Available}, has-sku={r.HasSku}, colours={r.NumColours}, valid={r.PayloadValid}");

// ---------------------------------------------------------------------
// Tier 2d — Random
// ---------------------------------------------------------------------
Header("Tier 2d — RandCanonical / RandUniform");
var rnd = await ctx.Events.Select(e => new
{
    e.UserName,
    Canonical = EfClass.Functions.RandCanonical(),
    Uniform = EfClass.Functions.RandUniform(0.0, 100.0),
}).ToListAsync();
foreach (var r in rnd)
    Console.WriteLine($"  {r.UserName,-10} canonical={r.Canonical:F4}, uniform0-100={r.Uniform:F2}");

// ---------------------------------------------------------------------
// Tier 2e — Server metadata
// ---------------------------------------------------------------------
Header("Tier 2e — Version / HostName / CurrentDatabase");
var meta = await ctx.Events.Select(e => new
{
    Version = EfClass.Functions.Version(),
    HostName = EfClass.Functions.HostName(),
    Database = EfClass.Functions.CurrentDatabase(),
}).FirstAsync();
Console.WriteLine($"  version       : {meta.Version}");
Console.WriteLine($"  hostName      : {meta.HostName}");
Console.WriteLine($"  currentDatabase: {meta.Database}");

// ---------------------------------------------------------------------
// Tier 3a — Tuples (vector similarity via DotProduct)
// ---------------------------------------------------------------------
Header("Tier 3a — DotProduct (vector similarity)");
// DotProduct binds Tuple-typed expressions, which we don't model in this
// schema; demonstrate against literal tuples via raw SQL.
var dot = await ctx.Database
    .SqlQueryRaw<double>("SELECT dotProduct((1.0, 2.0, 3.0), (4.0, 5.0, 6.0)) AS Value")
    .FirstAsync();
Console.WriteLine($"  dotProduct((1,2,3), (4,5,6)) = {dot}");

// ---------------------------------------------------------------------
// Tier 3b — IPv6
// ---------------------------------------------------------------------
Header("Tier 3b — IPv6StringToNum / IPv6NumToString round-trip");
var ip = await ctx.Events.Select(e => new
{
    e.UserName,
    e.IpV6,
    Canonical = EfClass.Functions.IPv6NumToString(EfClass.Functions.IPv6StringToNum(e.IpV6)),
}).ToListAsync();
foreach (var r in ip)
    Console.WriteLine($"  {r.UserName,-10} '{r.IpV6}' → canonical '{r.Canonical}'");

// ---------------------------------------------------------------------
// Tier 3c — UUID v7 helpers (client-side build, server-side decode)
// ---------------------------------------------------------------------
Header("Tier 3c — DateTimeToUUIDv7 (client-side) / UUIDv7ToDateTime (server-side)");
var fixedDt = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc);
var uuid = await ctx.Events.Select(e => new
{
    Original = fixedDt,
    Built = EfClass.Functions.DateTimeToUUIDv7(fixedDt),
    DecodedAfterRebuild = EfClass.Functions.UUIDv7ToDateTime(EfClass.Functions.DateTimeToUUIDv7(fixedDt)),
}).FirstAsync();
Console.WriteLine($"  original={uuid.Original:O}");
Console.WriteLine($"  built UUID v7={uuid.Built}");
Console.WriteLine($"  decoded ms timestamp={uuid.DecodedAfterRebuild:O}");

// ---------------------------------------------------------------------
// Tier 3d — Math specials
// ---------------------------------------------------------------------
Header("Tier 3d — Pi / Factorial / Hypot / RoundBankers");
var math = await ctx.Events.Select(e => new
{
    Pi = EfClass.Functions.Pi(),
    Factorial5 = EfClass.Functions.Factorial(5),
    Pythag = EfClass.Functions.Hypot(3.0, 4.0),
    BankersHalf = EfClass.Functions.RoundBankers(0.5, 0),
    BankersTwoPointFive = EfClass.Functions.RoundBankers(2.5, 0),
}).FirstAsync();
Console.WriteLine($"  pi() = {math.Pi:F6}");
Console.WriteLine($"  factorial(5) = {math.Factorial5}");
Console.WriteLine($"  hypot(3, 4) = {math.Pythag}");
Console.WriteLine($"  roundBankers(0.5) = {math.BankersHalf}, roundBankers(2.5) = {math.BankersTwoPointFive}  ← round-half-to-even");

// ---------------------------------------------------------------------
// Tier 3e — String extras
// ---------------------------------------------------------------------
Header("Tier 3e — Left / Right / InitCap / Repeat / LeftPad");
var strings = await ctx.Events.Select(e => new
{
    e.UserName,
    First5 = EfClass.Functions.Left(e.Message, 5),
    Last5 = EfClass.Functions.Right(e.Message, 5),
    Title = EfClass.Functions.InitCap(e.Message),
    Repeat3 = EfClass.Functions.Repeat("ab", 3),
    Padded = EfClass.Functions.LeftPad(e.UserName, 10, "·"),
}).ToListAsync();
foreach (var r in strings)
    Console.WriteLine($"  {r.UserName,-10} first5='{r.First5}', last5='{r.Last5}', title='{r.Title}', repeat='{r.Repeat3}', padded='{r.Padded}'");

Console.WriteLine("\nDone — all new translators exercised.");

// =====================================================================
// Entity / DbContext
// =====================================================================

public class Event
{
    public Guid Id { get; set; }
    public string UserName { get; set; } = "";
    public string Message { get; set; } = "";
    public string Payload { get; set; } = "";
    public long Score { get; set; }
    public double Latency { get; set; }
    public DateTime OccurredAt { get; set; }
    public string IpV6 { get; set; } = "";
    public string NumericString { get; set; } = "";
    public string BadNumericString { get; set; } = "";
}

public class TourDbContext : DbContext
{
    public DbSet<Event> Events => Set<Event>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        // ClickHouse.Driver speaks HTTP — use port 8123 (HTTP), NOT the
        // native-protocol 9000. Default credentials match the README's
        // suggested Docker invocation (User=clickhouse / Password=clickhouse).
        // Override via CH_CONN env var if your server uses different creds.
        // ClickHouse.Driver expects "Username=" (not "User=") for the user
        // name on the connection string.
        var conn = Environment.GetEnvironmentVariable("CH_CONN")
            ?? "Host=localhost;Port=8123;Username=clickhouse;Password=clickhouse;Database=new_functions_tour";
        options.UseClickHouse(conn);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Event>(entity =>
        {
            entity.ToTable("Events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OccurredAt, x.Id });
            entity.HasPartitionBy(x => x.OccurredAt, PartitionGranularity.Month);
        });
    }
}
