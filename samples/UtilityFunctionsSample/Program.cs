using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using EfClass = Microsoft.EntityFrameworkCore.EF;

// ============================================================
// Utility Functions Sample
// ============================================================
// Demonstrates ClickHouse utility functions exposed via EF.Functions:
// - Date truncation/bucketing (toStartOfHour, dateDiff, etc.)
// - Null handling (ifNull, coalesce, isNull, etc.)
// - String distance (levenshteinDistance, jaroWinklerSimilarity)
// - URL parsing (domain, protocol, extractURLParameter)
// - Hashing (cityHash64, sipHash64, MD5, SHA256)
// - Formatting (formatDateTime, formatReadableSize)
// - IP address functions (isIPAddressInRange, isIPv4String)
// - Encoding (base64Encode, hex)
// - Type checking (isNaN, isFinite)
// - String splitting/joining (splitByChar, arrayStringConcat)
// - UUID generation (generateUUIDv7)
// ============================================================

Console.WriteLine("Utility Functions Sample");
Console.WriteLine("=======================\n");

await using var context = new AnalyticsDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert sample data
Console.WriteLine("Inserting sample data...\n");

var events = new List<PageView>
{
    new()
    {
        Id = Guid.NewGuid(),
        Url = "https://www.example.com/products?category=electronics&page=2",
        UserName = "alice",
        NickName = null,
        IpAddress = "192.168.1.100",
        ViewedAt = DateTime.UtcNow.AddHours(-3),
        BytesTransferred = 1_073_741_824, // 1 GiB
        Score = 0.95,
        Tags = ["tech", "shopping"]
    },
    new()
    {
        Id = Guid.NewGuid(),
        Url = "https://blog.example.com/posts/hello-world?ref=twitter",
        UserName = "bob",
        NickName = "Bobby",
        IpAddress = "10.0.0.42",
        ViewedAt = DateTime.UtcNow.AddHours(-2),
        BytesTransferred = 524_288,
        Score = 0.72,
        Tags = ["blog", "social"]
    },
    new()
    {
        Id = Guid.NewGuid(),
        Url = "https://www.example.com/api/v2/users?limit=50&offset=100",
        UserName = "charlie",
        NickName = null,
        IpAddress = "203.0.113.5",
        ViewedAt = DateTime.UtcNow.AddMinutes(-30),
        BytesTransferred = 2_147_483_648, // 2 GiB
        Score = 0.88,
        Tags = ["api", "users"]
    },
    new()
    {
        Id = Guid.NewGuid(),
        Url = "https://docs.example.com/guide/getting-started",
        UserName = "alice",
        NickName = "Ali",
        IpAddress = "192.168.1.100",
        ViewedAt = DateTime.UtcNow.AddMinutes(-15),
        BytesTransferred = 10_240,
        Score = double.NaN, // intentionally NaN for type-check demo
        Tags = ["docs", "tutorial"]
    }
};

context.PageViews.AddRange(events);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {events.Count} page views.\n");

// ============================================================
// 1. Date Truncation / Bucketing
// ============================================================
Console.WriteLine("=== 1. Date Truncation ===\n");

var hourlyBuckets = await context.PageViews
    .Select(p => new
    {
        Hour = EfClass.Functions.ToStartOfHour(p.ViewedAt),
        p.UserName
    })
    .ToListAsync();

Console.WriteLine("Page views bucketed by hour:");
foreach (var row in hourlyBuckets)
{
    Console.WriteLine($"  {row.Hour:yyyy-MM-dd HH:mm} — {row.UserName}");
}

// DateDiff
Console.WriteLine();
var ageInMinutes = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        MinutesAgo = EfClass.Functions.DateDiff("minute", p.ViewedAt, DateTime.UtcNow)
    })
    .ToListAsync();

Console.WriteLine("How long ago each page was viewed:");
foreach (var row in ageInMinutes)
{
    Console.WriteLine($"  {row.UserName}: {row.MinutesAgo} minutes ago");
}

// ============================================================
// 2. Null Handling
// ============================================================
Console.WriteLine("\n=== 2. Null Handling ===\n");

var displayNames = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        DisplayName = EfClass.Functions.IfNull(p.NickName, p.UserName),
        HasNick = EfClass.Functions.IsNotNull(p.NickName)
    })
    .ToListAsync();

Console.WriteLine("Display names (NickName ?? UserName):");
foreach (var row in displayNames)
{
    Console.WriteLine($"  {row.UserName} → {row.DisplayName} (has nickname: {row.HasNick})");
}

// Coalesce
Console.WriteLine();
var coalesced = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        Label = EfClass.Functions.Coalesce(p.NickName, p.UserName)
    })
    .ToListAsync();

Console.WriteLine("Coalesce(NickName, UserName):");
foreach (var row in coalesced)
{
    Console.WriteLine($"  {row.UserName} → {row.Label}");
}

// ============================================================
// 3. String Distance
// ============================================================
Console.WriteLine("\n=== 3. String Distance ===\n");

var distances = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        Levenshtein = EfClass.Functions.LevenshteinDistance(p.UserName, "alice"),
        JaroWinkler = EfClass.Functions.JaroWinklerSimilarity(p.UserName, "alice")
    })
    .ToListAsync();

Console.WriteLine("Distance from 'alice':");
foreach (var row in distances)
{
    Console.WriteLine($"  {row.UserName}: levenshtein={row.Levenshtein}, jaro-winkler={row.JaroWinkler:F3}");
}

// ============================================================
// 4. URL Parsing
// ============================================================
Console.WriteLine("\n=== 4. URL Parsing ===\n");

var urlParts = await context.PageViews
    .Select(p => new
    {
        p.Url,
        Domain = EfClass.Functions.DomainWithoutWWW(p.Url),
        Proto = EfClass.Functions.Protocol(p.Url),
        Path = EfClass.Functions.UrlPath(p.Url)
    })
    .ToListAsync();

Console.WriteLine("URL components:");
foreach (var row in urlParts)
{
    Console.WriteLine($"  {row.Proto}://{row.Domain}{row.Path}");
}

// Extract query parameters
Console.WriteLine();
var queryParams = await context.PageViews
    .Select(p => new
    {
        p.Url,
        Params = EfClass.Functions.ExtractURLParameters(p.Url)
    })
    .ToListAsync();

Console.WriteLine("URL query parameters:");
foreach (var row in queryParams)
{
    Console.WriteLine($"  {row.Url}");
    Console.WriteLine($"    params: [{string.Join(", ", row.Params)}]");
}

// ============================================================
// 5. Hashing
// ============================================================
Console.WriteLine("\n=== 5. Hashing ===\n");

var hashes = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        City64 = EfClass.Functions.CityHash64(p.UserName),
        Md5 = EfClass.Functions.Md5(p.UserName)
    })
    .ToListAsync();

Console.WriteLine("Hashes of user names:");
foreach (var row in hashes)
{
    Console.WriteLine($"  {row.UserName}: cityHash64={row.City64}, md5={row.Md5}");
}

// ============================================================
// 6. Formatting
// ============================================================
Console.WriteLine("\n=== 6. Formatting ===\n");

var formatted = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        DateStr = EfClass.Functions.FormatDateTime(p.ViewedAt, "%Y-%m-%d %H:%i"),
        SizeStr = EfClass.Functions.FormatReadableSize(p.BytesTransferred)
    })
    .ToListAsync();

Console.WriteLine("Formatted values:");
foreach (var row in formatted)
{
    Console.WriteLine($"  {row.UserName}: viewed={row.DateStr}, transferred={row.SizeStr}");
}

// ============================================================
// 7. IP Address Functions
// ============================================================
Console.WriteLine("\n=== 7. IP Address Functions ===\n");

var privateIps = await context.PageViews
    .Where(p => EfClass.Functions.IsIPAddressInRange(p.IpAddress, "192.168.0.0/16"))
    .Select(p => new { p.UserName, p.IpAddress })
    .ToListAsync();

Console.WriteLine("Users on 192.168.0.0/16 network:");
foreach (var row in privateIps)
{
    Console.WriteLine($"  {row.UserName}: {row.IpAddress}");
}

Console.WriteLine();
var ipValidation = await context.PageViews
    .Select(p => new
    {
        p.IpAddress,
        IsV4 = EfClass.Functions.IsIPv4String(p.IpAddress),
        IsV6 = EfClass.Functions.IsIPv6String(p.IpAddress)
    })
    .ToListAsync();

Console.WriteLine("IP address validation:");
foreach (var row in ipValidation)
{
    Console.WriteLine($"  {row.IpAddress}: IPv4={row.IsV4}, IPv6={row.IsV6}");
}

// ============================================================
// 8. Encoding
// ============================================================
Console.WriteLine("\n=== 8. Encoding ===\n");

var encoded = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        Base64 = EfClass.Functions.Base64Encode(p.UserName),
        HexVal = EfClass.Functions.Hex(p.UserName)
    })
    .ToListAsync();

Console.WriteLine("Encoded user names:");
foreach (var row in encoded)
{
    Console.WriteLine($"  {row.UserName}: base64={row.Base64}, hex={row.HexVal}");
}

// ============================================================
// 9. Type Checking
// ============================================================
Console.WriteLine("\n=== 9. Type Checking ===\n");

var typeChecks = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        p.Score,
        IsNan = EfClass.Functions.IsNaN(p.Score),
        IsFin = EfClass.Functions.IsFinite(p.Score)
    })
    .ToListAsync();

Console.WriteLine("Score validation:");
foreach (var row in typeChecks)
{
    Console.WriteLine($"  {row.UserName}: score={row.Score}, isNaN={row.IsNan}, isFinite={row.IsFin}");
}

// Filter out invalid scores
var validScores = await context.PageViews
    .Where(p => EfClass.Functions.IsFinite(p.Score))
    .Select(p => new { p.UserName, p.Score })
    .ToListAsync();

Console.WriteLine($"\nValid scores only ({validScores.Count} rows):");
foreach (var row in validScores)
{
    Console.WriteLine($"  {row.UserName}: {row.Score}");
}

// ============================================================
// 10. String Splitting / Joining
// ============================================================
Console.WriteLine("\n=== 10. String Splitting / Joining ===\n");

var tagOps = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        p.Tags,
        Joined = EfClass.Functions.ArrayStringConcat(p.Tags, ", ")
    })
    .ToListAsync();

Console.WriteLine("Tags joined with ', ':");
foreach (var row in tagOps)
{
    Console.WriteLine($"  {row.UserName}: [{row.Joined}]");
}

// Split URL paths
Console.WriteLine();
var pathParts = await context.PageViews
    .Select(p => new
    {
        Path = EfClass.Functions.UrlPath(p.Url),
        Parts = EfClass.Functions.SplitByChar("/", EfClass.Functions.UrlPath(p.Url))
    })
    .ToListAsync();

Console.WriteLine("URL paths split by '/':");
foreach (var row in pathParts)
{
    Console.WriteLine($"  {row.Path} → [{string.Join(", ", row.Parts.Where(s => s.Length > 0))}]");
}

// ============================================================
// 11. UUID v7 Generation
// ============================================================
Console.WriteLine("\n=== 11. UUID v7 Generation ===\n");

var withV7 = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        OriginalId = p.Id,
        NewV7 = EfClass.Functions.NewGuidV7()
    })
    .ToListAsync();

Console.WriteLine("UUIDv7 generated server-side:");
foreach (var row in withV7)
{
    Console.WriteLine($"  {row.UserName}: original={row.OriginalId}, v7={row.NewV7}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definitions
// ============================================================

public class PageView
{
    public Guid Id { get; set; }
    public string Url { get; set; } = string.Empty;
    public string UserName { get; set; } = string.Empty;
    public string? NickName { get; set; }
    public string IpAddress { get; set; } = string.Empty;
    public DateTime ViewedAt { get; set; }
    public long BytesTransferred { get; set; }
    public double Score { get; set; }
    public string[] Tags { get; set; } = [];
}

// ============================================================
// DbContext Definition
// ============================================================

public class AnalyticsDbContext : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=utility_functions_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.ToTable("PageViews");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.ViewedAt, x.Id });
            entity.HasPartitionByMonth(x => x.ViewedAt);
        });
    }
}
