using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Approximate Functions Sample
// ============================================================
// Demonstrates ClickHouse approximate aggregate functions:
// - Count distinct variants (uniqCombined, uniqHLL12, uniqTheta, etc.)
// - Quantile algorithm variants (quantileTDigest, quantileExact, etc.)
// - Multi-quantile functions (quantiles, quantilesTDigest)
// - Weighted top-K (topKWeighted)
//
// These functions trade accuracy for speed, making them ideal for
// large-scale analytics where exact results are not required.
// ============================================================

Console.WriteLine("Approximate Functions Sample");
Console.WriteLine("============================\n");

await using var context = new AnalyticsDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureDeletedAsync();
await context.Database.EnsureCreatedAsync();

// Insert test data
Console.WriteLine("Inserting 10,000 page view events...\n");

var random = new Random(42);
var pages = new[] { "/home", "/products", "/checkout", "/about", "/blog", "/contact", "/pricing", "/docs" };
var regions = new[] { "US", "EU", "APAC" };
var now = DateTime.UtcNow;

var events = Enumerable.Range(0, 10000)
    .Select(i => new PageViewEvent
    {
        Id = Guid.NewGuid(),
        Timestamp = now.AddMinutes(-random.Next(0, 60 * 24 * 7)),
        UserId = (ulong)random.Next(1, 501),  // ~500 unique users
        Page = pages[random.Next(pages.Length)],
        Region = regions[random.Next(regions.Length)],
        LoadTimeMs = Math.Round(50 + random.NextDouble() * 950, 1),  // 50-1000ms
        ViewDurationSec = random.Next(1, 301),  // 1-300 seconds
    })
    .ToList();

context.PageViews.AddRange(events);
await context.SaveChangesAsync();

// ============================================================
// 1. Approximate Count Distinct Variants
// ============================================================
Console.WriteLine("--- 1. Approximate Count Distinct ---\n");

// Compare different algorithms for counting unique users
var countResults = await context.Database
    .SqlQueryRaw<CountDistinctResult>(@"
        SELECT
            uniq(UserId) AS Uniq,
            uniqCombined(UserId) AS UniqCombined,
            uniqCombined64(UserId) AS UniqCombined64,
            uniqHLL12(UserId) AS UniqHLL12,
            uniqTheta(UserId) AS UniqTheta,
            uniqExact(UserId) AS UniqExact
        FROM page_views")
    .ToListAsync();

var cr = countResults[0];
Console.WriteLine($"  Algorithm         | Unique Users");
Console.WriteLine($"  ------------------|--------------");
Console.WriteLine($"  uniq              | {cr.Uniq,12}");
Console.WriteLine($"  uniqCombined      | {cr.UniqCombined,12}");
Console.WriteLine($"  uniqCombined64    | {cr.UniqCombined64,12}");
Console.WriteLine($"  uniqHLL12         | {cr.UniqHLL12,12}");
Console.WriteLine($"  uniqTheta         | {cr.UniqTheta,12}");
Console.WriteLine($"  uniqExact (exact) | {cr.UniqExact,12}");
Console.WriteLine();

// ============================================================
// 2. Quantile Algorithm Variants
// ============================================================
Console.WriteLine("--- 2. Quantile Algorithm Variants (P95 Load Time) ---\n");

var quantileResults = await context.Database
    .SqlQueryRaw<QuantileVariantResult>(@"
        SELECT
            toFloat64(quantile(0.95)(LoadTimeMs)) AS Quantile,
            toFloat64(quantileTDigest(0.95)(LoadTimeMs)) AS QuantileTDigest,
            toFloat64(quantileDD(0.01, 0.95)(LoadTimeMs)) AS QuantileDD,
            toFloat64(quantileExact(0.95)(LoadTimeMs)) AS QuantileExact,
            toFloat64(quantileTiming(0.95)(LoadTimeMs)) AS QuantileTiming
        FROM page_views")
    .ToListAsync();

var qr = quantileResults[0];
Console.WriteLine($"  Algorithm         | P95 Load Time (ms)");
Console.WriteLine($"  ------------------|--------------------");
Console.WriteLine($"  quantile          | {qr.Quantile,18:F1}");
Console.WriteLine($"  quantileTDigest   | {qr.QuantileTDigest,18:F1}");
Console.WriteLine($"  quantileDD        | {qr.QuantileDD,18:F1}");
Console.WriteLine($"  quantileExact     | {qr.QuantileExact,18:F1}");
Console.WriteLine($"  quantileTiming    | {qr.QuantileTiming,18:F1}");
Console.WriteLine();

// ============================================================
// 3. Multi-Quantile (multiple percentiles in one pass)
// ============================================================
Console.WriteLine("--- 3. Multi-Quantile (Load Time Distribution) ---\n");

var multiQuantileResults = await context.Database
    .SqlQueryRaw<MultiQuantileResult>(@"
        SELECT
            Region,
            arrayMap(x -> toFloat64(x), quantiles(0.5, 0.9, 0.95, 0.99)(LoadTimeMs)) AS Percentiles
        FROM page_views
        GROUP BY Region
        ORDER BY Region")
    .ToListAsync();

Console.WriteLine($"  Region | P50 (ms)  | P90 (ms)  | P95 (ms)  | P99 (ms)");
Console.WriteLine($"  -------|-----------|-----------|-----------|-----------");
foreach (var r in multiQuantileResults)
{
    Console.WriteLine($"  {r.Region,-6} | {r.Percentiles[0],9:F1} | {r.Percentiles[1],9:F1} | {r.Percentiles[2],9:F1} | {r.Percentiles[3],9:F1}");
}
Console.WriteLine();

// ============================================================
// 4. Weighted Top-K
// ============================================================
Console.WriteLine("--- 4. Top Pages (by view count vs. weighted by duration) ---\n");

var topKResults = await context.Database
    .SqlQueryRaw<TopKComparisonResult>(@"
        SELECT
            topK(5)(Page) AS TopByCount,
            topKWeighted(5)(Page, ViewDurationSec) AS TopByDuration
        FROM page_views")
    .ToListAsync();

var tk = topKResults[0];
Console.WriteLine($"  Rank | By View Count        | By Total Duration (weighted)");
Console.WriteLine($"  -----|----------------------|-----------------------------");
for (int i = 0; i < Math.Max(tk.TopByCount.Length, tk.TopByDuration.Length); i++)
{
    var byCount = i < tk.TopByCount.Length ? tk.TopByCount[i] : "-";
    var byDuration = i < tk.TopByDuration.Length ? tk.TopByDuration[i] : "-";
    Console.WriteLine($"  {i + 1,4} | {byCount,-20} | {byDuration}");
}
Console.WriteLine();

// ============================================================
// 5. Per-Region Breakdown with Approximate Functions
// ============================================================
Console.WriteLine("--- 5. Per-Region Summary ---\n");

var regionResults = await context.Database
    .SqlQueryRaw<RegionSummaryResult>(@"
        SELECT
            Region,
            count() AS TotalViews,
            uniqCombined(UserId) AS UniqueUsers,
            toFloat64(quantileTDigest(0.5)(LoadTimeMs)) AS MedianLoadTime,
            toFloat64(quantileTDigest(0.95)(LoadTimeMs)) AS P95LoadTime
        FROM page_views
        GROUP BY Region
        ORDER BY Region")
    .ToListAsync();

Console.WriteLine($"  Region | Views  | Unique Users | Median (ms) | P95 (ms)");
Console.WriteLine($"  -------|--------|--------------|-------------|----------");
foreach (var r in regionResults)
{
    Console.WriteLine($"  {r.Region,-6} | {r.TotalViews,6} | {r.UniqueUsers,12} | {r.MedianLoadTime,11:F1} | {r.P95LoadTime,8:F1}");
}

Console.WriteLine("\nDone!");

// ============================================================
// Entity Definition
// ============================================================

public class PageViewEvent
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public ulong UserId { get; set; }
    public string Page { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public double LoadTimeMs { get; set; }
    public int ViewDurationSec { get; set; }
}

// ============================================================
// Result Types
// ============================================================

public class CountDistinctResult
{
    public ulong Uniq { get; set; }
    public ulong UniqCombined { get; set; }
    public ulong UniqCombined64 { get; set; }
    public ulong UniqHLL12 { get; set; }
    public ulong UniqTheta { get; set; }
    public ulong UniqExact { get; set; }
}

public class QuantileVariantResult
{
    public double Quantile { get; set; }
    public double QuantileTDigest { get; set; }
    public double QuantileDD { get; set; }
    public double QuantileExact { get; set; }
    public double QuantileTiming { get; set; }
}

public class MultiQuantileResult
{
    public string Region { get; set; } = string.Empty;
    public double[] Percentiles { get; set; } = [];
}

public class TopKComparisonResult
{
    public string[] TopByCount { get; set; } = [];
    public string[] TopByDuration { get; set; } = [];
}

public class RegionSummaryResult
{
    public string Region { get; set; } = string.Empty;
    public ulong TotalViews { get; set; }
    public ulong UniqueUsers { get; set; }
    public double MedianLoadTime { get; set; }
    public double P95LoadTime { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class AnalyticsDbContext : DbContext
{
    public DbSet<PageViewEvent> PageViews => Set<PageViewEvent>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Password=default;Database=approx_functions_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageViewEvent>(entity =>
        {
            entity.ToTable("page_views");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
        });
    }
}
