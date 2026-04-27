// IfCombinatorSample - Demonstrates every ClickHouse -If aggregate combinator.
//
// Scenario: an e-commerce order stream where each row carries a Status
// ("placed", "paid", "fulfilled", "cancelled"). One materialized view slices
// the stream with -If combinators so we get paid revenue, cancelled counts,
// per-status unique customer cardinality, quantile latency, and more in a
// single pass per region.
//
// The -If form (sumIf, countIf, quantileTDigestIf, etc.) is the idiomatic
// ClickHouse way to aggregate filtered slices; it evaluates the predicate
// inline without a separate WHERE pass, which matters at 10s of millions
// of rows.
//
// NOTE: These aggregates flow through the projection / materialized-view
// translator path. They are not yet recognized by EF Core's runtime LINQ
// pipeline for direct queries like `ctx.Orders.GroupBy().Select(g => g.SumIf(...))`.

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:25.6")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.\n");

try
{
    var connectionString = container.GetConnectionString();

    Console.WriteLine("=== EF.CH -If Combinator Sample ===\n");

    await DemoIfCombinators(connectionString);

    Console.WriteLine("=== Done ===");
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
}

// ---------------------------------------------------------------------------
// Demo: insert orders, let the MV roll them up via -If combinators, read back.
// ---------------------------------------------------------------------------
static async Task DemoIfCombinators(string connectionString)
{
    await using var context = new IfContext(connectionString);
    await context.Database.EnsureDeletedAsync();
    await context.Database.EnsureCreatedAsync();

    // Seed: 40 orders across 2 regions, various statuses, varying amounts and
    // fulfilment times. Includes some cancelled and errored rows so the -If
    // combinators have both "in" and "out" rows to filter.
    var rng = new Random(42);
    var regions = new[] { "us-east", "eu-west" };
    var statuses = new[] { "paid", "paid", "paid", "fulfilled", "cancelled", "error" };
    var products = new[] { "widget", "gadget", "gizmo", "doodad", "thingamajig" };
    var baseTime = new DateTime(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc);

    var orders = Enumerable.Range(0, 200).Select(i => new Order
    {
        OrderId = Guid.NewGuid(),
        Region = regions[rng.Next(regions.Length)],
        Status = statuses[rng.Next(statuses.Length)],
        CustomerId = (ulong)rng.Next(1, 41),
        ProductName = products[rng.Next(products.Length)],
        Amount = Math.Round(10 + rng.NextDouble() * 490, 2),
        Weight = rng.Next(1, 10),
        FulfillTimeMs = Math.Round(50 + rng.NextDouble() * 4950, 1),
        CreatedAt = baseTime.AddMinutes(i * 3)
    }).ToList();

    await context.BulkInsertAsync(orders);
    Console.WriteLine($"Inserted {orders.Count} orders.");

    // The MV populates asynchronously; give it a moment and force a merge so
    // per-region rows have collapsed into one summary row each.
    await Task.Delay(500);
    await context.Database.OptimizeTableAsync("region_summary", o => o.WithFinal());

    // ------- Show the SQL the MV was defined with -------
    Console.WriteLine("\nGenerated MV SELECT clause (excerpt):\n");
    var ddl = context.Database.GenerateCreateScript();
    foreach (var line in ddl.Split('\n'))
    {
        var t = line.TrimEnd();
        if (t.Contains("If(", StringComparison.Ordinal) || t.Contains("Sum(", StringComparison.Ordinal))
        {
            Console.WriteLine("  " + t.Trim());
        }
    }

    // ------- Read back the aggregated summaries -------
    // The MV's target columns are all Float64/UInt64/String/Array(…) so the entity's
    // CLR types map cleanly through the LINQ + Final() path — no raw-SQL cast shim needed.
    var summaries = await context.RegionSummaries
        .Final()
        .OrderBy(r => r.Region)
        .ToListAsync();

    Console.WriteLine("\nPer-region summaries (populated by the -If materialized view):\n");
    foreach (var s in summaries)
    {
        Console.WriteLine($"Region: {s.Region}");
        Console.WriteLine($"  OrderCount                 : {s.OrderCount}");
        Console.WriteLine($"  PaidRevenue (sumIf)        : {s.PaidRevenue:F2}");
        Console.WriteLine($"  CancelledCount (countIf)   : {s.CancelledCount}");
        Console.WriteLine($"  PaidCustomers (uniqIf)     : {s.PaidCustomers}");
        Console.WriteLine($"  PaidCustomers approx       : {s.PaidCustomersApprox}  (uniqCombinedIf)");
        Console.WriteLine($"  Latest fulfilled amount    : {s.LatestFulfilledAmount:F2}  (argMaxIf)");
        Console.WriteLine($"  Earliest fulfilled amount  : {s.EarliestFulfilledAmount:F2}  (argMinIf)");
        Console.WriteLine($"  Top paid products          : [{string.Join(", ", s.TopPaidProducts)}]  (topKIf)");
        Console.WriteLine($"  Top weighted paid products : [{string.Join(", ", s.TopWeightedPaidProducts)}]  (topKWeightedIf)");
        Console.WriteLine($"  Cancelled order ids (≤5)   : {s.CancelledOrderIds.Length} ids  (groupArrayIf)");
        Console.WriteLine($"  Unique paid products       : [{string.Join(", ", s.UniqPaidProducts)}]  (groupUniqArrayIf)");
        Console.WriteLine($"  Median paid amount         : {s.MedianPaidAmount:F2}  (medianIf)");
        Console.WriteLine($"  Stddev paid amount         : {s.StddevPaidAmount:F2}  (stddevPopIf)");
        Console.WriteLine($"  Var paid amount            : {s.VarPaidAmount:F2}  (varPopIf)");
        Console.WriteLine($"  P95 fulfill (exact)        : {s.P95FulfillTimeExact:F1} ms  (quantileExactIf)");
        Console.WriteLine($"  P95 fulfill (tDigest)      : {s.P95FulfillTimeTDigest:F1} ms  (quantileTDigestIf)");
        Console.WriteLine($"  P95 fulfill (timing)       : {s.P95FulfillTimeTiming:F1} ms  (quantileTimingIf)");
        Console.WriteLine($"  P95 fulfill (DD)           : {s.P95FulfillTimeDD:F1} ms  (quantileDDIf)");
        Console.WriteLine($"  Fulfill P50/P90/P99        : [{string.Join(", ", s.FulfillTimePercentiles.Select(p => p.ToString("F1")))}]  (quantilesIf)");
        Console.WriteLine($"  Fulfill P50/P90/P99 tDigest: [{string.Join(", ", s.FulfillTimePercentilesTDigest.Select(p => p.ToString("F1")))}]  (quantilesTDigestIf)");
        Console.WriteLine();
    }

    await context.Database.EnsureDeletedAsync();
}

// ---------------------------------------------------------------------------
// Entities
// ---------------------------------------------------------------------------

public class Order
{
    public Guid OrderId { get; set; }
    public string Region { get; set; } = "";
    public string Status { get; set; } = "";
    public ulong CustomerId { get; set; }
    public string ProductName { get; set; } = "";
    public double Amount { get; set; }
    public int Weight { get; set; }
    public double FulfillTimeMs { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>Target table for the -If materialized view.</summary>
public class RegionSummary
{
    public string Region { get; set; } = "";
    public ulong OrderCount { get; set; }
    public double PaidRevenue { get; set; }
    public ulong CancelledCount { get; set; }
    public ulong PaidCustomers { get; set; }
    public ulong PaidCustomersApprox { get; set; }
    public double LatestFulfilledAmount { get; set; }
    public double EarliestFulfilledAmount { get; set; }
    public string[] TopPaidProducts { get; set; } = [];
    public string[] TopWeightedPaidProducts { get; set; } = [];
    public Guid[] CancelledOrderIds { get; set; } = [];
    public string[] UniqPaidProducts { get; set; } = [];
    public double MedianPaidAmount { get; set; }
    public double StddevPaidAmount { get; set; }
    public double VarPaidAmount { get; set; }
    public double P95FulfillTimeExact { get; set; }
    public double P95FulfillTimeTDigest { get; set; }
    public double P95FulfillTimeTiming { get; set; }
    public double P95FulfillTimeDD { get; set; }
    public double[] FulfillTimePercentiles { get; set; } = [];
    public double[] FulfillTimePercentilesTDigest { get; set; } = [];
}

/// <summary>Shape used to deserialize the raw SqlQueryRaw result.</summary>
public class RegionSummaryResult
{
    public string Region { get; set; } = "";
    public ulong OrderCount { get; set; }
    public double PaidRevenue { get; set; }
    public ulong CancelledCount { get; set; }
    public ulong PaidCustomers { get; set; }
    public ulong PaidCustomersApprox { get; set; }
    public double LatestFulfilledAmount { get; set; }
    public double EarliestFulfilledAmount { get; set; }
    public string[] TopPaidProducts { get; set; } = [];
    public string[] TopWeightedPaidProducts { get; set; } = [];
    public Guid[] CancelledOrderIds { get; set; } = [];
    public string[] UniqPaidProducts { get; set; } = [];
    public double MedianPaidAmount { get; set; }
    public double StddevPaidAmount { get; set; }
    public double VarPaidAmount { get; set; }
    public double P95FulfillTimeExact { get; set; }
    public double P95FulfillTimeTDigest { get; set; }
    public double P95FulfillTimeTiming { get; set; }
    public double P95FulfillTimeDD { get; set; }
    public double[] FulfillTimePercentiles { get; set; } = [];
    public double[] FulfillTimePercentilesTDigest { get; set; } = [];
}

// ---------------------------------------------------------------------------
// DbContext
// ---------------------------------------------------------------------------

public class IfContext(string connectionString) : DbContext
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<RegionSummary> RegionSummaries => Set<RegionSummary>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse(connectionString);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.OrderId);
            entity.UseMergeTree(x => new { x.Region, x.OrderId });
        });

        modelBuilder.Entity<RegionSummary>(entity =>
        {
            entity.ToTable("region_summary");
            entity.HasNoKey();
            entity.UseReplacingMergeTree(x => new { x.Region });

            // Demonstrate every new -If combinator in a single MV definition.
            // Each selector's predicate is evaluated inline by ClickHouse in the
            // same GROUP BY pass — no separate WHERE or filter step per column.

        });
        modelBuilder.MaterializedView<RegionSummary>().From<Order>().DefinedAs(orders => orders
                    .GroupBy(o => o.Region)
                    .Select(g => new RegionSummary
                    {
                        Region = g.Key,

                        // Baseline: plain aggregates for comparison.
                        OrderCount = (ulong)g.Count(),

                        // Core -If combinators
                        PaidRevenue = ClickHouseAggregates.SumIf(g, o => o.Amount, o => o.Status == "paid"),
                        CancelledCount = (ulong)ClickHouseAggregates.CountIf(g, o => o.Status == "cancelled"),
                        PaidCustomers = ClickHouseAggregates.UniqIf(g, o => o.CustomerId, o => o.Status == "paid"),

                        // Approximate distinct variants
                        PaidCustomersApprox = ClickHouseAggregates.UniqCombinedIf(g, o => o.CustomerId, o => o.Status == "paid"),

                        // Two-selector
                        LatestFulfilledAmount = ClickHouseAggregates.ArgMaxIf(g, o => o.Amount, o => o.CreatedAt, o => o.Status == "fulfilled"),
                        EarliestFulfilledAmount = ClickHouseAggregates.ArgMinIf(g, o => o.Amount, o => o.CreatedAt, o => o.Status == "fulfilled"),

                        // Array collectors
                        TopPaidProducts = ClickHouseAggregates.TopKIf(g, 3, o => o.ProductName, o => o.Status == "paid"),
                        TopWeightedPaidProducts = ClickHouseAggregates.TopKWeightedIf(g, 3, o => o.ProductName, o => o.Weight, o => o.Status == "paid"),
                        CancelledOrderIds = ClickHouseAggregates.GroupArrayIf(g, 5, o => o.OrderId, o => o.Status == "cancelled"),
                        UniqPaidProducts = ClickHouseAggregates.GroupUniqArrayIf(g, o => o.ProductName, o => o.Status == "paid"),

                        // Statistical -If family
                        MedianPaidAmount = ClickHouseAggregates.MedianIf(g, o => o.Amount, o => o.Status == "paid"),
                        StddevPaidAmount = ClickHouseAggregates.StddevPopIf(g, o => o.Amount, o => o.Status == "paid"),
                        VarPaidAmount = ClickHouseAggregates.VarPopIf(g, o => o.Amount, o => o.Status == "paid"),

                        // Parametric quantile -If variants
                        P95FulfillTimeExact = ClickHouseAggregates.QuantileExactIf(g, 0.95, o => o.FulfillTimeMs, o => o.Status == "paid"),
                        P95FulfillTimeTDigest = ClickHouseAggregates.QuantileTDigestIf(g, 0.95, o => o.FulfillTimeMs, o => o.Status == "paid"),
                        P95FulfillTimeTiming = ClickHouseAggregates.QuantileTimingIf(g, 0.95, o => o.FulfillTimeMs, o => o.Status == "paid"),
                        P95FulfillTimeDD = ClickHouseAggregates.QuantileDDIf(g, 0.01, 0.95, o => o.FulfillTimeMs, o => o.Status == "paid"),

                        // Multi-quantile: 3 percentiles in one pass
                        FulfillTimePercentiles = ClickHouseAggregates.QuantilesIf(g, new[] { 0.5, 0.9, 0.99 }, o => o.FulfillTimeMs, o => o.Status == "paid"),
                        FulfillTimePercentilesTDigest = ClickHouseAggregates.QuantilesTDigestIf(g, new[] { 0.5, 0.9, 0.99 }, o => o.FulfillTimeMs, o => o.Status == "paid")
                    }));
    }
}
