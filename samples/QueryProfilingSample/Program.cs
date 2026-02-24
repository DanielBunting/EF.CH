// -----------------------------------------------------------------
// QueryProfilingSample - Query Profiling and EXPLAIN with EF.CH
// -----------------------------------------------------------------
// Demonstrates:
//   1. EXPLAIN (basic query plan)
//   2. EXPLAIN PLAN, EXPLAIN SYNTAX
//   3. EXPLAIN AST, EXPLAIN PIPELINE
//   4. ToListWithStatsAsync (query execution with statistics)
//   5. Performance analysis workflow
// -----------------------------------------------------------------

using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;

// Start ClickHouse container
var container = new ClickHouseBuilder()
    .WithImage("clickhouse/clickhouse-server:latest")
    .Build();

Console.WriteLine("Starting ClickHouse container...");
await container.StartAsync();
Console.WriteLine("ClickHouse container started.");

try
{
    var connectionString = container.GetConnectionString();
    await SeedData(connectionString);

    await DemoExplain(connectionString);
    await DemoExplainPlanAndSyntax(connectionString);
    await DemoExplainAstAndPipeline(connectionString);
    await DemoToListWithStats(connectionString);
    await DemoPerformanceWorkflow(connectionString);
}
finally
{
    Console.WriteLine("\nStopping container...");
    await container.DisposeAsync();
    Console.WriteLine("Done.");
}

// -----------------------------------------------------------------
// Seed sample data
// -----------------------------------------------------------------
static async Task SeedData(string connectionString)
{
    await using var context = new ProfilingDemoContext(connectionString);
    await context.Database.EnsureCreatedAsync();

    // Generate 10,000 page views for profiling
    var random = new Random(42);
    var now = DateTime.UtcNow;
    var pageViews = new List<PageView>(10_000);
    for (int i = 0; i < 10_000; i++)
    {
        pageViews.Add(new PageView
        {
            Id = (ulong)i,
            UserId = (ulong)(i % 100) + 1,
            Url = $"/page/{i % 50}",
            Duration = random.Next(100, 5100),
            ViewedAt = now.AddSeconds(-i * 10),
        });
    }

    await context.BulkInsertAsync(pageViews);
    Console.WriteLine("Seeded 10,000 page view records.\n");
}

// -----------------------------------------------------------------
// Demo 1: Basic EXPLAIN
// -----------------------------------------------------------------
static async Task DemoExplain(string connectionString)
{
    Console.WriteLine("=== 1. EXPLAIN (Basic) ===\n");

    await using var context = new ProfilingDemoContext(connectionString);

    // Build a LINQ query
    var query = context.PageViews
        .Where(p => p.Duration > 1000)
        .OrderByDescending(p => p.ViewedAt)
        .Take(100);

    // Get the EXPLAIN output
    var explain = await query.ExplainAsync(context);

    Console.WriteLine($"EXPLAIN type: {explain.Type}");
    Console.WriteLine($"Elapsed: {explain.Elapsed.TotalMilliseconds:F1}ms");
    Console.WriteLine($"\nOriginal SQL:\n  {explain.OriginalSql}\n");
    Console.WriteLine("EXPLAIN output:");
    foreach (var line in explain.Output)
    {
        Console.WriteLine($"  {line}");
    }
}

// -----------------------------------------------------------------
// Demo 2: EXPLAIN PLAN and EXPLAIN SYNTAX
// -----------------------------------------------------------------
static async Task DemoExplainPlanAndSyntax(string connectionString)
{
    Console.WriteLine("\n=== 2. EXPLAIN PLAN and EXPLAIN SYNTAX ===\n");

    await using var context = new ProfilingDemoContext(connectionString);

    var query = context.PageViews
        .Where(p => p.UserId == 42)
        .GroupBy(p => p.Url)
        .Select(g => new
        {
            Url = g.Key,
            AvgDuration = g.Average(p => p.Duration),
            ViewCount = g.Count()
        })
        .OrderByDescending(x => x.ViewCount);

    // EXPLAIN PLAN: Shows the execution plan (what operations ClickHouse will perform)
    Console.WriteLine("--- EXPLAIN PLAN ---");
    var plan = await query.ExplainPlanAsync(context);
    Console.WriteLine("Shows the execution plan tree:");
    foreach (var line in plan.Output)
    {
        Console.WriteLine($"  {line}");
    }

    // EXPLAIN SYNTAX: Shows the query after syntax optimization
    Console.WriteLine("\n--- EXPLAIN SYNTAX ---");
    var syntax = await query.ExplainSyntaxAsync(context);
    Console.WriteLine("Shows the optimized SQL after ClickHouse rewrites:");
    foreach (var line in syntax.Output)
    {
        Console.WriteLine($"  {line}");
    }
}

// -----------------------------------------------------------------
// Demo 3: EXPLAIN AST and EXPLAIN PIPELINE
// -----------------------------------------------------------------
static async Task DemoExplainAstAndPipeline(string connectionString)
{
    Console.WriteLine("\n=== 3. EXPLAIN AST and EXPLAIN PIPELINE ===\n");

    await using var context = new ProfilingDemoContext(connectionString);

    var query = context.PageViews
        .Where(p => p.Duration > 500)
        .OrderBy(p => p.ViewedAt);

    // EXPLAIN AST: Shows the abstract syntax tree
    Console.WriteLine("--- EXPLAIN AST ---");
    var ast = await query.ExplainAstAsync(context);
    Console.WriteLine("Abstract Syntax Tree (first 15 lines):");
    foreach (var line in ast.Output.Take(15))
    {
        Console.WriteLine($"  {line}");
    }
    if (ast.Output.Count > 15)
    {
        Console.WriteLine($"  ... ({ast.Output.Count - 15} more lines)");
    }

    // EXPLAIN PIPELINE: Shows the query execution pipeline
    Console.WriteLine("\n--- EXPLAIN PIPELINE ---");
    var pipeline = await query.ExplainPipelineAsync(context);
    Console.WriteLine("Execution pipeline (processing stages):");
    foreach (var line in pipeline.Output)
    {
        Console.WriteLine($"  {line}");
    }
}

// -----------------------------------------------------------------
// Demo 4: ToListWithStatsAsync
// -----------------------------------------------------------------
static async Task DemoToListWithStats(string connectionString)
{
    Console.WriteLine("\n=== 4. ToListWithStatsAsync ===\n");

    await using var context = new ProfilingDemoContext(connectionString);

    // Execute a query and get both results and execution statistics
    var query = context.PageViews
        .Where(p => p.Duration > 2000)
        .OrderByDescending(p => p.Duration)
        .Take(10);

    var result = await query.ToListWithStatsAsync(context);

    Console.WriteLine($"Query returned {result.Count} results in {result.Elapsed.TotalMilliseconds:F1}ms");
    Console.WriteLine($"SQL: {result.Sql}\n");

    if (result.Statistics != null)
    {
        Console.WriteLine("Execution statistics (from system.query_log):");
        Console.WriteLine($"  Rows read:      {result.Statistics.RowsRead:N0}");
        Console.WriteLine($"  Bytes read:      {result.Statistics.BytesRead:N0}");
        Console.WriteLine($"  Duration:        {result.Statistics.QueryDurationMs:F2}ms");
        Console.WriteLine($"  Memory usage:    {result.Statistics.MemoryUsage:N0} bytes");
        Console.WriteLine($"  Peak memory:     {result.Statistics.PeakMemoryUsage:N0} bytes");
        Console.WriteLine($"\n  Summary: {result.Statistics.Summary}");
    }
    else
    {
        Console.WriteLine("Statistics not yet available (async logging delay).");
        Console.WriteLine("In production, statistics are best-effort due to ClickHouse's");
        Console.WriteLine("asynchronous query_log flushing.");
    }

    Console.WriteLine("\nTop slow page views:");
    foreach (var pv in result.Results)
    {
        Console.WriteLine($"  [{pv.Id}] {pv.Url} - {pv.Duration}ms by user {pv.UserId}");
    }
}

// -----------------------------------------------------------------
// Demo 5: Performance Analysis Workflow
// -----------------------------------------------------------------
static async Task DemoPerformanceWorkflow(string connectionString)
{
    Console.WriteLine("\n=== 5. Performance Analysis Workflow ===\n");

    await using var context = new ProfilingDemoContext(connectionString);

    Console.WriteLine("Workflow: Identify slow queries, then optimize with EXPLAIN.\n");

    // Step 1: Write the query
    var query = context.PageViews
        .Where(p => p.UserId == 42 && p.Duration > 1000)
        .GroupBy(p => p.Url)
        .Select(g => new
        {
            Url = g.Key,
            AvgDuration = g.Average(p => p.Duration),
            MaxDuration = g.Max(p => p.Duration),
            ViewCount = g.Count()
        })
        .OrderByDescending(x => x.AvgDuration);

    // Step 2: Check the execution plan
    Console.WriteLine("Step 1: Check execution plan with ExplainPlanAsync");
    var plan = await query.ExplainPlanAsync(context);
    Console.WriteLine($"  Plan ({plan.Output.Count} lines):");
    foreach (var line in plan.Output.Take(5))
    {
        Console.WriteLine($"    {line}");
    }

    // Step 3: Check the optimized syntax
    Console.WriteLine("\nStep 2: Check optimized SQL with ExplainSyntaxAsync");
    var syntax = await query.ExplainSyntaxAsync(context);
    Console.WriteLine("  Optimized SQL:");
    foreach (var line in syntax.Output.Take(8))
    {
        Console.WriteLine($"    {line}");
    }

    // Step 4: Execute with stats to measure actual performance
    Console.WriteLine("\nStep 3: Execute with ToListWithStatsAsync");
    var result = await query.ToListWithStatsAsync(context);
    Console.WriteLine($"  Results: {result.Count} rows");
    Console.WriteLine($"  Wall time: {result.Elapsed.TotalMilliseconds:F1}ms");
    if (result.Statistics != null)
    {
        Console.WriteLine($"  Rows read: {result.Statistics.RowsRead:N0}");
        Console.WriteLine($"  Query duration: {result.Statistics.QueryDurationMs:F2}ms");
    }

    // Step 5: Optimization tips
    Console.WriteLine("\nStep 4: Optimization strategies");
    Console.WriteLine("  - Add skip indices for frequently filtered columns:");
    Console.WriteLine("    entity.HasIndex(x => x.UserId).UseBloomFilter();");
    Console.WriteLine("  - Use PREWHERE for selective filters on wide tables:");
    Console.WriteLine("    query.PreWhere(p => p.UserId == 42)");
    Console.WriteLine("  - Add projections for common aggregation patterns:");
    Console.WriteLine("    entity.HasProjection(\"by_user\").OrderBy(x => x.UserId);");
    Console.WriteLine("  - Use SAMPLE for approximate results on large datasets:");
    Console.WriteLine("    query.Sample(0.1) // 10% sample");
}

// -----------------------------------------------------------------
// Entities
// -----------------------------------------------------------------

public class PageView
{
    public ulong Id { get; set; }
    public ulong UserId { get; set; }
    public string Url { get; set; } = string.Empty;
    public int Duration { get; set; }
    public DateTime ViewedAt { get; set; }
}

// -----------------------------------------------------------------
// DbContext
// -----------------------------------------------------------------

public class ProfilingDemoContext(string connectionString) : DbContext
{
    public DbSet<PageView> PageViews => Set<PageView>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseClickHouse(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PageView>(entity =>
        {
            entity.HasNoKey();
            entity.ToTable("page_views");
            entity.UseMergeTree(x => new { x.ViewedAt, x.Id });

            // Skip indices for faster filtering
            entity.HasIndex(x => x.UserId)
                .UseBloomFilter(falsePositive: 0.01)
                .HasGranularity(3);

            entity.HasIndex(x => x.Url)
                .UseSet(maxRows: 100)
                .HasGranularity(4);
        });
    }
}
