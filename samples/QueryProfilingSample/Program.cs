using EF.CH.Extensions;
using EF.CH.QueryProfiling;
using Microsoft.EntityFrameworkCore;

// ============================================================
// Query Profiling Sample
// ============================================================
// Demonstrates ClickHouse query profiling capabilities:
// - EXPLAIN queries (PLAN, AST, SYNTAX, QUERY TREE, PIPELINE, ESTIMATE)
// - EXPLAIN with options (JSON, indexes, actions, header, graph)
// - Query execution with statistics (ToListWithStatsAsync)
// - Raw SQL EXPLAIN (ExplainSqlAsync)
// - Convenience methods (ExplainPlanAsync, ExplainAstAsync, etc.)
// ============================================================

Console.WriteLine("Query Profiling Sample");
Console.WriteLine("======================\n");

await using var context = new ProfilingDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureCreatedAsync();

// Insert sample data
Console.WriteLine("Inserting sample data...\n");

var random = new Random(42);
var orders = new List<Order>();

for (var i = 0; i < 1000; i++)
{
    orders.Add(new Order
    {
        Id = Guid.NewGuid(),
        CustomerId = $"CUST-{random.Next(100):D3}",
        OrderDate = DateTime.UtcNow.AddDays(-random.Next(365)),
        Amount = Math.Round(random.NextDouble() * 1000, 2),
        Status = random.Next(4) switch
        {
            0 => "pending",
            1 => "shipped",
            2 => "delivered",
            _ => "cancelled"
        },
        Region = random.Next(4) switch
        {
            0 => "North",
            1 => "South",
            2 => "East",
            _ => "West"
        }
    });
}

context.Orders.AddRange(orders);
await context.SaveChangesAsync();
Console.WriteLine($"Inserted {orders.Count} orders.\n");

// ============================================================
// 1. Basic EXPLAIN PLAN
// ============================================================
Console.WriteLine("--- 1. Basic EXPLAIN PLAN ---");

var query = context.Orders.Where(o => o.Status == "shipped");
var explainResult = await query.ExplainAsync(context);

Console.WriteLine($"EXPLAIN Type: {explainResult.Type}");
Console.WriteLine($"Elapsed: {explainResult.Elapsed.TotalMilliseconds:F2}ms");
Console.WriteLine("Output:");
Console.WriteLine(explainResult.FormattedOutput);
Console.WriteLine();

// ============================================================
// 2. All EXPLAIN Types
// ============================================================
Console.WriteLine("--- 2. All EXPLAIN Types ---");

// EXPLAIN AST - Abstract Syntax Tree
Console.WriteLine("EXPLAIN AST:");
var astResult = await query.ExplainAstAsync(context);
Console.WriteLine(string.Join("\n", astResult.Output.Take(5)));
Console.WriteLine("...\n");

// EXPLAIN SYNTAX - Query after syntax optimization
Console.WriteLine("EXPLAIN SYNTAX:");
var syntaxResult = await query.ExplainSyntaxAsync(context);
Console.WriteLine(syntaxResult.FormattedOutput);
Console.WriteLine();

// EXPLAIN QUERY TREE - Query tree representation
Console.WriteLine("EXPLAIN QUERY TREE:");
var queryTreeResult = await query.ExplainQueryTreeAsync(context);
Console.WriteLine(string.Join("\n", queryTreeResult.Output.Take(5)));
Console.WriteLine("...\n");

// EXPLAIN PIPELINE - Execution pipeline
Console.WriteLine("EXPLAIN PIPELINE:");
var pipelineResult = await query.ExplainPipelineAsync(context);
Console.WriteLine(pipelineResult.FormattedOutput);
Console.WriteLine();

// EXPLAIN ESTIMATE - Row count estimates
Console.WriteLine("EXPLAIN ESTIMATE:");
var estimateResult = await query.ExplainEstimateAsync(context);
Console.WriteLine(estimateResult.FormattedOutput);
Console.WriteLine();

// ============================================================
// 3. EXPLAIN with JSON Output
// ============================================================
Console.WriteLine("--- 3. EXPLAIN with JSON Output ---");

var jsonExplain = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Json = true;
});

Console.WriteLine("JSON Output (first 500 chars):");
var jsonOutput = jsonExplain.JsonOutput ?? jsonExplain.FormattedOutput;
Console.WriteLine(jsonOutput.Length > 500 ? jsonOutput[..500] + "..." : jsonOutput);
Console.WriteLine();

// ============================================================
// 4. EXPLAIN with Options (indexes, actions)
// ============================================================
Console.WriteLine("--- 4. EXPLAIN with Options (indexes, actions) ---");

var detailedQuery = context.Orders
    .Where(o => o.OrderDate > DateTime.UtcNow.AddDays(-30))
    .Where(o => o.Amount > 100);

var detailedExplain = await detailedQuery.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Indexes = true;
    opts.Actions = true;
    opts.Header = true;
});

Console.WriteLine("EXPLAIN with indexes and actions:");
Console.WriteLine(detailedExplain.FormattedOutput);
Console.WriteLine();

// ============================================================
// 5. EXPLAIN Pipeline with Graph
// ============================================================
Console.WriteLine("--- 5. EXPLAIN Pipeline with Graph ---");

var graphExplain = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Pipeline;
    opts.Graph = true;
});

Console.WriteLine("DOT Graph output (for Graphviz visualization):");
Console.WriteLine(string.Join("\n", graphExplain.Output.Take(10)));
Console.WriteLine("...\n");

// ============================================================
// 6. EXPLAIN ESTIMATE
// ============================================================
Console.WriteLine("--- 6. EXPLAIN ESTIMATE ---");

var largeQuery = context.Orders.Where(o => o.Region == "North");
var estimate = await largeQuery.ExplainEstimateAsync(context);

Console.WriteLine("Estimated rows and bytes to read:");
Console.WriteLine(estimate.FormattedOutput);
Console.WriteLine();

// ============================================================
// 7. Complex Query EXPLAIN
// ============================================================
Console.WriteLine("--- 7. Complex Query EXPLAIN ---");

var complexQuery = context.Orders
    .Where(o => o.Status == "delivered")
    .Where(o => o.Amount > 50)
    .GroupBy(o => o.Region)
    .Select(g => new { Region = g.Key, TotalAmount = g.Sum(o => o.Amount), Count = g.Count() })
    .OrderByDescending(x => x.TotalAmount);

var complexExplain = await complexQuery.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Actions = true;
});

Console.WriteLine("Complex query with GROUP BY:");
Console.WriteLine($"Original SQL:\n{complexExplain.OriginalSql}");
Console.WriteLine();
Console.WriteLine("Execution Plan:");
Console.WriteLine(complexExplain.FormattedOutput);
Console.WriteLine();

// ============================================================
// 8. Query with Statistics (ToListWithStatsAsync)
// ============================================================
Console.WriteLine("--- 8. Query with Statistics (ToListWithStatsAsync) ---");

var statsQuery = context.Orders
    .Where(o => o.Status == "shipped")
    .OrderByDescending(o => o.Amount)
    .Take(50);

var resultWithStats = await statsQuery.ToListWithStatsAsync(context);

Console.WriteLine($"Results: {resultWithStats.Count} orders");
Console.WriteLine($"Elapsed: {resultWithStats.Elapsed.TotalMilliseconds:F2}ms");
Console.WriteLine($"SQL: {resultWithStats.Sql}");

if (resultWithStats.Statistics != null)
{
    Console.WriteLine("\nExecution Statistics:");
    Console.WriteLine($"  Rows read: {resultWithStats.Statistics.RowsRead:N0}");
    Console.WriteLine($"  Bytes read: {resultWithStats.Statistics.BytesRead:N0}");
    Console.WriteLine($"  Duration: {resultWithStats.Statistics.QueryDurationMs:F2}ms");
    Console.WriteLine($"  Memory usage: {resultWithStats.Statistics.MemoryUsage:N0} bytes");
    Console.WriteLine($"  Peak memory: {resultWithStats.Statistics.PeakMemoryUsage:N0} bytes");
    Console.WriteLine($"\nSummary: {resultWithStats.Statistics.Summary}");
}
else
{
    Console.WriteLine("\nNote: Statistics not available (may not be logged immediately).");
}
Console.WriteLine();

// ============================================================
// 9. Raw SQL EXPLAIN
// ============================================================
Console.WriteLine("--- 9. Raw SQL EXPLAIN ---");

var rawSql = "SELECT Region, sum(Amount) as Total FROM Orders GROUP BY Region ORDER BY Total DESC";
var rawExplain = await context.ExplainSqlAsync(rawSql, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Actions = true;
});

Console.WriteLine($"Raw SQL: {rawSql}");
Console.WriteLine();
Console.WriteLine("Execution Plan:");
Console.WriteLine(rawExplain.FormattedOutput);
Console.WriteLine();

// ============================================================
// 10. Convenience Methods
// ============================================================
Console.WriteLine("--- 10. Convenience Methods Summary ---");

var sampleQuery = context.Orders.Where(o => o.Amount > 500);

Console.WriteLine("Available convenience methods:");
Console.WriteLine("  - ExplainPlanAsync()     -> EXPLAIN PLAN");
Console.WriteLine("  - ExplainAstAsync()      -> EXPLAIN AST");
Console.WriteLine("  - ExplainSyntaxAsync()   -> EXPLAIN SYNTAX");
Console.WriteLine("  - ExplainQueryTreeAsync() -> EXPLAIN QUERY TREE");
Console.WriteLine("  - ExplainPipelineAsync() -> EXPLAIN PIPELINE");
Console.WriteLine("  - ExplainEstimateAsync() -> EXPLAIN ESTIMATE");
Console.WriteLine("  - ToListWithStatsAsync() -> Execute with statistics");
Console.WriteLine("  - ExplainSqlAsync()      -> EXPLAIN raw SQL (on DbContext)");
Console.WriteLine();

// Quick demonstration
var planResult = await sampleQuery.ExplainPlanAsync(context);
Console.WriteLine($"ExplainPlanAsync result type: {planResult.Type}");

var estimateResult2 = await sampleQuery.ExplainEstimateAsync(context);
Console.WriteLine($"ExplainEstimateAsync result type: {estimateResult2.Type}");
Console.WriteLine();

Console.WriteLine("Done!");

// ============================================================
// Entity Definitions
// ============================================================

/// <summary>
/// Order entity for profiling demonstrations.
/// </summary>
public class Order
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public DateTime OrderDate { get; set; }
    public double Amount { get; set; }
    public string Status { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
}

// ============================================================
// DbContext Definition
// ============================================================

public class ProfilingDbContext : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=query_profiling_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
            entity.HasPartitionByMonth(x => x.OrderDate);
        });
    }
}
