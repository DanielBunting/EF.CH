# Query Profiling

EF.CH provides query profiling capabilities to analyze ClickHouse query execution plans, estimate performance, and collect execution statistics. Use these tools to understand how your LINQ queries translate to SQL and how ClickHouse executes them.

## Why Query Profiling?

ClickHouse provides powerful EXPLAIN commands that reveal:

- **Execution plans**: How ClickHouse will execute your query
- **Index usage**: Whether primary keys and skip indices are being used
- **Row estimates**: How many rows will be read
- **Pipeline structure**: The processing stages involved

Understanding query plans helps you:

- Identify slow queries before they impact production
- Verify that filters use appropriate indices
- Optimize complex aggregations and joins
- Estimate resource requirements

## Basic Usage

### EXPLAIN a LINQ Query

```csharp
using EF.CH.Extensions;

var query = context.Orders.Where(o => o.Status == "shipped");
var result = await query.ExplainAsync(context);

Console.WriteLine(result.FormattedOutput);
// Output:
// Expression ((Project names + Projection))
//   Filter (WHERE)
//     ReadFromMergeTree (default.Orders)
```

### Convenience Methods

For common EXPLAIN types, use the shorthand methods:

```csharp
var plan = await query.ExplainPlanAsync(context);       // EXPLAIN PLAN
var ast = await query.ExplainAstAsync(context);         // EXPLAIN AST
var syntax = await query.ExplainSyntaxAsync(context);   // EXPLAIN SYNTAX
var tree = await query.ExplainQueryTreeAsync(context);  // EXPLAIN QUERY TREE
var pipeline = await query.ExplainPipelineAsync(context); // EXPLAIN PIPELINE
var estimate = await query.ExplainEstimateAsync(context); // EXPLAIN ESTIMATE
```

## EXPLAIN Types

| Type | SQL Generated | Description |
|------|---------------|-------------|
| `Plan` | `EXPLAIN PLAN` | Query execution plan (default) |
| `Ast` | `EXPLAIN AST` | Abstract Syntax Tree of the query |
| `Syntax` | `EXPLAIN SYNTAX` | Query after syntax optimization/rewriting |
| `QueryTree` | `EXPLAIN QUERY TREE` | Query tree representation |
| `Pipeline` | `EXPLAIN PIPELINE` | Query execution pipeline stages |
| `Estimate` | `EXPLAIN ESTIMATE` | Estimated row counts and bytes to read |

### EXPLAIN PLAN

Shows the logical execution plan:

```csharp
var result = await query.ExplainPlanAsync(context);
// Expression ((Project names + Projection))
//   Filter (WHERE)
//     ReadFromMergeTree (default.Orders)
```

### EXPLAIN AST

Shows the Abstract Syntax Tree:

```csharp
var result = await query.ExplainAstAsync(context);
// SelectWithUnionQuery (children 1)
//  ExpressionList (children 1)
//   SelectQuery (children 3)
//    ExpressionList (children 1)
//     Asterisk
```

### EXPLAIN SYNTAX

Shows the query after ClickHouse rewrites it:

```csharp
var result = await query.ExplainSyntaxAsync(context);
// SELECT Id, CustomerId, OrderDate, Amount, Status
// FROM default.Orders
// WHERE Status = 'shipped'
```

### EXPLAIN PIPELINE

Shows the processing pipeline:

```csharp
var result = await query.ExplainPipelineAsync(context);
// (Expression)
// ExpressionTransform
//   (Filter)
//   FilterTransform
//     (ReadFromMergeTree)
//     MergeTreeSelect(pool: ReadPool, algorithm: Thread)
```

### EXPLAIN ESTIMATE

Shows row and byte estimates:

```csharp
var result = await query.ExplainEstimateAsync(context);
// database    table     rows    bytes    parts
// default     Orders    10000   45000    3
```

## ExplainOptions

Configure EXPLAIN output using the options callback:

```csharp
var result = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Json = true;
    opts.Indexes = true;
    opts.Actions = true;
});
```

### Available Options

| Option | Type | Default | Description | Applicable Types |
|--------|------|---------|-------------|------------------|
| `Type` | `ExplainType` | `Plan` | Type of EXPLAIN to execute | All |
| `Json` | `bool` | `false` | Output result as JSON | Plan |
| `Indexes` | `bool` | `false` | Show index usage information | Plan, Pipeline |
| `Actions` | `bool` | `false` | Show detailed actions | Plan |
| `Header` | `bool` | `false` | Show column headers | Plan, Pipeline |
| `Graph` | `bool` | `false` | Output as DOT graph | Pipeline |
| `Passes` | `bool` | `false` | Show optimization passes | QueryTree |
| `Description` | `bool` | `false` | Detailed descriptions | Plan, QueryTree |
| `Compact` | `bool` | `false` | Compact output | Pipeline |

### JSON Output

Get the execution plan as JSON for programmatic analysis:

```csharp
var result = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Json = true;
});

Console.WriteLine(result.JsonOutput);
// [
//   {
//     "Plan": {
//       "Node Type": "Expression",
//       "Description": "(Project names + Projection)",
//       "Plans": [...]
//     }
//   }
// ]
```

### Index Analysis

Check if your query uses indices effectively:

```csharp
var result = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Indexes = true;
});
// Shows which parts/granules are skipped due to indices
```

### Pipeline Graph

Generate a DOT graph for visualization:

```csharp
var result = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Pipeline;
    opts.Graph = true;
});

// Save to file and visualize with Graphviz
File.WriteAllText("pipeline.dot", result.FormattedOutput);
// dot -Tpng pipeline.dot -o pipeline.png
```

## Query Statistics

Execute a query and retrieve execution statistics:

```csharp
var resultWithStats = await context.Orders
    .Where(o => o.Status == "shipped")
    .OrderByDescending(o => o.Amount)
    .Take(100)
    .ToListWithStatsAsync(context);

Console.WriteLine($"Results: {resultWithStats.Count}");
Console.WriteLine($"Rows read: {resultWithStats.Statistics?.RowsRead}");
Console.WriteLine($"Bytes read: {resultWithStats.Statistics?.BytesRead}");
Console.WriteLine($"Duration: {resultWithStats.Statistics?.QueryDurationMs}ms");
Console.WriteLine($"Summary: {resultWithStats.Statistics?.Summary}");
```

### QueryStatistics Properties

| Property | Type | Description |
|----------|------|-------------|
| `RowsRead` | `long` | Total rows read during execution |
| `BytesRead` | `long` | Total bytes read during execution |
| `QueryDurationMs` | `double` | Query duration in milliseconds |
| `MemoryUsage` | `long` | Memory usage in bytes |
| `PeakMemoryUsage` | `long` | Peak memory usage in bytes |
| `Summary` | `string` | Formatted human-readable summary |

### QueryResultWithStats Properties

| Property | Type | Description |
|----------|------|-------------|
| `Results` | `IReadOnlyList<T>` | The query results |
| `Statistics` | `QueryStatistics?` | Execution statistics (may be null) |
| `Sql` | `string` | The SQL query that was executed |
| `Elapsed` | `TimeSpan` | Total time including stats retrieval |
| `Count` | `int` | Number of results |

## Raw SQL EXPLAIN

Explain raw SQL queries directly:

```csharp
var result = await context.ExplainSqlAsync(
    "SELECT Region, sum(Amount) FROM Orders GROUP BY Region ORDER BY sum(Amount) DESC",
    opts =>
    {
        opts.Type = ExplainType.Plan;
        opts.Actions = true;
    });

Console.WriteLine(result.FormattedOutput);
```

## ExplainResult Properties

| Property | Type | Description |
|----------|------|-------------|
| `Type` | `ExplainType` | Type of EXPLAIN that was executed |
| `Output` | `IReadOnlyList<string>` | Raw output lines from EXPLAIN |
| `FormattedOutput` | `string` | Output lines joined with newlines |
| `OriginalSql` | `string` | Original SQL query (without EXPLAIN) |
| `ExplainSql` | `string` | Full EXPLAIN SQL that was executed |
| `Elapsed` | `TimeSpan` | Time to execute the EXPLAIN query |
| `JsonOutput` | `string?` | JSON output when `Json = true` |

## Use Cases

### Performance Debugging

Identify why a query is slow:

```csharp
var slowQuery = context.Events
    .Where(e => e.UserId == "user123")
    .OrderByDescending(e => e.Timestamp)
    .Take(100);

// Check if ORDER BY uses index
var estimate = await slowQuery.ExplainEstimateAsync(context);
Console.WriteLine($"Estimated rows to read: {estimate.FormattedOutput}");

// Check execution plan
var plan = await slowQuery.ExplainAsync(context, opts =>
{
    opts.Indexes = true;
    opts.Actions = true;
});
Console.WriteLine(plan.FormattedOutput);
```

### Query Optimization

Compare different query approaches:

```csharp
// Approach 1: Filter then group
var query1 = context.Orders
    .Where(o => o.Status == "shipped")
    .GroupBy(o => o.Region)
    .Select(g => new { Region = g.Key, Total = g.Sum(o => o.Amount) });

var estimate1 = await query1.ExplainEstimateAsync(context);

// Approach 2: Group with filter
var query2 = context.Orders
    .GroupBy(o => o.Region)
    .Select(g => new { Region = g.Key, Total = g.Where(o => o.Status == "shipped").Sum(o => o.Amount) });

var estimate2 = await query2.ExplainEstimateAsync(context);

// Compare estimated reads
```

### Validating Index Usage

Ensure skip indices are effective:

```csharp
var query = context.Logs
    .Where(l => l.ErrorCode == 500)  // Assuming skip index on ErrorCode
    .Where(l => l.Timestamp > DateTime.UtcNow.AddDays(-1));

var result = await query.ExplainAsync(context, opts =>
{
    opts.Indexes = true;
});

// Look for "Skip" in output to verify index is being used
```

## Limitations

- **Statistics retrieval**: Statistics are retrieved from `system.query_log` after query execution. Due to ClickHouse's asynchronous logging, statistics may not be immediately available and retrieval is best-effort.
- **EXPLAIN accuracy**: EXPLAIN ESTIMATE provides estimates, not exact counts. Actual execution may differ.
- **JSON output**: Only available for `ExplainType.Plan`.
- **Graph output**: Only available for `ExplainType.Pipeline`.

## Complete Example

```csharp
using EF.CH.Extensions;
using EF.CH.QueryProfiling;

// Build a complex query
var query = context.Orders
    .Where(o => o.OrderDate > DateTime.UtcNow.AddDays(-30))
    .Where(o => o.Status == "delivered")
    .GroupBy(o => o.Region)
    .Select(g => new
    {
        Region = g.Key,
        TotalAmount = g.Sum(o => o.Amount),
        OrderCount = g.Count()
    })
    .OrderByDescending(x => x.TotalAmount);

// Get execution plan with full details
var plan = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Actions = true;
    opts.Indexes = true;
    opts.Header = true;
});

Console.WriteLine("=== Execution Plan ===");
Console.WriteLine(plan.FormattedOutput);
Console.WriteLine($"\nOriginal SQL: {plan.OriginalSql}");
Console.WriteLine($"Elapsed: {plan.Elapsed.TotalMilliseconds:F2}ms");

// Get row estimate
var estimate = await query.ExplainEstimateAsync(context);
Console.WriteLine($"\n=== Estimate ===\n{estimate.FormattedOutput}");

// Execute with statistics
var results = await query.ToListWithStatsAsync(context);
Console.WriteLine($"\n=== Results ===");
Console.WriteLine($"Rows returned: {results.Count}");
if (results.Statistics != null)
{
    Console.WriteLine($"Statistics: {results.Statistics.Summary}");
}
```

## See Also

- [Query Profiling Sample](../../samples/QueryProfilingSample/README.md)
- [Query Modifiers](./query-modifiers.md) - PreWhere, Final, Sample, WithSettings
- [ClickHouse EXPLAIN Documentation](https://clickhouse.com/docs/en/sql-reference/statements/explain)
