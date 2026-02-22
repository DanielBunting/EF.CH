# Query Profiling

EF.CH provides query profiling tools to inspect how ClickHouse executes LINQ queries. Seven EXPLAIN variants expose different levels of query plan detail, and `ToListWithStatsAsync` returns execution statistics alongside query results.

## EXPLAIN Variants

All EXPLAIN methods accept an `IQueryable<T>` and the `DbContext`, returning an `ExplainResult` with the plan output.

### ExplainAsync

The default EXPLAIN output showing the query execution plan.

```csharp
var query = context.Set<Order>()
    .Where(x => x.Amount > 100)
    .OrderBy(x => x.OrderDate);

var result = await query.ExplainAsync(context);

Console.WriteLine(result.Output);
```

Generated SQL:

```sql
EXPLAIN SELECT "o"."Id", "o"."Amount", "o"."OrderDate"
FROM "Order" AS "o"
WHERE "o"."Amount" > 100
ORDER BY "o"."OrderDate"
```

### ExplainPlanAsync

Shows the logical query plan with details about each processing step.

```csharp
var plan = await query.ExplainPlanAsync(context);
Console.WriteLine(plan.Output);
```

```sql
EXPLAIN PLAN SELECT ...
```

### ExplainAstAsync

Displays the Abstract Syntax Tree of the parsed query.

```csharp
var ast = await query.ExplainAstAsync(context);
Console.WriteLine(ast.Output);
```

```sql
EXPLAIN AST SELECT ...
```

### ExplainSyntaxAsync

Shows the query after ClickHouse's syntax optimizations have been applied.

```csharp
var syntax = await query.ExplainSyntaxAsync(context);
Console.WriteLine(syntax.Output);
```

```sql
EXPLAIN SYNTAX SELECT ...
```

### ExplainQueryTreeAsync

Displays the query tree representation used by the new ClickHouse query analyzer.

```csharp
var tree = await query.ExplainQueryTreeAsync(context);
Console.WriteLine(tree.Output);
```

```sql
EXPLAIN QUERY TREE SELECT ...
```

### ExplainPipelineAsync

Shows the execution pipeline -- the sequence of processors that will handle the query.

```csharp
var pipeline = await query.ExplainPipelineAsync(context);
Console.WriteLine(pipeline.Output);
```

```sql
EXPLAIN PIPELINE SELECT ...
```

### ExplainEstimateAsync

Returns estimated row counts and data size without executing the query.

```csharp
var estimate = await query.ExplainEstimateAsync(context);
Console.WriteLine(estimate.Output);
```

```sql
EXPLAIN ESTIMATE SELECT ...
```

## ToListWithStatsAsync

Executes a query and returns both the results and execution statistics from `system.query_log`.

```csharp
var result = await context.Set<Order>()
    .Where(x => x.Amount > 100)
    .ToListWithStatsAsync(context);

// Access query results
var orders = result.Results;

// Access execution statistics
var stats = result.Statistics;
Console.WriteLine($"Rows read: {stats?.RowsRead}");
Console.WriteLine($"Bytes read: {stats?.BytesRead}");
Console.WriteLine($"Elapsed: {stats?.ElapsedMs}ms");
Console.WriteLine($"Memory usage: {stats?.MemoryUsage}");
```

> **Note:** Statistics are retrieved from `system.query_log` after query execution. Due to ClickHouse's asynchronous logging, statistics may not be immediately available. Retrieval is best-effort.

## Explaining Raw SQL

You can also explain raw SQL strings directly.

```csharp
var result = await context.ExplainSqlAsync(
    "SELECT count() FROM events WHERE timestamp > now() - INTERVAL 1 HOUR"
);
Console.WriteLine(result.Output);
```

## Performance Analysis Workflow

A typical workflow for investigating slow queries:

```csharp
// 1. Get the execution plan
var plan = await query.ExplainPlanAsync(context);
Console.WriteLine("=== Query Plan ===");
Console.WriteLine(plan.Output);

// 2. Check the pipeline for parallelism
var pipeline = await query.ExplainPipelineAsync(context);
Console.WriteLine("=== Pipeline ===");
Console.WriteLine(pipeline.Output);

// 3. Get row/size estimates
var estimate = await query.ExplainEstimateAsync(context);
Console.WriteLine("=== Estimate ===");
Console.WriteLine(estimate.Output);

// 4. Execute and measure
var result = await query.ToListWithStatsAsync(context);
Console.WriteLine($"=== Statistics ===");
Console.WriteLine($"Rows: {result.Statistics?.RowsRead}");
Console.WriteLine($"Bytes: {result.Statistics?.BytesRead}");
Console.WriteLine($"Time: {result.Statistics?.ElapsedMs}ms");

// 5. Check optimized syntax
var syntax = await query.ExplainSyntaxAsync(context);
Console.WriteLine("=== Optimized SQL ===");
Console.WriteLine(syntax.Output);
```

> **Note:** EXPLAIN QUERY TREE requires ClickHouse 23.4 or later. EXPLAIN ESTIMATE requires ClickHouse 21.2 or later. Other variants are available in all supported ClickHouse versions.

## See Also

- [Query Modifiers](../features/query-modifiers.md) -- PREWHERE, SETTINGS, and other performance-related extensions
- [Skip Indices](../features/skip-indices.md) -- data skipping indices for query acceleration
