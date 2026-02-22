# Query Profiling Sample

Demonstrates query profiling and EXPLAIN features using EF.CH.

## What This Sample Shows

1. **EXPLAIN** - Basic query plan output via `ExplainAsync`
2. **EXPLAIN PLAN / SYNTAX** - Execution plan details and optimized SQL via `ExplainPlanAsync` and `ExplainSyntaxAsync`
3. **EXPLAIN AST / PIPELINE** - Abstract syntax tree and execution pipeline via `ExplainAstAsync` and `ExplainPipelineAsync`
4. **ToListWithStatsAsync** - Execute queries and retrieve execution statistics from `system.query_log`
5. **Performance analysis workflow** - Step-by-step process for identifying and optimizing slow queries

## Prerequisites

- .NET 8.0 SDK
- Docker (for Testcontainers)

## Running

```bash
dotnet run --project samples/QueryProfilingSample/
```

## Key Concepts

### EXPLAIN Types

| Method | SQL | Purpose |
|--------|-----|---------|
| `ExplainAsync()` | `EXPLAIN SELECT ...` | Basic execution plan |
| `ExplainPlanAsync()` | `EXPLAIN PLAN SELECT ...` | Detailed execution plan tree |
| `ExplainSyntaxAsync()` | `EXPLAIN SYNTAX SELECT ...` | Query after syntax optimization |
| `ExplainAstAsync()` | `EXPLAIN AST SELECT ...` | Abstract syntax tree |
| `ExplainPipelineAsync()` | `EXPLAIN PIPELINE SELECT ...` | Query execution pipeline |
| `ExplainEstimateAsync()` | `EXPLAIN ESTIMATE SELECT ...` | Estimated row counts and sizes |

### Query Statistics

`ToListWithStatsAsync` executes the query and retrieves statistics from `system.query_log`:

```csharp
var result = await query.ToListWithStatsAsync(context);

Console.WriteLine($"Rows: {result.Count}");
Console.WriteLine($"Rows read: {result.Statistics?.RowsRead}");
Console.WriteLine($"Bytes read: {result.Statistics?.BytesRead}");
Console.WriteLine($"Duration: {result.Statistics?.QueryDurationMs}ms");
Console.WriteLine($"Memory: {result.Statistics?.MemoryUsage} bytes");
```

### Performance Optimization Workflow

1. Write the LINQ query
2. Check the execution plan with `ExplainPlanAsync`
3. Review optimized SQL with `ExplainSyntaxAsync`
4. Measure actual performance with `ToListWithStatsAsync`
5. Apply optimizations (skip indices, PREWHERE, projections, SAMPLE)
