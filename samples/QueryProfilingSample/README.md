# QueryProfilingSample

Demonstrates ClickHouse query profiling capabilities: EXPLAIN queries, execution statistics, and query plan analysis.

## What This Shows

- `ExplainAsync()` for EXPLAIN queries with full options
- All EXPLAIN types (PLAN, AST, SYNTAX, QUERY TREE, PIPELINE, ESTIMATE)
- EXPLAIN options (JSON, indexes, actions, header, graph)
- `ToListWithStatsAsync()` for query execution with statistics
- `ExplainSqlAsync()` for raw SQL EXPLAIN
- Convenience methods (ExplainPlanAsync, ExplainAstAsync, etc.)

## API Reference

| Method | Description | Returns |
|--------|-------------|---------|
| `query.ExplainAsync(context)` | Execute EXPLAIN with options | `ExplainResult` |
| `query.ExplainPlanAsync(context)` | EXPLAIN PLAN shorthand | `ExplainResult` |
| `query.ExplainAstAsync(context)` | EXPLAIN AST shorthand | `ExplainResult` |
| `query.ExplainSyntaxAsync(context)` | EXPLAIN SYNTAX shorthand | `ExplainResult` |
| `query.ExplainQueryTreeAsync(context)` | EXPLAIN QUERY TREE shorthand | `ExplainResult` |
| `query.ExplainPipelineAsync(context)` | EXPLAIN PIPELINE shorthand | `ExplainResult` |
| `query.ExplainEstimateAsync(context)` | EXPLAIN ESTIMATE shorthand | `ExplainResult` |
| `query.ToListWithStatsAsync(context)` | Execute with statistics | `QueryResultWithStats<T>` |
| `context.ExplainSqlAsync(sql)` | EXPLAIN raw SQL | `ExplainResult` |

## EXPLAIN Types

| Type | Description |
|------|-------------|
| `Plan` | Query execution plan (default) |
| `Ast` | Abstract Syntax Tree |
| `Syntax` | Query after syntax optimization |
| `QueryTree` | Query tree representation |
| `Pipeline` | Query execution pipeline |
| `Estimate` | Estimated rows and bytes to read |

## ExplainOptions

| Option | Type | Description | Applicable Types |
|--------|------|-------------|------------------|
| `Type` | `ExplainType` | Type of EXPLAIN to execute | All |
| `Json` | `bool` | Output as JSON | Plan |
| `Indexes` | `bool` | Show index usage | Plan, Pipeline |
| `Actions` | `bool` | Show detailed actions | Plan |
| `Header` | `bool` | Show column headers | Plan, Pipeline |
| `Graph` | `bool` | Output as DOT graph | Pipeline |
| `Passes` | `bool` | Show optimization passes | QueryTree |
| `Description` | `bool` | Detailed descriptions | Plan, QueryTree |
| `Compact` | `bool` | Compact output | Pipeline |

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
# Start ClickHouse
docker-compose up -d

# Run the sample
dotnet run

# Stop ClickHouse
docker-compose down
```

## Expected Output

```
Query Profiling Sample
======================

Creating database and tables...
Inserting sample data...

Inserted 1000 orders.

--- 1. Basic EXPLAIN PLAN ---
EXPLAIN Type: Plan
Elapsed: 12.34ms
Output:
Expression ((Project names + Projection))
  Filter (WHERE)
    ReadFromMergeTree (default.Orders)

--- 2. All EXPLAIN Types ---
EXPLAIN AST:
SelectWithUnionQuery (children 1)
 ExpressionList (children 1)
  SelectQuery (children 3)
...

EXPLAIN SYNTAX:
SELECT ... FROM Orders WHERE Status = 'shipped'

EXPLAIN QUERY TREE:
QUERY id: 0
  PROJECTION COLUMNS
...

EXPLAIN PIPELINE:
(Expression)
ExpressionTransform
  (Filter)
  FilterTransform
    (ReadFromMergeTree)
    MergeTreeSelect(pool: ReadPool, algorithm: Thread)

EXPLAIN ESTIMATE:
default    Orders    1000    8000    1

--- 3. EXPLAIN with JSON Output ---
JSON Output (first 500 chars):
[
  {
    "Plan": {
      "Node Type": "Expression",
      ...
    }
  }
]
...

--- 8. Query with Statistics (ToListWithStatsAsync) ---
Results: 50 orders
Elapsed: 25.67ms
SQL: SELECT ... FROM Orders WHERE Status = 'shipped' ORDER BY Amount DESC LIMIT 50

Execution Statistics:
  Rows read: 1,000
  Bytes read: 45,678
  Duration: 5.23ms
  Memory usage: 1,234,567 bytes
  Peak memory: 2,345,678 bytes

Summary: Rows read: 1,000, Bytes read: 44.61 KB, Duration: 5.23ms, Memory: 1.18 MB, Peak: 2.24 MB

Done!
```

## Key Code

### Basic EXPLAIN

```csharp
using EF.CH.Extensions;

var query = context.Orders.Where(o => o.Status == "shipped");
var result = await query.ExplainAsync(context);

Console.WriteLine(result.FormattedOutput);
```

### EXPLAIN with Options

```csharp
var result = await query.ExplainAsync(context, opts =>
{
    opts.Type = ExplainType.Plan;
    opts.Json = true;
    opts.Indexes = true;
    opts.Actions = true;
});

Console.WriteLine(result.JsonOutput);
```

### Convenience Methods

```csharp
// Shorthand for specific EXPLAIN types
var plan = await query.ExplainPlanAsync(context);
var ast = await query.ExplainAstAsync(context);
var syntax = await query.ExplainSyntaxAsync(context);
var queryTree = await query.ExplainQueryTreeAsync(context);
var pipeline = await query.ExplainPipelineAsync(context);
var estimate = await query.ExplainEstimateAsync(context);
```

### Query with Statistics

```csharp
var resultWithStats = await query.ToListWithStatsAsync(context);

Console.WriteLine($"Results: {resultWithStats.Count}");
Console.WriteLine($"Statistics: {resultWithStats.Statistics?.Summary}");
```

### Raw SQL EXPLAIN

```csharp
var result = await context.ExplainSqlAsync(
    "SELECT Region, sum(Amount) FROM Orders GROUP BY Region",
    opts => opts.Type = ExplainType.Plan);
```

## ExplainResult Properties

| Property | Type | Description |
|----------|------|-------------|
| `Type` | `ExplainType` | Type of EXPLAIN executed |
| `Output` | `IReadOnlyList<string>` | Raw output lines |
| `FormattedOutput` | `string` | Output joined with newlines |
| `OriginalSql` | `string` | Original SQL query |
| `ExplainSql` | `string` | Full EXPLAIN SQL executed |
| `Elapsed` | `TimeSpan` | Time to execute EXPLAIN |
| `JsonOutput` | `string?` | JSON output when Json=true |

## QueryStatistics Properties

| Property | Type | Description |
|----------|------|-------------|
| `RowsRead` | `long` | Rows read during execution |
| `BytesRead` | `long` | Bytes read during execution |
| `QueryDurationMs` | `double` | Query duration in milliseconds |
| `MemoryUsage` | `long` | Memory usage in bytes |
| `PeakMemoryUsage` | `long` | Peak memory usage in bytes |
| `Summary` | `string` | Formatted summary string |

## QueryResultWithStats Properties

| Property | Type | Description |
|----------|------|-------------|
| `Results` | `IReadOnlyList<T>` | Query results |
| `Statistics` | `QueryStatistics?` | Execution statistics (may be null) |
| `Sql` | `string` | SQL query executed |
| `Elapsed` | `TimeSpan` | Total elapsed time |
| `Count` | `int` | Number of results |

## Learn More

- [Query Profiling Documentation](../../docs/features/query-profiling.md)
- [ClickHouse EXPLAIN Docs](https://clickhouse.com/docs/en/sql-reference/statements/explain)
