# Aggregate Combinators

ClickHouse aggregate combinators modify aggregate function behavior. EF.CH provides full LINQ support for `-State`, `-Merge`, `-If`, and `Array*` combinators.

## Overview

Aggregate combinators are ClickHouse's way of controlling how aggregate functions operate:

| Combinator | Suffix | Purpose |
|------------|--------|---------|
| State | `-State` | Produce intermediate binary state instead of final result |
| Merge | `-Merge` | Combine previously stored states |
| If | `-If` | Conditional aggregation (only aggregate rows matching predicate) |
| Array | `array*` | Apply aggregate to array elements |

## When to Use

- **AggregatingMergeTree tables** with pre-computed aggregate states
- **Materialized views** that roll up data in stages (raw → hourly → daily)
- **Conditional aggregation** without complex CASE expressions
- **Array column aggregation** (sum, avg, min, max of array elements)

## State Combinators

State combinators produce binary aggregate state that can be stored and later merged. The state is stored in `byte[]` columns with `AggregateFunction` column type.

### Entity Setup

```csharp
public class HourlySummary
{
    public DateTime Hour { get; set; }
    public string Endpoint { get; set; } = string.Empty;

    // Aggregate state columns (opaque binary)
    public byte[] CountState { get; set; } = [];
    public byte[] SumAmountState { get; set; } = [];
    public byte[] AvgTimeState { get; set; } = [];
    public byte[] UniqUsersState { get; set; } = [];
    public byte[] P95LatencyState { get; set; } = [];
}
```

### Fluent Configuration

```csharp
modelBuilder.Entity<HourlySummary>(entity =>
{
    entity.HasNoKey();
    entity.UseAggregatingMergeTree(x => new { x.Hour, x.Endpoint });

    // Configure AggregateFunction column types
    entity.Property(e => e.CountState)
        .HasAggregateFunction("count", typeof(ulong));

    entity.Property(e => e.SumAmountState)
        .HasAggregateFunction("sum", typeof(long));

    entity.Property(e => e.AvgTimeState)
        .HasAggregateFunction("avg", typeof(double));

    entity.Property(e => e.UniqUsersState)
        .HasAggregateFunction("uniq", typeof(ulong));

    // For complex types, use raw ClickHouse type string
    entity.Property(e => e.P95LatencyState)
        .HasAggregateFunctionRaw("quantile", "Float64");
});
```

### Generated DDL

```sql
CREATE TABLE "hourly_summary" (
    "Hour" DateTime64(3),
    "Endpoint" String,
    "CountState" AggregateFunction(count, UInt64),
    "SumAmountState" AggregateFunction(sum, Int64),
    "AvgTimeState" AggregateFunction(avg, Float64),
    "UniqUsersState" AggregateFunction(uniq, UInt64),
    "P95LatencyState" AggregateFunction(quantile, Float64)
) ENGINE = AggregatingMergeTree()
ORDER BY ("Hour", "Endpoint")
```

### Using State Methods in LINQ

```csharp
using EF.CH.Extensions;

// In materialized view or projection definition
entity.AsMaterializedView<HourlySummary, ApiRequest>(
    query: requests => requests
        .GroupBy(r => new { Hour = r.Timestamp.ToStartOfHour(), r.Endpoint })
        .Select(g => new HourlySummary
        {
            Hour = g.Key.Hour,
            Endpoint = g.Key.Endpoint,
            CountState = g.CountState(),                         // countState()
            SumAmountState = g.SumState(r => r.Amount),          // sumState(Amount)
            AvgTimeState = g.AvgState(r => (double)r.ResponseTimeMs),
            UniqUsersState = g.UniqState(r => r.UserId),         // uniqState(UserId)
            P95LatencyState = g.QuantileState(0.95, r => r.Latency)
        }),
    populate: false);
```

### Available State Methods

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `CountState()` | `countState()` | Count state |
| `SumState(selector)` | `sumState(column)` | Sum state |
| `AvgState(selector)` | `avgState(column)` | Average state |
| `MinState(selector)` | `minState(column)` | Minimum state |
| `MaxState(selector)` | `maxState(column)` | Maximum state |
| `UniqState(selector)` | `uniqState(column)` | Approximate distinct count state |
| `UniqExactState(selector)` | `uniqExactState(column)` | Exact distinct count state |
| `QuantileState(level, selector)` | `quantileState(level)(column)` | Quantile state |
| `AnyState(selector)` | `anyState(column)` | Any value state |
| `AnyLastState(selector)` | `anyLastState(column)` | Last value state |

## Merge Combinators

Merge combinators combine previously stored aggregate states to produce final values.

### Querying with Merge

```csharp
using EF.CH.Extensions;

// Roll up hourly data to daily aggregates
var dailyStats = await context.HourlySummary
    .Where(s => s.Hour >= startOfDay && s.Hour < endOfDay)
    .GroupBy(s => s.Endpoint)
    .Select(g => new
    {
        Endpoint = g.Key,
        RequestCount = g.CountMerge(s => s.CountState),        // countMerge()
        TotalAmount = g.SumMerge<HourlySummary, long>(s => s.SumAmountState),
        AvgTime = g.AvgMerge(s => s.AvgTimeState),             // avgMerge()
        UniqueUsers = g.UniqMerge(s => s.UniqUsersState)       // uniqMerge()
    })
    .ToListAsync();
```

### Available Merge Methods

| Method | ClickHouse SQL | Return Type |
|--------|---------------|-------------|
| `CountMerge(selector)` | `countMerge(column)` | `long` |
| `SumMerge<TSource, TResult>(selector)` | `sumMerge(column)` | `TResult` |
| `AvgMerge(selector)` | `avgMerge(column)` | `double` |
| `MinMerge<TSource, TResult>(selector)` | `minMerge(column)` | `TResult` |
| `MaxMerge<TSource, TResult>(selector)` | `maxMerge(column)` | `TResult` |
| `UniqMerge(selector)` | `uniqMerge(column)` | `ulong` |
| `UniqExactMerge(selector)` | `uniqExactMerge(column)` | `ulong` |
| `QuantileMerge(level, selector)` | `quantileMerge(level)(column)` | `double` |
| `AnyMerge<TSource, TResult>(selector)` | `anyMerge(column)` | `TResult` |
| `AnyLastMerge<TSource, TResult>(selector)` | `anyLastMerge(column)` | `TResult` |

## If Combinators

If combinators apply aggregation conditionally, only including rows where the predicate is true.

### Using If Methods

```csharp
using EF.CH.Extensions;

var stats = await context.Orders
    .GroupBy(o => o.OrderDate.Date)
    .Select(g => new
    {
        Date = g.Key,
        // Count only high-value orders
        HighValueCount = g.CountIf(o => o.Amount > 1000),       // countIf(Amount > 1000)

        // Sum only high-value amounts
        HighValueTotal = g.SumIf(o => o.Amount, o => o.Amount > 1000),

        // Average of successful orders only
        AvgSuccessTime = g.AvgIf(o => o.ProcessingTime, o => o.Status == "success"),

        // Unique customers with high-value orders
        UniqueHighValueCustomers = g.UniqIf(o => o.CustomerId, o => o.Amount > 1000)
    })
    .ToListAsync();
```

### Available If Methods

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `CountIf(predicate)` | `countIf(condition)` | Count rows matching condition |
| `SumIf(selector, predicate)` | `sumIf(column, condition)` | Sum matching rows |
| `AvgIf(selector, predicate)` | `avgIf(column, condition)` | Average matching rows |
| `MinIf(selector, predicate)` | `minIf(column, condition)` | Minimum of matching rows |
| `MaxIf(selector, predicate)` | `maxIf(column, condition)` | Maximum of matching rows |
| `UniqIf(selector, predicate)` | `uniqIf(column, condition)` | Unique count of matching rows |
| `UniqExactIf(selector, predicate)` | `uniqExactIf(column, condition)` | Exact unique count |
| `AnyIf(selector, predicate)` | `anyIf(column, condition)` | Any value from matching rows |

## Array Combinators

Array combinators aggregate elements within array columns.

### Using Array Methods

```csharp
using EF.CH.Extensions;

var productStats = await context.Products
    .Select(p => new
    {
        p.Name,
        TotalPrices = p.Prices.ArraySum(),    // arraySum(Prices)
        AvgPrice = p.Prices.ArrayAvg(),       // arrayAvg(Prices)
        MinPrice = p.Prices.ArrayMin(),       // arrayMin(Prices)
        MaxPrice = p.Prices.ArrayMax(),       // arrayMax(Prices)
        PriceCount = p.Prices.ArrayCount()    // length(Prices)
    })
    .ToListAsync();
```

### Available Array Methods

| Method | ClickHouse SQL | Description |
|--------|---------------|-------------|
| `ArraySum()` | `arraySum(array)` | Sum of array elements |
| `ArrayAvg()` | `arrayAvg(array)` | Average of array elements |
| `ArrayMin()` | `arrayMin(array)` | Minimum element |
| `ArrayMax()` | `arrayMax(array)` | Maximum element |
| `ArrayCount()` | `length(array)` | Number of elements |

## Complete Example: Multi-Level Rollup

```csharp
// 1. Raw events table
public class ApiRequest
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public int ResponseTimeMs { get; set; }
    public bool Success { get; set; }
}

// 2. Hourly aggregates with state columns
public class HourlyApiStats
{
    public DateTime Hour { get; set; }
    public string Endpoint { get; set; } = string.Empty;
    public byte[] CountState { get; set; } = [];
    public byte[] AvgTimeState { get; set; } = [];
    public byte[] UniqUsersState { get; set; } = [];
    public byte[] SuccessCountState { get; set; } = [];
}

// 3. Configuration
modelBuilder.Entity<HourlyApiStats>(entity =>
{
    entity.HasNoKey();
    entity.UseAggregatingMergeTree(x => new { x.Hour, x.Endpoint });

    entity.Property(e => e.CountState).HasAggregateFunction("count", typeof(ulong));
    entity.Property(e => e.AvgTimeState).HasAggregateFunction("avg", typeof(double));
    entity.Property(e => e.UniqUsersState).HasAggregateFunction("uniq", typeof(ulong));
    entity.Property(e => e.SuccessCountState).HasAggregateFunction("count", typeof(ulong));

    // Materialized view from raw events
    entity.AsMaterializedView<HourlyApiStats, ApiRequest>(
        query: requests => requests
            .GroupBy(r => new { Hour = r.Timestamp.ToStartOfHour(), r.Endpoint })
            .Select(g => new HourlyApiStats
            {
                Hour = g.Key.Hour,
                Endpoint = g.Key.Endpoint,
                CountState = g.CountState(),
                AvgTimeState = g.AvgState(r => (double)r.ResponseTimeMs),
                UniqUsersState = g.UniqState(r => r.UserId),
                // Conditional state: only count successful requests
                SuccessCountState = g.CountState() // Note: for conditional state, use raw SQL
            }),
        populate: false);
});

// 4. Query: Roll up hourly to daily
var dailyReport = await context.HourlyApiStats
    .Where(s => s.Hour >= startDate && s.Hour < endDate)
    .GroupBy(s => new { Date = s.Hour.Date, s.Endpoint })
    .Select(g => new
    {
        Date = g.Key.Date,
        Endpoint = g.Key.Endpoint,
        TotalRequests = g.CountMerge(s => s.CountState),
        AvgResponseTime = g.AvgMerge(s => s.AvgTimeState),
        UniqueUsers = g.UniqMerge(s => s.UniqUsersState)
    })
    .OrderBy(x => x.Date)
    .ThenBy(x => x.Endpoint)
    .ToListAsync();
```

## Supported Underlying Types

For `HasAggregateFunction`, these CLR types are supported:

| CLR Type | ClickHouse Type |
|----------|-----------------|
| `byte` | `UInt8` |
| `sbyte` | `Int8` |
| `short` | `Int16` |
| `ushort` | `UInt16` |
| `int` | `Int32` |
| `uint` | `UInt32` |
| `long` | `Int64` |
| `ulong` | `UInt64` |
| `float` | `Float32` |
| `double` | `Float64` |
| `decimal` | `Decimal(18,4)` |
| `string` | `String` |
| `DateTime` | `DateTime64(3)` |

For unsupported or complex types, use `HasAggregateFunctionRaw`:

```csharp
entity.Property(e => e.CustomState)
    .HasAggregateFunctionRaw("groupArray", "Tuple(String, Int32)");
```

## See Also

- [AggregatingMergeTree Engine](../engines/aggregating-mergetree.md)
- [Materialized Views](materialized-views.md)
- [Projections](projections.md)
- [ClickHouse Aggregate Combinators](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators)
