# Aggregate Functions

EF.CH translates 66+ aggregate functions from C# LINQ to ClickHouse SQL. Standard LINQ aggregates (`Sum`, `Average`, `Min`, `Max`) are automatically translated to their null-safe `OrNull` variants, returning `NULL` for empty result sets instead of throwing or returning misleading zero values.

All ClickHouse-specific aggregates are defined on the `ClickHouseAggregates` static class and are used as extension methods on `IEnumerable<T>` within LINQ `GroupBy(...).Select(...)` projections.

---

## Standard LINQ Aggregates (Null-Safe via OrNull)

Standard LINQ aggregation methods are translated to null-safe ClickHouse equivalents automatically.

| C# Method | ClickHouse SQL | Return Type | Notes |
|-----------|----------------|-------------|-------|
| `g.Sum(x => x.Amount)` | `sumOrNull(Amount)` | Nullable numeric | Null-safe; type-wrapped for compatibility |
| `g.Average(x => x.Amount)` | `avgOrNull(Amount)` | `double?` | Null-safe; converts integers to Float64 |
| `g.Min(x => x.Amount)` | `minOrNull(Amount)` | Nullable of source type | Null-safe |
| `g.Max(x => x.Amount)` | `maxOrNull(Amount)` | Nullable of source type | Null-safe |
| `g.Count()` | `count()` | `int` | Standard count |
| `g.LongCount()` | `count()` | `long` | 64-bit count |
| `g.Count(x => x.Active)` | `countIf(Active)` | `int` | Conditional count via predicate |

```csharp
var result = await context.Orders
    .GroupBy(o => o.Category)
    .Select(g => new
    {
        Category = g.Key,
        TotalAmount = g.Sum(o => o.Amount),       // sumOrNull(Amount)
        AvgPrice = g.Average(o => o.Price),        // avgOrNull(Price)
        MinPrice = g.Min(o => o.Price),            // minOrNull(Price)
        MaxPrice = g.Max(o => o.Price),            // maxOrNull(Price)
        OrderCount = g.Count(),                    // count()
        ActiveCount = g.Count(o => o.IsActive)     // countIf(IsActive)
    })
    .ToListAsync();
```

---

## Uniqueness Aggregates

Approximate and exact count-distinct functions. All return `ulong`.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `g.Uniq(x => x.UserId)` | `uniq(UserId)` | Approximate count distinct (HyperLogLog), ~2% variance |
| `g.UniqExact(x => x.UserId)` | `uniqExact(UserId)` | Exact count distinct, more memory-intensive |
| `g.UniqCombined(x => x.UserId)` | `uniqCombined(UserId)` | Combined HyperLogLog + hash table + error correction |
| `g.UniqCombined64(x => x.UserId)` | `uniqCombined64(UserId)` | 64-bit hash variant for all data types |
| `g.UniqHLL12(x => x.UserId)` | `uniqHLL12(UserId)` | HyperLogLog with 2^12 cells |
| `g.UniqTheta(x => x.UserId)` | `uniqTheta(UserId)` | Theta sketch, supports set operations |

```csharp
var result = await context.PageViews
    .GroupBy(v => v.Page)
    .Select(g => new
    {
        Page = g.Key,
        ApproxVisitors = g.Uniq(v => v.UserId),         // uniq(UserId)
        ExactVisitors = g.UniqExact(v => v.UserId),      // uniqExact(UserId)
        EfficientCount = g.UniqCombined(v => v.UserId)   // uniqCombined(UserId)
    })
    .ToListAsync();
```

---

## ArgMax / ArgMin

Return the value of one column at the row where another column is maximum or minimum.

| C# Method | ClickHouse SQL |
|-----------|----------------|
| `g.ArgMax(x => x.Price, x => x.Timestamp)` | `argMax(Price, Timestamp)` |
| `g.ArgMin(x => x.Price, x => x.Timestamp)` | `argMin(Price, Timestamp)` |

```csharp
var result = await context.StockPrices
    .GroupBy(s => s.Symbol)
    .Select(g => new
    {
        Symbol = g.Key,
        LatestPrice = g.ArgMax(s => s.Price, s => s.Timestamp),   // argMax(Price, Timestamp)
        EarliestPrice = g.ArgMin(s => s.Price, s => s.Timestamp)  // argMin(Price, Timestamp)
    })
    .ToListAsync();
```

---

## Any Value

Return an arbitrary or last-encountered value from the group.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `g.AnyValue(x => x.Name)` | `any(Name)` | Returns first encountered value |
| `g.AnyLastValue(x => x.Name)` | `anyLast(Name)` | Returns last encountered value |

```csharp
var result = await context.Events
    .GroupBy(e => e.UserId)
    .Select(g => new
    {
        UserId = g.Key,
        SampleEvent = g.AnyValue(e => e.EventType),       // any(EventType)
        LastEvent = g.AnyLastValue(e => e.EventType)       // anyLast(EventType)
    })
    .ToListAsync();
```

---

## Quantile Family

Percentile and median calculations with multiple algorithm choices.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `g.Quantile(0.95, x => x.Duration)` | `quantile(0.95)(Duration)` | Approximate quantile (t-digest) |
| `g.Median(x => x.Duration)` | `median(Duration)` | 50th percentile shorthand |
| `g.QuantileTDigest(0.95, x => x.Duration)` | `quantileTDigest(0.95)(Duration)` | Explicit t-digest algorithm |
| `g.QuantileDD(0.01, 0.95, x => x.Duration)` | `quantileDD(0.01, 0.95)(Duration)` | DDSketch with relative accuracy parameter |
| `g.QuantileExact(0.95, x => x.Duration)` | `quantileExact(0.95)(Duration)` | Exact quantile, more resource-intensive |
| `g.QuantileTiming(0.95, x => x.Duration)` | `quantileTiming(0.95)(Duration)` | Optimized for timing/latency distributions |
| `g.Quantiles(new[] {0.5, 0.9, 0.99}, x => x.Duration)` | `quantiles(0.5, 0.9, 0.99)(Duration)` | Multiple quantiles in a single pass |
| `g.QuantilesTDigest(new[] {0.5, 0.9, 0.99}, x => x.Duration)` | `quantilesTDigest(0.5, 0.9, 0.99)(Duration)` | Multiple t-digest quantiles in a single pass |

```csharp
var result = await context.Requests
    .GroupBy(r => r.Endpoint)
    .Select(g => new
    {
        Endpoint = g.Key,
        P50 = g.Quantile(0.5, r => r.LatencyMs),                 // quantile(0.5)(LatencyMs)
        P95 = g.Quantile(0.95, r => r.LatencyMs),                // quantile(0.95)(LatencyMs)
        P99 = g.QuantileExact(0.99, r => r.LatencyMs),           // quantileExact(0.99)(LatencyMs)
        MedianLatency = g.Median(r => r.LatencyMs),              // median(LatencyMs)
        Percentiles = g.Quantiles(new[] { 0.5, 0.9, 0.99 },     // quantiles(0.5, 0.9, 0.99)(LatencyMs)
                                  r => r.LatencyMs)
    })
    .ToListAsync();
```

---

## Statistical Aggregates

Population and sample variance/standard deviation.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `g.StddevPop(x => x.Value)` | `stddevPop(Value)` | Population standard deviation |
| `g.StddevSamp(x => x.Value)` | `stddevSamp(Value)` | Sample standard deviation |
| `g.VarPop(x => x.Value)` | `varPop(Value)` | Population variance |
| `g.VarSamp(x => x.Value)` | `varSamp(Value)` | Sample variance |

```csharp
var result = await context.Measurements
    .GroupBy(m => m.SensorId)
    .Select(g => new
    {
        SensorId = g.Key,
        StdDev = g.StddevPop(m => m.Temperature),    // stddevPop(Temperature)
        Variance = g.VarPop(m => m.Temperature),      // varPop(Temperature)
        SampleStdDev = g.StddevSamp(m => m.Temperature) // stddevSamp(Temperature)
    })
    .ToListAsync();
```

---

## Array Aggregates

Collect values into arrays or find the most frequent values.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `g.GroupArray(x => x.Tag)` | `groupArray(Tag)` | Collect all values into an array |
| `g.GroupArray(10, x => x.Tag)` | `groupArray(10)(Tag)` | Collect up to 10 values |
| `g.GroupUniqArray(x => x.Tag)` | `groupUniqArray(Tag)` | Collect all unique values |
| `g.GroupUniqArray(10, x => x.Tag)` | `groupUniqArray(10)(Tag)` | Collect up to 10 unique values |
| `g.TopK(5, x => x.Tag)` | `topK(5)(Tag)` | Top 5 most frequent values |
| `g.TopKWeighted(5, x => x.Tag, x => x.Weight)` | `topKWeighted(5)(Tag, Weight)` | Top 5 weighted by another column |

```csharp
var result = await context.Articles
    .GroupBy(a => a.Author)
    .Select(g => new
    {
        Author = g.Key,
        AllTags = g.GroupArray(a => a.Tag),           // groupArray(Tag)
        Top3Tags = g.GroupArray(3, a => a.Tag),       // groupArray(3)(Tag)
        UniqueTags = g.GroupUniqArray(a => a.Tag),    // groupUniqArray(Tag)
        TopCategories = g.TopK(3, a => a.Category)    // topK(3)(Category)
    })
    .ToListAsync();
```

---

## -If Combinators (Conditional Aggregation)

Apply an aggregate only to rows matching a predicate. Translated to ClickHouse `-If` combinator functions.

| C# Method | ClickHouse SQL |
|-----------|----------------|
| `g.CountIf(x => x.Active)` | `countIf(Active)` |
| `g.SumIf(x => x.Amount, x => x.Active)` | `sumIf(Amount, Active)` |
| `g.AvgIf(x => x.Amount, x => x.Active)` | `avgIf(Amount, Active)` |
| `g.MinIf(x => x.Amount, x => x.Active)` | `minIf(Amount, Active)` |
| `g.MaxIf(x => x.Amount, x => x.Active)` | `maxIf(Amount, Active)` |
| `g.UniqIf(x => x.UserId, x => x.Active)` | `uniqIf(UserId, Active)` |
| `g.UniqExactIf(x => x.UserId, x => x.Active)` | `uniqExactIf(UserId, Active)` |
| `g.AnyIf(x => x.Name, x => x.Active)` | `anyIf(Name, Active)` |
| `g.AnyLastIf(x => x.Name, x => x.Active)` | `anyLastIf(Name, Active)` |
| `g.QuantileIf(0.95, x => x.Duration, x => x.Active)` | `quantileIf(0.95)(Duration, Active)` |

```csharp
var result = await context.Orders
    .GroupBy(o => o.Region)
    .Select(g => new
    {
        Region = g.Key,
        PaidTotal = g.SumIf(o => o.Amount, o => o.IsPaid),        // sumIf(Amount, IsPaid)
        PaidCount = g.CountIf(o => o.IsPaid),                     // countIf(IsPaid)
        AvgPaidAmount = g.AvgIf(o => o.Amount, o => o.IsPaid),    // avgIf(Amount, IsPaid)
        PaidUsers = g.UniqIf(o => o.UserId, o => o.IsPaid),       // uniqIf(UserId, IsPaid)
        P95Latency = g.QuantileIf(0.95,                           // quantileIf(0.95)(Duration, IsPaid)
            o => o.Duration,
            o => o.IsPaid)
    })
    .ToListAsync();
```

---

## DefaultForNull Sentinel Exclusion

When columns are configured with `HasDefaultForNull(sentinel)`, the provider automatically excludes sentinel values from `OrNull` aggregate functions. This means `Sum()` on a column where missing values default to 0 will correctly ignore those rows in the aggregation.

```csharp
// Configuration
modelBuilder.Entity<Metric>()
    .Property(m => m.Value)
    .HasDefaultForNull(0); // 0 is the sentinel for "no value"

// Query - sentinel values are automatically excluded from the aggregate
var total = await context.Metrics
    .GroupBy(m => m.Category)
    .Select(g => new
    {
        Category = g.Key,
        Total = g.Sum(m => m.Value)  // sumOrNull(Value) with sentinel exclusion
    })
    .ToListAsync();
```

---

## See Also

- [Aggregate Combinators](aggregate-combinators.md) -- `-State`, `-Merge`, and array combinators for AggregatingMergeTree
- [DateTime Functions](datetime-functions.md) -- date truncation functions commonly used in GROUP BY
- [Math Functions](math-functions.md) -- mathematical functions for computed aggregations
