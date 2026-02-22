# Aggregate Combinators

ClickHouse aggregate combinators extend base aggregate functions with additional behavior. EF.CH supports three combinator families: `-State` (store intermediate aggregate state), `-Merge` (combine stored states into final results), and array combinators (operate on array columns directly).

The `-State` and `-Merge` combinators are the foundation of the AggregatingMergeTree materialized view pattern, which enables incremental aggregation over streaming data.

---

## The AggregatingMergeTree Pattern

The `-State`/`-Merge` pattern works in two phases:

1. **Write phase**: A materialized view uses `-State` combinators to compute and store intermediate aggregate states as opaque binary data (`byte[]`) in an AggregatingMergeTree table.
2. **Read phase**: Queries use `-Merge` combinators to combine stored states into final aggregate results.

This pattern allows ClickHouse to incrementally update aggregates as new data arrives, without reprocessing historical data.

```
Raw Events Table (Null/MergeTree engine)
        |
        v  [Materialized View with -State functions]
        |
AggregatingMergeTree Table (stores binary aggregate states)
        |
        v  [Query with -Merge functions]
        |
    Final Results
```

---

## -State Combinators

State combinators compute an intermediate aggregate state and store it as opaque binary data (`byte[]`). All state methods are defined on `ClickHouseAggregates` and return `byte[]`.

| C# Method | ClickHouse SQL | Base Function |
|-----------|----------------|---------------|
| `g.CountState()` | `countState()` | `count` |
| `g.SumState(x => x.Amount)` | `sumState(Amount)` | `sum` |
| `g.AvgState(x => x.Amount)` | `avgState(Amount)` | `avg` |
| `g.MinState(x => x.Amount)` | `minState(Amount)` | `min` |
| `g.MaxState(x => x.Amount)` | `maxState(Amount)` | `max` |
| `g.UniqState(x => x.UserId)` | `uniqState(UserId)` | `uniq` |
| `g.UniqExactState(x => x.UserId)` | `uniqExactState(UserId)` | `uniqExact` |
| `g.QuantileState(0.95, x => x.Duration)` | `quantileState(0.95)(Duration)` | `quantile` |
| `g.AnyState(x => x.Name)` | `anyState(Name)` | `any` |
| `g.AnyLastState(x => x.Name)` | `anyLastState(Name)` | `anyLast` |

### Materialized View Definition with -State

```csharp
// AggregatingMergeTree target table entity
public class HourlySummary
{
    public DateTime Hour { get; set; }
    public string Category { get; set; } = "";
    public byte[] TotalAmountState { get; set; } = [];
    public byte[] OrderCountState { get; set; } = [];
    public byte[] UniqueUsersState { get; set; } = [];
    public byte[] P95LatencyState { get; set; } = [];
}

// Configuration
modelBuilder.Entity<HourlySummary>(entity =>
{
    entity.UseAggregatingMergeTree(x => new { x.Hour, x.Category });

    entity.Property(x => x.TotalAmountState)
        .HasAggregateFunction("sum", typeof(decimal));
    entity.Property(x => x.OrderCountState)
        .HasAggregateFunction("count");
    entity.Property(x => x.UniqueUsersState)
        .HasAggregateFunction("uniq", typeof(long));
    entity.Property(x => x.P95LatencyState)
        .HasAggregateFunctionRaw("quantile(0.95)", "Float64");

    entity.AsMaterializedView<HourlySummary, Order>(
        query: orders => orders
            .GroupBy(o => new { Hour = o.CreatedAt.ToStartOfHour(), o.Category })
            .Select(g => new HourlySummary
            {
                Hour = g.Key.Hour,
                Category = g.Key.Category,
                TotalAmountState = g.SumState(o => o.Amount),
                OrderCountState = g.CountState(),
                UniqueUsersState = g.UniqState(o => o.UserId),
                P95LatencyState = g.QuantileState(0.95, o => o.LatencyMs)
            })
    );
});
```

---

## -Merge Combinators

Merge combinators combine stored intermediate states into final aggregate values. They take a selector for the `byte[]` state column.

| C# Method | ClickHouse SQL | Return Type |
|-----------|----------------|-------------|
| `g.CountMerge(x => x.CountState)` | `countMerge(CountState)` | `long` |
| `g.SumMerge<TSource, TValue>(x => x.SumState)` | `sumMerge(SumState)` | `TValue` |
| `g.AvgMerge(x => x.AvgState)` | `avgMerge(AvgState)` | `double` |
| `g.MinMerge<TSource, TValue>(x => x.MinState)` | `minMerge(MinState)` | `TValue` |
| `g.MaxMerge<TSource, TValue>(x => x.MaxState)` | `maxMerge(MaxState)` | `TValue` |
| `g.UniqMerge(x => x.UniqState)` | `uniqMerge(UniqState)` | `ulong` |
| `g.UniqExactMerge(x => x.UniqExactState)` | `uniqExactMerge(UniqExactState)` | `ulong` |
| `g.QuantileMerge(0.95, x => x.QuantileState)` | `quantileMerge(0.95)(QuantileState)` | `double` |
| `g.AnyMerge<TSource, TValue>(x => x.AnyState)` | `anyMerge(AnyState)` | `TValue` |
| `g.AnyLastMerge<TSource, TValue>(x => x.AnyLastState)` | `anyLastMerge(AnyLastState)` | `TValue` |

### Querying with -Merge

```csharp
// Read from the AggregatingMergeTree table using -Merge combinators
var result = await context.HourlySummaries
    .Where(s => s.Hour >= startDate && s.Hour < endDate)
    .GroupBy(s => s.Category)
    .Select(g => new
    {
        Category = g.Key,
        TotalAmount = g.SumMerge<HourlySummary, decimal>(s => s.TotalAmountState),
        OrderCount = g.CountMerge(s => s.OrderCountState),
        UniqueUsers = g.UniqMerge(s => s.UniqueUsersState),
        P95Latency = g.QuantileMerge(0.95, s => s.P95LatencyState)
    })
    .ToListAsync();
```

---

## Complete -State/-Merge Example

This end-to-end example shows both the write and read sides of the pattern.

```csharp
// 1. Source table (raw events)
public class RawEvent
{
    public DateTime Timestamp { get; set; }
    public string Region { get; set; } = "";
    public double Value { get; set; }
    public long UserId { get; set; }
}

// 2. Aggregated table (stores intermediate states)
public class RegionalSummary
{
    public DateTime Day { get; set; }
    public string Region { get; set; } = "";
    public byte[] ValueSumState { get; set; } = [];
    public byte[] EventCountState { get; set; } = [];
    public byte[] UniqueUserState { get; set; } = [];
}

// 3. Configuration
modelBuilder.Entity<RegionalSummary>(entity =>
{
    entity.UseAggregatingMergeTree(x => new { x.Day, x.Region });

    entity.Property(x => x.ValueSumState).HasAggregateFunction("sum", typeof(double));
    entity.Property(x => x.EventCountState).HasAggregateFunction("count");
    entity.Property(x => x.UniqueUserState).HasAggregateFunction("uniq", typeof(long));

    entity.AsMaterializedView<RegionalSummary, RawEvent>(
        query: events => events
            .GroupBy(e => new { Day = e.Timestamp.Date, e.Region })
            .Select(g => new RegionalSummary
            {
                Day = g.Key.Day,
                Region = g.Key.Region,
                ValueSumState = g.SumState(e => e.Value),
                EventCountState = g.CountState(),
                UniqueUserState = g.UniqState(e => e.UserId)
            })
    );
});

// 4. Query with -Merge (roll up daily summaries into weekly)
var weeklySummary = await context.RegionalSummaries
    .GroupBy(s => new { Week = s.Day.ToStartOfWeek(), s.Region })
    .Select(g => new
    {
        Week = g.Key.Week,
        Region = g.Key.Region,
        TotalValue = g.SumMerge<RegionalSummary, double>(s => s.ValueSumState),
        EventCount = g.CountMerge(s => s.EventCountState),
        UniqueUsers = g.UniqMerge(s => s.UniqueUserState)
    })
    .ToListAsync();
```

---

## Array Combinators

Array combinators operate directly on array columns (not on grouped data). They are defined as extension methods on `T[]` and `IEnumerable<T>`.

| C# Method | ClickHouse SQL | Return Type | Notes |
|-----------|----------------|-------------|-------|
| `x.Scores.ArraySum()` | `arraySum(Scores)` | `T` | Sum of array elements |
| `x.Scores.ArrayAvg()` | `arrayAvg(Scores)` | `double` | Average of array elements |
| `x.Scores.ArrayMin()` | `arrayMin(Scores)` | `T` | Minimum array element |
| `x.Scores.ArrayMax()` | `arrayMax(Scores)` | `T` | Maximum array element |
| `x.Scores.ArrayCount()` | `length(Scores)` | `int` | Count of array elements |
| `x.Scores.ArrayCount(v => v > 50)` | `arrayCount(x -> x > 50, Scores)` | `int` | Count of elements matching predicate |

```csharp
var result = await context.Students
    .Select(s => new
    {
        s.Name,
        AvgScore = s.Scores.ArrayAvg(),                    // arrayAvg(Scores)
        TopScore = s.Scores.ArrayMax(),                     // arrayMax(Scores)
        TotalScore = s.Scores.ArraySum(),                   // arraySum(Scores)
        NumScores = s.Scores.ArrayCount(),                  // length(Scores)
        PassCount = s.Scores.ArrayCount(v => v >= 60)       // arrayCount(x -> x >= 60, Scores)
    })
    .ToListAsync();
```

### Combining Array Combinators with Grouping

Array combinators can be used together with regular aggregates:

```csharp
var result = await context.Students
    .GroupBy(s => s.Grade)
    .Select(g => new
    {
        Grade = g.Key,
        StudentCount = g.Count(),
        AvgTopScore = g.Average(s => s.Scores.ArrayMax())  // avgOrNull(arrayMax(Scores))
    })
    .ToListAsync();
```

---

## See Also

- [Aggregate Functions](aggregate-functions.md) -- standard aggregates, uniqueness, quantiles, and -If combinators
- [DateTime Functions](datetime-functions.md) -- date truncation functions for GROUP BY keys
