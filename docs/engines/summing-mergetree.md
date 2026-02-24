# SummingMergeTree Engine

SummingMergeTree automatically sums numeric columns when merging rows with the same ORDER BY key. This is designed for pre-aggregated data where you want ClickHouse to maintain running totals during background merges.

## Basic Configuration

```csharp
modelBuilder.Entity<HourlyStats>(entity =>
{
    entity.UseSummingMergeTree(x => new { x.Hour, x.Category });
});
```

```sql
CREATE TABLE "HourlyStats" (
    "Hour" DateTime64(3),
    "Category" String,
    "Views" Int64,
    "Clicks" Int64,
    "Revenue" Decimal(18, 4)
)
ENGINE = SummingMergeTree()
ORDER BY ("Hour", "Category")
```

## Which Columns Get Summed

During background merges, ClickHouse collapses rows that share the same ORDER BY key values. The behavior for each column is:

- **ORDER BY columns** (`Hour`, `Category` above): kept as-is (they define the group)
- **Numeric columns not in ORDER BY** (`Views`, `Clicks`, `Revenue`): values are summed
- **Non-numeric columns not in ORDER BY**: an arbitrary value is kept (typically the first)

```csharp
public class HourlyStats
{
    public DateTime Hour { get; set; }       // ORDER BY -- kept as-is
    public string Category { get; set; }      // ORDER BY -- kept as-is
    public long Views { get; set; }           // summed during merges
    public long Clicks { get; set; }          // summed during merges
    public decimal Revenue { get; set; }      // summed during merges
}
```

## String Overload

```csharp
entity.UseSummingMergeTree("Hour", "Category");
```

```sql
ENGINE = SummingMergeTree()
ORDER BY ("Hour", "Category")
```

## ORDER BY Design

The ORDER BY key determines which rows get merged together. Choose it carefully:

```csharp
// Per hour, per category
entity.UseSummingMergeTree(x => new { x.Hour, x.Category });

// Per day only (all categories merged together)
entity.UseSummingMergeTree(x => x.Day);
```

> **Note:** Summation only happens during background merges, not at insert time. Until a merge runs, duplicate keys may exist. Always use `GROUP BY` in queries to get correct results regardless of merge state:
>
> ```csharp
> context.HourlyStats
>     .GroupBy(s => new { s.Hour, s.Category })
>     .Select(g => new { g.Key.Hour, g.Key.Category, Views = g.Sum(s => s.Views) })
> ```

## With Partitioning

```csharp
modelBuilder.Entity<DailyMetrics>(entity =>
{
    entity.UseSummingMergeTree(x => new { x.Date, x.MetricName })
        .HasPartitionByMonth(x => x.Date);
});
```

```sql
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM("Date")
ORDER BY ("Date", "MetricName")
```

> **Note:** Merges only happen within a single partition. Rows with the same ORDER BY key in different partitions are never summed together.

## Complete Example

```csharp
public class PageStats
{
    public DateTime Hour { get; set; }
    public string PageUrl { get; set; } = string.Empty;
    public long UniqueVisitors { get; set; }
    public long PageViews { get; set; }
    public decimal TotalDuration { get; set; }
}

modelBuilder.Entity<PageStats>(entity =>
{
    entity.ToTable("page_stats");
    entity.HasNoKey();

    entity.UseSummingMergeTree(x => new { x.Hour, x.PageUrl })
        .HasPartitionByMonth(x => x.Hour)
        .HasTtl(x => x.Hour, ClickHouseInterval.Months(6));
});
```

```sql
CREATE TABLE "page_stats" (
    "Hour" DateTime64(3),
    "PageUrl" String,
    "UniqueVisitors" Int64,
    "PageViews" Int64,
    "TotalDuration" Decimal(18, 4)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM("Hour")
ORDER BY ("Hour", "PageUrl")
TTL "Hour" + INTERVAL 6 MONTH
```

Insert incremental counts:

```csharp
await context.BulkInsertAsync(new[]
{
    new PageStats { Hour = hour, PageUrl = "/home", PageViews = 1, UniqueVisitors = 1 },
    new PageStats { Hour = hour, PageUrl = "/home", PageViews = 1, UniqueVisitors = 1 },
});
// After merge, these collapse into PageViews = 2, UniqueVisitors = 2
```

## See Also

- [MergeTree](mergetree.md) -- base engine without automatic summation
- [AggregatingMergeTree](aggregating-mergetree.md) -- for complex aggregate functions beyond simple sums
- [Materialized Views](../features/materialized-views.md) -- combine with SummingMergeTree for real-time aggregation pipelines
