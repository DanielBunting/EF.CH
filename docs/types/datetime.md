# DateTime Types

ClickHouse has several date and time types. EF.CH maps .NET datetime types appropriately and provides helper functions for ClickHouse-specific operations.

## Type Mappings

| .NET Type | ClickHouse Type | Precision |
|-----------|-----------------|-----------|
| `DateTime` | `DateTime64(3)` | Milliseconds |
| `DateTimeOffset` | `DateTime64(3)` | Milliseconds + timezone |
| `DateOnly` | `Date` | Day |
| `TimeOnly` | `Time` | Microseconds |
| `TimeSpan` | `Int64` | Nanoseconds |

## Entity Definition

```csharp
public class Event
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }          // DateTime64(3)
    public DateTimeOffset CreatedAt { get; set; }    // DateTime64(3) with TZ
    public DateOnly EventDate { get; set; }          // Date
    public TimeOnly EventTime { get; set; }          // Time
    public TimeSpan Duration { get; set; }           // Int64 (nanoseconds)
}
```

## Configuration

DateTime types work without special configuration:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });
});
```

## Generated DDL

```sql
CREATE TABLE "Events" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "CreatedAt" DateTime64(3) NOT NULL,
    "EventDate" Date NOT NULL,
    "EventTime" Time NOT NULL,
    "Duration" Int64 NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Timestamp", "Id")
```

## Standard LINQ Operations

### DateTime Properties

```csharp
// Filter by year
var events2024 = await context.Events
    .Where(e => e.Timestamp.Year == 2024)
    .ToListAsync();
// SQL: ... WHERE toYear("Timestamp") = 2024

// Filter by month
var januaryEvents = await context.Events
    .Where(e => e.Timestamp.Month == 1)
    .ToListAsync();
// SQL: ... WHERE toMonth("Timestamp") = 1

// Filter by day
var day15 = await context.Events
    .Where(e => e.Timestamp.Day == 15)
    .ToListAsync();
```

### DateTime Arithmetic

```csharp
// Events in the last 24 hours
var recent = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddHours(-24))
    .ToListAsync();
// SQL: ... WHERE "Timestamp" > addHours(now(), -24)

// Events in the last 7 days
var lastWeek = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-7))
    .ToListAsync();
```

### Date Property

```csharp
// Get just the date part
var eventDates = await context.Events
    .Select(e => new { e.Id, Date = e.Timestamp.Date })
    .ToListAsync();
// SQL: ... toDate("Timestamp")
```

## ClickHouse-Specific Functions

Import the extensions:

```csharp
using EF.CH.Extensions;
```

### Time Truncation Functions

```csharp
// Truncate to start of hour
var hourlyGroups = await context.Events
    .GroupBy(e => e.Timestamp.ToStartOfHour())
    .Select(g => new { Hour = g.Key, Count = g.Count() })
    .ToListAsync();
// SQL: ... toStartOfHour("Timestamp")

// Truncate to start of day
var dailyGroups = await context.Events
    .GroupBy(e => e.Timestamp.ToStartOfDay())
    .Select(g => new { Day = g.Key, Count = g.Count() })
    .ToListAsync();
// SQL: ... toStartOfDay("Timestamp")

// Truncate to start of month
var monthlyGroups = await context.Events
    .GroupBy(e => e.Timestamp.ToStartOfMonth())
    .Select(g => new { Month = g.Key, Count = g.Count() })
    .ToListAsync();
// SQL: ... toStartOfMonth("Timestamp")
```

### Available Truncation Functions

| Method | ClickHouse Function | Use Case |
|--------|---------------------|----------|
| `ToStartOfMinute()` | `toStartOfMinute()` | Per-minute metrics |
| `ToStartOfFiveMinutes()` | `toStartOfFiveMinutes()` | 5-minute buckets |
| `ToStartOfFifteenMinutes()` | `toStartOfFifteenMinutes()` | 15-minute buckets |
| `ToStartOfHour()` | `toStartOfHour()` | Hourly aggregation |
| `ToStartOfDay()` | `toStartOfDay()` | Daily reports |
| `ToStartOfMonth()` | `toStartOfMonth()` | Monthly reports |
| `ToStartOfQuarter()` | `toStartOfQuarter()` | Quarterly reports |
| `ToStartOfYear()` | `toStartOfYear()` | Yearly reports |

### Date Formatting Functions

```csharp
// Get YYYYMM format
var monthKeys = await context.Events
    .Select(e => new { e.Id, Month = e.Timestamp.ToYYYYMM() })
    .ToListAsync();
// SQL: ... toYYYYMM("Timestamp")

// Get YYYYMMDD format
var dayKeys = await context.Events
    .Select(e => new { e.Id, Day = e.Timestamp.ToYYYYMMDD() })
    .ToListAsync();
// SQL: ... toYYYYMMDD("Timestamp")
```

### Week/Quarter Functions

```csharp
// ISO week number
var weekNum = e.Timestamp.ToISOWeek();    // toISOWeek()

// ISO year
var isoYear = e.Timestamp.ToISOYear();    // toISOYear()

// Day of week (1=Monday, 7=Sunday)
var dow = e.Timestamp.ToDayOfWeek();      // toDayOfWeek()

// Day of year (1-366)
var doy = e.Timestamp.ToDayOfYear();      // toDayOfYear()

// Quarter (1-4)
var quarter = e.Timestamp.ToQuarter();    // toQuarter()
```

## Querying Examples

### Time-Series Aggregation

```csharp
// Hourly event counts
var hourlyStats = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-1))
    .GroupBy(e => e.Timestamp.ToStartOfHour())
    .Select(g => new
    {
        Hour = g.Key,
        Count = g.Count()
    })
    .OrderBy(x => x.Hour)
    .ToListAsync();
```

### Daily Aggregation

```csharp
// Daily event summary
var dailyStats = await context.Events
    .Where(e => e.Timestamp > DateTime.UtcNow.AddDays(-30))
    .GroupBy(e => e.Timestamp.ToStartOfDay())
    .Select(g => new
    {
        Date = g.Key,
        Count = g.Count(),
        UniqueUsers = g.Select(e => e.UserId).Distinct().Count()
    })
    .ToListAsync();
```

### Monthly Trends

```csharp
// Monthly trends by category
var monthlyTrends = await context.Events
    .GroupBy(e => new
    {
        Month = e.Timestamp.ToYYYYMM(),
        e.EventType
    })
    .Select(g => new
    {
        g.Key.Month,
        g.Key.EventType,
        Count = g.Count()
    })
    .ToListAsync();
```

## Partitioning by Time

Use time-based partitioning for efficient data management:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Monthly partitions (most common)
    entity.HasPartitionByMonth(x => x.Timestamp);

    // Or daily for high-volume data
    // entity.HasPartitionByDay(x => x.Timestamp);

    // Or yearly for long-retention data
    // entity.HasPartitionByYear(x => x.Timestamp);
});
```

## Timezone Handling

### DateTimeOffset

Use `DateTimeOffset` when timezone matters:

```csharp
public class UserActivity
{
    public Guid UserId { get; set; }
    public DateTimeOffset ActivityTime { get; set; }  // Preserves timezone
}

// Insert with timezone
context.Activities.Add(new UserActivity
{
    UserId = Guid.NewGuid(),
    ActivityTime = DateTimeOffset.Now  // Local timezone preserved
});
```

### UTC Convention

For consistency, use UTC:

```csharp
// Always use UTC
Timestamp = DateTime.UtcNow

// Convert from local if needed
Timestamp = localTime.ToUniversalTime()
```

## Real-World Examples

### Metrics Table

```csharp
public class Metric
{
    public DateTime Timestamp { get; set; }
    public string MetricName { get; set; } = string.Empty;
    public double Value { get; set; }
    public Dictionary<string, string> Labels { get; set; } = new();
}

modelBuilder.Entity<Metric>(entity =>
{
    entity.HasNoKey();
    entity.UseMergeTree(x => new { x.Timestamp, x.MetricName });
    entity.HasPartitionByDay(x => x.Timestamp);
    entity.HasTtl("Timestamp + INTERVAL 90 DAY");
});
```

### Audit Log

```csharp
public class AuditEntry
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public DateOnly AuditDate { get; set; }  // For partitioning
    public string Action { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public string Details { get; set; } = string.Empty;
}
```

## Limitations

- **No TimeZoneInfo**: Timezone conversion should be done in application code
- **TimeSpan Precision**: Stored as nanoseconds, may lose precision on round-trip
- **No DateTimeKind**: ClickHouse doesn't preserve `DateTimeKind`

## Best Practices

### Use UTC

```csharp
// Good: Always UTC
Timestamp = DateTime.UtcNow

// Avoid: Local time causes timezone confusion
Timestamp = DateTime.Now
```

### Use DateOnly for Dates

```csharp
// Good: When you only need the date
public DateOnly OrderDate { get; set; }

// Avoid: DateTime when you don't need time
public DateTime OrderDate { get; set; }  // Wastes space
```

### Partition by Time

```csharp
// Good: Partition time-series data
entity.HasPartitionByMonth(x => x.Timestamp);

// With TTL for automatic cleanup
entity.HasTtl("Timestamp + INTERVAL 1 YEAR");
```

## See Also

- [Type Mappings Overview](overview.md)
- [Partitioning](../features/partitioning.md)
- [TTL](../features/ttl.md)
