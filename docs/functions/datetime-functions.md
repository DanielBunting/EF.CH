# DateTime Functions

EF.CH translates .NET `DateTime`, `DateOnly`, and related member accesses and method calls into ClickHouse date/time functions. This includes property accessors (`.Year`, `.Month`, etc.), arithmetic methods (`.AddDays()`, etc.), static members (`DateTime.Now`), and ClickHouse-specific date truncation functions.

---

## DateTime Member Translations

Standard .NET `DateTime` property accesses are translated to ClickHouse extraction functions.

| C# Member | ClickHouse SQL | Return Type |
|-----------|----------------|-------------|
| `.Year` | `toYear(column)` | `int` |
| `.Month` | `toMonth(column)` | `int` |
| `.Day` | `toDayOfMonth(column)` | `int` |
| `.Hour` | `toHour(column)` | `int` |
| `.Minute` | `toMinute(column)` | `int` |
| `.Second` | `toSecond(column)` | `int` |
| `.Millisecond` | `toMillisecond(column)` | `int` |
| `.DayOfYear` | `toDayOfYear(column)` | `int` |
| `.DayOfWeek` | `toDayOfWeek(column) % 7` | `DayOfWeek` |
| `.Date` | `toDate(column)` | `DateTime` |
| `.Ticks` | `toUnixTimestamp64Milli(column) * 10000 + 621355968000000000` | `long` |

```csharp
var results = await context.Events
    .Select(e => new
    {
        Year = e.Timestamp.Year,           // toYear(Timestamp)
        Month = e.Timestamp.Month,         // toMonth(Timestamp)
        Day = e.Timestamp.Day,             // toDayOfMonth(Timestamp)
        Hour = e.Timestamp.Hour,           // toHour(Timestamp)
        DayOfWeek = e.Timestamp.DayOfWeek, // toDayOfWeek(Timestamp) % 7
        DateOnly = e.Timestamp.Date        // toDate(Timestamp)
    })
    .ToListAsync();
```

---

## DateTime Static Members

| C# Expression | ClickHouse SQL | Notes |
|---------------|----------------|-------|
| `DateTime.Now` | `now()` | Current server-local time |
| `DateTime.UtcNow` | `now('UTC')` | Current UTC time |
| `DateTime.Today` | `today()` | Current date (no time component) |

```csharp
var recentEvents = await context.Events
    .Where(e => e.Timestamp >= DateTime.UtcNow.AddHours(-1))  // now('UTC') + addHours(...)
    .ToListAsync();

var todayEvents = await context.Events
    .Where(e => e.Timestamp >= DateTime.Today)                // today()
    .ToListAsync();
```

---

## DateTime Arithmetic Methods

The `.Add*()` methods on `DateTime` translate to ClickHouse `add*()` functions.

| C# Method | ClickHouse SQL |
|-----------|----------------|
| `.AddYears(n)` | `addYears(column, n)` |
| `.AddMonths(n)` | `addMonths(column, n)` |
| `.AddDays(n)` | `addDays(column, n)` |
| `.AddHours(n)` | `addHours(column, n)` |
| `.AddMinutes(n)` | `addMinutes(column, n)` |
| `.AddSeconds(n)` | `addSeconds(column, n)` |
| `.AddMilliseconds(n)` | `addMilliseconds(column, n)` |
| `.AddTicks(n)` | `addMilliseconds(column, n / 10000)` |

```csharp
var results = await context.Subscriptions
    .Select(s => new
    {
        s.UserId,
        StartDate = s.CreatedAt,
        ExpiryDate = s.CreatedAt.AddMonths(12),      // addMonths(CreatedAt, 12)
        GracePeriod = s.CreatedAt.AddDays(30),        // addDays(CreatedAt, 30)
        NextCheckIn = s.LastSeen.AddHours(1)          // addHours(LastSeen, 1)
    })
    .ToListAsync();
```

---

## DateOnly Member Translations

`DateOnly` properties translate to the same ClickHouse functions as their `DateTime` counterparts.

| C# Member | ClickHouse SQL |
|-----------|----------------|
| `.Year` | `toYear(column)` |
| `.Month` | `toMonth(column)` |
| `.Day` | `toDayOfMonth(column)` |
| `.DayOfYear` | `toDayOfYear(column)` |
| `.DayOfWeek` | `toDayOfWeek(column) % 7` |

```csharp
var results = await context.Reports
    .Select(r => new
    {
        Year = r.ReportDate.Year,          // toYear(ReportDate)
        Quarter = r.ReportDate.Month / 4 + 1,
        IsWeekend = r.ReportDate.DayOfWeek == DayOfWeek.Saturday
                 || r.ReportDate.DayOfWeek == DayOfWeek.Sunday
    })
    .ToListAsync();
```

---

## Date Truncation Functions

Date truncation functions are available in two forms:
1. **Extension methods** on `DateTime` via `ClickHouseFunctions` (e.g., `timestamp.ToStartOfHour()`)
2. **EF.Functions methods** via `ClickHouseDateTruncDbFunctionsExtensions` (e.g., `EF.Functions.ToStartOfHour(timestamp)`)

Both forms produce identical SQL.

| C# Method | ClickHouse SQL | Truncates To |
|-----------|----------------|--------------|
| `.ToStartOfYear()` / `EF.Functions.ToStartOfYear(dt)` | `toStartOfYear(column)` | First day of the year |
| `.ToStartOfQuarter()` / `EF.Functions.ToStartOfQuarter(dt)` | `toStartOfQuarter(column)` | First day of the quarter |
| `.ToStartOfMonth()` / `EF.Functions.ToStartOfMonth(dt)` | `toStartOfMonth(column)` | First day of the month |
| `.ToStartOfWeek()` / `EF.Functions.ToStartOfWeek(dt)` | `toStartOfWeek(column)` | Start of the week (Sunday) |
| `EF.Functions.ToMonday(dt)` | `toMonday(column)` | Nearest Monday |
| `.ToStartOfDay()` / `EF.Functions.ToStartOfDay(dt)` | `toStartOfDay(column)` | Start of the day (midnight) |
| `.ToStartOfHour()` / `EF.Functions.ToStartOfHour(dt)` | `toStartOfHour(column)` | Start of the hour |
| `.ToStartOfMinute()` / `EF.Functions.ToStartOfMinute(dt)` | `toStartOfMinute(column)` | Start of the minute |
| `.ToStartOfFiveMinutes()` / `EF.Functions.ToStartOfFiveMinutes(dt)` | `toStartOfFiveMinutes(column)` | Nearest 5-minute boundary |
| `.ToStartOfFifteenMinutes()` / `EF.Functions.ToStartOfFifteenMinutes(dt)` | `toStartOfFifteenMinutes(column)` | Nearest 15-minute boundary |

### Time-Series Aggregation

Date truncation is commonly used as a GROUP BY key for time-series aggregation:

```csharp
// Hourly event counts
var hourly = await context.Events
    .GroupBy(e => e.Timestamp.ToStartOfHour())
    .Select(g => new
    {
        Hour = g.Key,                              // toStartOfHour(Timestamp)
        Count = g.Count()
    })
    .ToListAsync();

// 5-minute granularity metrics
var fiveMin = await context.Metrics
    .GroupBy(m => m.Timestamp.ToStartOfFiveMinutes())
    .Select(g => new
    {
        Bucket = g.Key,                            // toStartOfFiveMinutes(Timestamp)
        AvgValue = g.Average(m => m.Value)
    })
    .ToListAsync();

// Monthly rollup
var monthly = await context.Orders
    .GroupBy(o => o.CreatedAt.ToStartOfMonth())
    .Select(g => new
    {
        Month = g.Key,                             // toStartOfMonth(CreatedAt)
        Revenue = g.Sum(o => o.Amount)
    })
    .ToListAsync();
```

---

## DateDiff

Calculate the difference between two dates in a specified unit.

| C# Method | ClickHouse SQL |
|-----------|----------------|
| `EF.Functions.DateDiff("day", start, end)` | `dateDiff('day', start, end)` |

Valid units: `'second'`, `'minute'`, `'hour'`, `'day'`, `'week'`, `'month'`, `'quarter'`, `'year'`.

```csharp
var results = await context.Orders
    .Select(o => new
    {
        o.Id,
        DaysToShip = EF.Functions.DateDiff("day", o.OrderDate, o.ShipDate),
        HoursOpen = EF.Functions.DateDiff("hour", o.CreatedAt, o.ClosedAt)
    })
    .ToListAsync();
```

---

## Additional Date Extraction Functions

Extension methods on `DateTime` for ClickHouse-specific date extraction via `ClickHouseFunctions`.

| C# Method | ClickHouse SQL | Notes |
|-----------|----------------|-------|
| `dt.ToYYYYMM()` | `toYYYYMM(column)` | Integer in YYYYMM format (e.g., 202601) |
| `dt.ToYYYYMMDD()` | `toYYYYMMDD(column)` | Integer in YYYYMMDD format (e.g., 20260115) |
| `dt.ToISOYear()` | `toISOYear(column)` | ISO year number |
| `dt.ToISOWeek()` | `toISOWeek(column)` | ISO week number (1-53) |
| `dt.ToDayOfWeek()` | `toDayOfWeek(column)` | Day of week (1=Monday, 7=Sunday) |
| `dt.ToDayOfYear()` | `toDayOfYear(column)` | Day of year (1-366) |
| `dt.ToQuarter()` | `toQuarter(column)` | Quarter number (1-4) |
| `dt.ToUnixTimestamp64Milli()` | `toUnixTimestamp64Milli(column)` | Unix timestamp in milliseconds |

```csharp
var results = await context.Events
    .Select(e => new
    {
        YearMonth = e.Timestamp.ToYYYYMM(),          // toYYYYMM(Timestamp)
        DateInt = e.Timestamp.ToYYYYMMDD(),           // toYYYYMMDD(Timestamp)
        IsoWeek = e.Timestamp.ToISOWeek(),            // toISOWeek(Timestamp)
        Quarter = e.Timestamp.ToQuarter(),            // toQuarter(Timestamp)
        UnixMs = e.Timestamp.ToUnixTimestamp64Milli() // toUnixTimestamp64Milli(Timestamp)
    })
    .ToListAsync();
```

---

## Nullable DateTime Support

Date truncation and extraction methods also support nullable `DateTime?` values. The result type becomes nullable to match.

```csharp
var results = await context.Orders
    .Select(o => new
    {
        // ShippedAt is DateTime? - results are also nullable
        ShipMonth = o.ShippedAt.ToStartOfMonth(),     // toStartOfMonth(ShippedAt) -- returns DateTime?
        ShipYYMM = o.ShippedAt.ToYYYYMM()             // toYYYYMM(ShippedAt) -- returns int?
    })
    .ToListAsync();
```

---

## See Also

- [Aggregate Functions](aggregate-functions.md) -- aggregate functions commonly paired with date truncation GROUP BY
- [Utility Functions](utility-functions.md) -- FormatDateTime and ParseDateTime formatting functions
- [Math Functions](math-functions.md) -- mathematical functions for computed date expressions
