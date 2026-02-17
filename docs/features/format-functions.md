# Formatting & Parsing Functions

ClickHouse provides functions for formatting dates, numbers, and byte sizes into human-readable strings, and for parsing strings back into dates. EF.CH exposes these as `EF.Functions` extensions.

## Available Functions

### Formatting

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `FormatDateTime(dt, fmt)` | `formatDateTime(dt, fmt)` | Format a DateTime with a format string |
| `FormatReadableSize(bytes)` | `formatReadableSize(bytes)` | Bytes → human-readable (e.g. `"1.00 GiB"`) |
| `FormatReadableDecimalSize(bytes)` | `formatReadableDecimalSize(bytes)` | Bytes → decimal units (e.g. `"1.00 GB"`) |
| `FormatReadableQuantity(n)` | `formatReadableQuantity(n)` | Number → readable (e.g. `"1.50 million"`) |
| `FormatReadableTimeDelta(seconds)` | `formatReadableTimeDelta(seconds)` | Seconds → time string (e.g. `"1 hour, 30 minutes"`) |

### Parsing

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `ParseDateTime(s, fmt)` | `parseDateTime(s, fmt)` | `DateTime` | Parse a string using a format |

## Format String Reference

ClickHouse uses `%`-based format specifiers (similar to `strftime`):

| Specifier | Meaning | Example |
|-----------|---------|---------|
| `%Y` | 4-digit year | `2026` |
| `%m` | Month (01-12) | `02` |
| `%d` | Day (01-31) | `17` |
| `%H` | Hour 24h (00-23) | `14` |
| `%i` | Minute (00-59) | `30` |
| `%S` | Second (00-59) | `05` |
| `%F` | Date (`%Y-%m-%d`) | `2026-02-17` |
| `%T` | Time (`%H:%i:%S`) | `14:30:05` |

> **Note:** `%M` is the full month name (e.g. `February`), not minutes. Use `%i` for minutes.

## Usage Examples

### Custom Date Formatting

```csharp
using EF.CH.Extensions;

var formatted = await context.Orders
    .Select(o => new
    {
        o.Id,
        OrderDate = EF.Functions.FormatDateTime(o.CreatedAt, "%Y-%m-%d %H:%i")
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Id", formatDateTime("CreatedAt", '%Y-%m-%d %H:%i') AS "OrderDate"
FROM "Orders"
```

### Human-Readable File Sizes

```csharp
var files = await context.Uploads
    .Select(u => new
    {
        u.FileName,
        Size = EF.Functions.FormatReadableSize(u.SizeBytes)
    })
    .ToListAsync();
// Result: [{ FileName: "backup.tar.gz", Size: "2.30 GiB" }, ...]
```

### Dashboard Display Values

```csharp
var stats = await context.Metrics
    .GroupBy(m => 1) // single group
    .Select(g => new
    {
        TotalEvents = EF.Functions.FormatReadableQuantity((double)g.Count()),
        TotalBytes = EF.Functions.FormatReadableSize(g.Sum(m => m.BytesProcessed)),
        AvgDuration = EF.Functions.FormatReadableTimeDelta(g.Average(m => m.DurationSeconds))
    })
    .FirstAsync();
// Result: { TotalEvents: "12.50 million", TotalBytes: "1.23 TiB", AvgDuration: "2 minutes, 15 seconds" }
```

### Parsing Date Strings

```csharp
// Parse date strings from imported data
var parsed = await context.RawEvents
    .Select(e => new
    {
        e.Id,
        Timestamp = EF.Functions.ParseDateTime(e.DateString, "%Y-%m-%d %H:%i:%S")
    })
    .ToListAsync();
```

## Notes

- `FormatReadableSize` uses binary units (KiB, MiB, GiB). Use `FormatReadableDecimalSize` for decimal units (KB, MB, GB).
- `FormatReadableQuantity` takes `double`, not `long` — cast if needed.
- `FormatReadableTimeDelta` takes seconds as `double`.
- `ParseDateTime` throws a ClickHouse exception if the string doesn't match the format.

## Learn More

- [ClickHouse formatDateTime](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#formatdatetime)
- [ClickHouse Other Functions](https://clickhouse.com/docs/en/sql-reference/functions/other-functions)
