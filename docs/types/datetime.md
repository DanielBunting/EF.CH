# DateTime Types

## CLR to ClickHouse Mapping

```
DateTime       --> DateTime64(3)
DateTimeOffset --> DateTime64(3, 'timezone')
DateOnly       --> Date
DateOnly       --> Date32  (extended range)
TimeOnly       --> Time
TimeSpan       --> Int64   (nanoseconds)
```

## DateTime

```csharp
public class Event
{
    public uint Id { get; set; }
    public DateTime CreatedAt { get; set; }
}
```

```sql
CREATE TABLE "Events" (
    "Id" UInt32,
    "CreatedAt" DateTime64(3)
) ENGINE = MergeTree() ORDER BY ("Id")
```

### Configurable Precision

The precision parameter controls sub-second decimal places (0-9):

| Precision | Resolution | Format Example |
|-----------|------------|----------------|
| 0 | Seconds | `2024-01-15 10:30:00` |
| 3 | Milliseconds | `2024-01-15 10:30:00.123` |
| 6 | Microseconds | `2024-01-15 10:30:00.123456` |
| 9 | Nanoseconds | `2024-01-15 10:30:00.123456789` |

```csharp
entity.Property(x => x.CreatedAt).HasPrecision(6);
// DDL: "CreatedAt" DateTime64(6)
```

### DateTime with Timezone

```csharp
// DateTime64 with explicit timezone
entity.Property(x => x.CreatedAt).HasColumnType("DateTime64(3, 'UTC')");
```

## DateTimeOffset

`DateTimeOffset` maps to `DateTime64` with a required IANA timezone. Values are always stored as UTC in ClickHouse; the timezone determines how the offset is calculated when reading.

```csharp
public class ScheduledEvent
{
    public uint Id { get; set; }
    public DateTimeOffset ScheduledAt { get; set; }
}

// Configuration
entity.Property(x => x.ScheduledAt)
    .HasTimeZone("America/New_York");
```

```sql
CREATE TABLE "ScheduledEvents" (
    "Id" UInt32,
    "ScheduledAt" DateTime64(3, 'America/New_York')
) ENGINE = MergeTree() ORDER BY ("Id")
```

The value converter handles timezone conversion:

- **Write path**: Converts any `DateTimeOffset` to UTC before storage.
- **Read path**: Reads the UTC value and converts to a `DateTimeOffset` with the correct offset for the configured timezone, properly accounting for DST transitions.

```csharp
// If timezone is America/New_York:
// Writing: 2024-07-15 14:00:00 -04:00 --> stored as 2024-07-15 18:00:00 UTC
// Reading: 2024-07-15 18:00:00 UTC --> returned as 2024-07-15 14:00:00 -04:00 (EDT)
// Reading: 2024-01-15 18:00:00 UTC --> returned as 2024-01-15 13:00:00 -05:00 (EST)
```

## DateOnly

```csharp
public class Person
{
    public uint Id { get; set; }
    public DateOnly BirthDate { get; set; }
}
```

```sql
"BirthDate" Date
```

For dates outside the `Date` range (1970-2149), use `Date32`:

```csharp
entity.Property(x => x.BirthDate).HasColumnType("Date32");
// DDL: "BirthDate" Date32  -- range: 1900-01-01 to 2299-12-31
```

## TimeOnly

```csharp
public TimeOnly OpenTime { get; set; }
// DDL: "OpenTime" Time
```

ClickHouse `Time` stores time-of-day with nanosecond precision. SQL literals use `HH:mm:ss` or `HH:mm:ss.ffffff` format.

## TimeSpan

ClickHouse has no native duration type. `TimeSpan` is stored as `Int64` nanoseconds through a built-in value converter:

```csharp
public TimeSpan Duration { get; set; }
// DDL: "Duration" Int64
```

The converter multiplies `TimeSpan.Ticks` (100-nanosecond units) by 100 to get nanoseconds:

- **Write**: `TimeSpan.FromHours(1)` --> `3600000000000` nanoseconds
- **Read**: `3600000000000` nanoseconds --> `TimeSpan.FromHours(1)`

## DateTime Member Translations

### Property Access

```csharp
context.Events.Select(e => e.CreatedAt.Year)
```

```sql
SELECT toYear("CreatedAt") FROM "Events"
```

| C# Member | ClickHouse SQL |
|-----------|----------------|
| `.Year` | `toYear(col)` |
| `.Month` | `toMonth(col)` |
| `.Day` | `toDayOfMonth(col)` |
| `.Hour` | `toHour(col)` |
| `.Minute` | `toMinute(col)` |
| `.Second` | `toSecond(col)` |
| `.Millisecond` | `toMillisecond(col)` |
| `.DayOfYear` | `toDayOfYear(col)` |
| `.DayOfWeek` | `toDayOfWeek(col) % 7` |
| `.Date` | `toDate(col)` |
| `.Ticks` | `toUnixTimestamp64Milli(col) * 10000 + 621355968000000000` |

> **Note:** `.DayOfWeek` requires a modulo operation because ClickHouse uses Monday=1 through Sunday=7, while .NET uses Sunday=0 through Saturday=6. The formula `toDayOfWeek(x) % 7` converts correctly.

### Static Members

```csharp
context.Events.Select(e => DateTime.Now)     // now()
context.Events.Select(e => DateTime.UtcNow)  // now('UTC')
context.Events.Select(e => DateTime.Today)   // today()
```

### Add Methods

```csharp
context.Events.Select(e => e.CreatedAt.AddDays(7))
```

```sql
SELECT addDays("CreatedAt", 7) FROM "Events"
```

| C# Method | ClickHouse SQL |
|-----------|----------------|
| `.AddYears(n)` | `addYears(col, n)` |
| `.AddMonths(n)` | `addMonths(col, n)` |
| `.AddDays(n)` | `addDays(col, n)` |
| `.AddHours(n)` | `addHours(col, n)` |
| `.AddMinutes(n)` | `addMinutes(col, n)` |
| `.AddSeconds(n)` | `addSeconds(col, n)` |
| `.AddMilliseconds(n)` | `addMilliseconds(col, toInt64(n))` |
| `.AddTicks(n)` | `addMilliseconds(col, n / 10000)` |

These translations work on `DateTime`, `DateTimeOffset`, and `DateOnly` types.

## DateOnly Member Translations

`DateOnly` supports the same date-part extractions:

| C# Member | ClickHouse SQL |
|-----------|----------------|
| `.Year` | `toYear(col)` |
| `.Month` | `toMonth(col)` |
| `.Day` | `toDayOfMonth(col)` |
| `.DayOfYear` | `toDayOfYear(col)` |
| `.DayOfWeek` | `toDayOfWeek(col) % 7` |

## See Also

- [Type System Overview](overview.md)
