# UtilityFunctionsSample

Demonstrates ClickHouse utility functions translated from LINQ via `EF.Functions`.

## What This Shows

- **Date truncation**: `ToStartOfHour`, `ToStartOfDay`, `ToStartOfMonth`, `DateDiff` — time-series bucketing
- **Null handling**: `IfNull`, `Coalesce`, `IsNull`, `IsNotNull` — null-safe operations
- **String distance**: `LevenshteinDistance`, `JaroWinklerSimilarity` — fuzzy string matching
- **URL parsing**: `Domain`, `Protocol`, `UrlPath`, `ExtractURLParameters` — decompose URLs
- **Hashing**: `CityHash64`, `Md5`, `Sha256` — hash values server-side
- **Formatting**: `FormatDateTime`, `FormatReadableSize` — human-readable output
- **IP addresses**: `IsIPAddressInRange`, `IsIPv4String` — IP filtering and validation
- **Encoding**: `Base64Encode`, `Hex` — encode/decode values
- **Type checking**: `IsNaN`, `IsFinite` — validate numeric data
- **String split/join**: `SplitByChar`, `ArrayStringConcat` — split and rejoin strings
- **UUID v7**: `NewGuidV7` — time-sortable UUID generation

## Function Reference

### Date Truncation

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `ToStartOfYear(dt)` | `toStartOfYear(dt)` | Round down to year start |
| `ToStartOfQuarter(dt)` | `toStartOfQuarter(dt)` | Round down to quarter start |
| `ToStartOfMonth(dt)` | `toStartOfMonth(dt)` | Round down to month start |
| `ToStartOfWeek(dt)` | `toStartOfWeek(dt)` | Round down to week start (Sunday) |
| `ToMonday(dt)` | `toMonday(dt)` | Round down to Monday |
| `ToStartOfDay(dt)` | `toStartOfDay(dt)` | Round down to day start |
| `ToStartOfHour(dt)` | `toStartOfHour(dt)` | Round down to hour start |
| `ToStartOfMinute(dt)` | `toStartOfMinute(dt)` | Round down to minute start |
| `ToStartOfFiveMinutes(dt)` | `toStartOfFiveMinutes(dt)` | Round down to 5-min interval |
| `ToStartOfFifteenMinutes(dt)` | `toStartOfFifteenMinutes(dt)` | Round down to 15-min interval |
| `DateDiff(unit, start, end)` | `dateDiff(unit, start, end)` | Difference in given unit |

### Null Handling

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `IfNull(value, default)` | `ifNull(x, default)` | Return default if null |
| `NullIf(value, compare)` | `nullIf(x, y)` | Return null if equal |
| `AssumeNotNull(value)` | `assumeNotNull(x)` | Treat as non-nullable |
| `Coalesce(a, b)` | `coalesce(a, b)` | First non-null value |
| `Coalesce(a, b, c)` | `coalesce(a, b, c)` | First non-null value |
| `IsNull(value)` | `isNull(x)` | True if null |
| `IsNotNull(value)` | `isNotNull(x)` | True if not null |

### String Distance

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `LevenshteinDistance(s1, s2)` | `levenshteinDistance(s1, s2)` | Edit distance |
| `LevenshteinDistanceUTF8(s1, s2)` | `levenshteinDistanceUTF8(s1, s2)` | UTF-8 aware edit distance |
| `DamerauLevenshteinDistance(s1, s2)` | `damerauLevenshteinDistance(s1, s2)` | Edit distance with transpositions |
| `JaroSimilarity(s1, s2)` | `jaroSimilarity(s1, s2)` | Jaro similarity (0..1) |
| `JaroWinklerSimilarity(s1, s2)` | `jaroWinklerSimilarity(s1, s2)` | Jaro-Winkler similarity (0..1) |

### URL Parsing

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `Domain(url)` | `domain(url)` | Extract domain |
| `DomainWithoutWWW(url)` | `domainWithoutWWW(url)` | Domain without www prefix |
| `TopLevelDomain(url)` | `topLevelDomain(url)` | Extract TLD |
| `Protocol(url)` | `protocol(url)` | Extract protocol |
| `UrlPath(url)` | `path(url)` | Extract path |
| `ExtractURLParameter(url, name)` | `extractURLParameter(url, name)` | Get query param value |
| `ExtractURLParameters(url)` | `extractURLParameters(url)` | Get all query params |
| `CutURLParameter(url, name)` | `cutURLParameter(url, name)` | Remove a query param |
| `DecodeURLComponent(s)` | `decodeURLComponent(s)` | URL-decode |
| `EncodeURLComponent(s)` | `encodeURLComponent(s)` | URL-encode |

### Hashing

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `CityHash64(value)` | `cityHash64(x)` | CityHash64 |
| `SipHash64(value)` | `sipHash64(x)` | SipHash64 |
| `XxHash64(value)` | `xxHash64(x)` | xxHash64 |
| `MurmurHash3_64(value)` | `murmurHash3_64(x)` | MurmurHash3 64-bit |
| `FarmHash64(value)` | `farmHash64(x)` | FarmHash64 |
| `Md5(value)` | `hex(MD5(x))` | MD5 as hex string |
| `Sha256(value)` | `hex(SHA256(x))` | SHA-256 as hex string |
| `ConsistentHash(hash, buckets)` | `yandexConsistentHash(hash, n)` | Consistent hash to bucket |

### Formatting

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `FormatDateTime(dt, fmt)` | `formatDateTime(dt, fmt)` | Format date/time |
| `FormatReadableSize(bytes)` | `formatReadableSize(bytes)` | e.g. "1.00 GiB" |
| `FormatReadableDecimalSize(bytes)` | `formatReadableDecimalSize(bytes)` | e.g. "1.00 GB" |
| `FormatReadableQuantity(n)` | `formatReadableQuantity(n)` | e.g. "1.00 million" |
| `FormatReadableTimeDelta(sec)` | `formatReadableTimeDelta(sec)` | e.g. "1 hour, 30 minutes" |
| `ParseDateTime(s, fmt)` | `parseDateTime(s, fmt)` | Parse string to DateTime |

### IP Address

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `IPv4NumToString(ip)` | `IPv4NumToString(ip)` | UInt32 to dotted string |
| `IPv4StringToNum(s)` | `IPv4StringToNum(s)` | Dotted string to UInt32 |
| `IsIPAddressInRange(addr, cidr)` | `isIPAddressInRange(addr, cidr)` | Check if in CIDR range |
| `IsIPv4String(s)` | `isIPv4String(s)` | Validate IPv4 string |
| `IsIPv6String(s)` | `isIPv6String(s)` | Validate IPv6 string |

### Encoding

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `Base64Encode(s)` | `base64Encode(s)` | Encode to Base64 |
| `Base64Decode(s)` | `base64Decode(s)` | Decode from Base64 |
| `Hex(value)` | `hex(x)` | Hex representation |
| `Unhex(s)` | `unhex(s)` | Hex string to bytes |

### Type Checking

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `IsNaN(value)` | `isNaN(x)` | Check for NaN |
| `IsFinite(value)` | `isFinite(x)` | Check for finite value |
| `IsInfinite(value)` | `isInfinite(x)` | Check for infinity |

### String Split / Join

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `SplitByChar(sep, s)` | `splitByChar(sep, s)` | Split by single char |
| `SplitByString(sep, s)` | `splitByString(sep, s)` | Split by string |
| `ArrayStringConcat(arr)` | `arrayStringConcat(arr)` | Join array elements |
| `ArrayStringConcat(arr, sep)` | `arrayStringConcat(arr, sep)` | Join with separator |

### UUID

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `NewGuidV7()` | `generateUUIDv7()` | Time-sortable UUID v7 |

## Prerequisites

- .NET 8.0+
- ClickHouse server running on localhost:8123

## Running

```bash
dotnet run
```

## Key Code

### Time-Series Bucketing

```csharp
// Group page views by hour
var hourly = await context.PageViews
    .GroupBy(p => EF.Functions.ToStartOfHour(p.ViewedAt))
    .Select(g => new { Hour = g.Key, Count = g.Count() })
    .ToListAsync();

// How many days since each event
var ages = await context.PageViews
    .Select(p => new
    {
        p.UserName,
        DaysAgo = EF.Functions.DateDiff("day", p.ViewedAt, DateTime.UtcNow)
    })
    .ToListAsync();
```

### URL Analytics

```csharp
// Extract domains and query parameters
var analytics = await context.PageViews
    .Select(p => new
    {
        Domain = EF.Functions.DomainWithoutWWW(p.Url),
        Path = EF.Functions.UrlPath(p.Url),
        Ref = EF.Functions.ExtractURLParameter(p.Url, "ref")
    })
    .ToListAsync();
```

### IP Filtering

```csharp
// Find requests from a specific subnet
var internal = await context.PageViews
    .Where(p => EF.Functions.IsIPAddressInRange(p.IpAddress, "10.0.0.0/8"))
    .ToListAsync();
```

### Fuzzy String Matching

```csharp
// Find users with names similar to a search term
var matches = await context.Users
    .Where(u => EF.Functions.JaroWinklerSimilarity(u.Name, "alice") > 0.8)
    .ToListAsync();
```

## Learn More

- [ClickHouse Date Functions](https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions)
- [ClickHouse URL Functions](https://clickhouse.com/docs/en/sql-reference/functions/url-functions)
- [ClickHouse Hash Functions](https://clickhouse.com/docs/en/sql-reference/functions/hash-functions)
- [ClickHouse IP Functions](https://clickhouse.com/docs/en/sql-reference/functions/ip-address-functions)
