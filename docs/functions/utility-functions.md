# Utility Functions

EF.CH exposes a broad set of ClickHouse utility functions through `EF.Functions` extension methods. These cover null handling, string distance/similarity, URL parsing, hashing, formatting, IP address operations, encoding, type checking, and UUID generation.

All utility functions are LINQ translation stubs accessed via `EF.Functions.*` and will throw if invoked outside of a query context.

---

## Null Functions

Explicit null-handling functions from `ClickHouseNullDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.IfNull(value, default)` | `ifNull(value, default)` | `T` | Returns default if value is NULL |
| `EF.Functions.NullIf(value, compare)` | `nullIf(value, compare)` | `T?` | Returns NULL if value equals compare |
| `EF.Functions.AssumeNotNull(value)` | `assumeNotNull(value)` | `T` | Treat nullable as non-nullable (undefined behavior if NULL) |
| `EF.Functions.Coalesce(a, b)` | `coalesce(a, b)` | `T` | First non-NULL argument (2-arg) |
| `EF.Functions.Coalesce(a, b, c)` | `coalesce(a, b, c)` | `T` | First non-NULL argument (3-arg) |
| `EF.Functions.IsNull(value)` | `isNull(value)` | `bool` | True if value is NULL |
| `EF.Functions.IsNotNull(value)` | `isNotNull(value)` | `bool` | True if value is not NULL |

```csharp
var results = await context.Users
    .Select(u => new
    {
        DisplayName = EF.Functions.IfNull(u.Nickname, u.FullName),  // ifNull(Nickname, FullName)
        CleanStatus = EF.Functions.NullIf(u.Status, ""),            // nullIf(Status, '')
        HasEmail = EF.Functions.IsNotNull(u.Email),                 // isNotNull(Email)
        BestContact = EF.Functions.Coalesce(u.Phone, u.Email, "N/A") // coalesce(Phone, Email, 'N/A')
    })
    .ToListAsync();
```

---

## String Distance and Similarity

String comparison functions from `ClickHouseStringDistanceDbFunctionsExtensions`. Useful for fuzzy matching, deduplication, and search relevance.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.LevenshteinDistance(s1, s2)` | `levenshteinDistance(s1, s2)` | `ulong` | Minimum single-character edits |
| `EF.Functions.LevenshteinDistanceUTF8(s1, s2)` | `levenshteinDistanceUTF8(s1, s2)` | `ulong` | UTF-8 aware Levenshtein distance |
| `EF.Functions.DamerauLevenshteinDistance(s1, s2)` | `damerauLevenshteinDistance(s1, s2)` | `ulong` | Levenshtein + transpositions |
| `EF.Functions.JaroSimilarity(s1, s2)` | `jaroSimilarity(s1, s2)` | `double` | Jaro similarity (0..1) |
| `EF.Functions.JaroWinklerSimilarity(s1, s2)` | `jaroWinklerSimilarity(s1, s2)` | `double` | Jaro-Winkler similarity (0..1) |

```csharp
// Find similar product names
var similar = await context.Products
    .Where(p => EF.Functions.LevenshteinDistance(p.Name, "Widget Pro") <= 3)
    .OrderBy(p => EF.Functions.LevenshteinDistance(p.Name, "Widget Pro"))
    .ToListAsync();

// Fuzzy name matching with similarity score
var matches = await context.Customers
    .Select(c => new
    {
        c.Name,
        Similarity = EF.Functions.JaroWinklerSimilarity(c.Name, searchTerm)
    })
    .Where(x => x.Similarity > 0.8)
    .OrderByDescending(x => x.Similarity)
    .ToListAsync();
```

---

## URL Parsing

URL component extraction functions from `ClickHouseUrlDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.Domain(url)` | `domain(url)` | `string` | Extract domain |
| `EF.Functions.DomainWithoutWWW(url)` | `domainWithoutWWW(url)` | `string` | Extract domain, strip "www." |
| `EF.Functions.TopLevelDomain(url)` | `topLevelDomain(url)` | `string` | Extract TLD (e.g., "com") |
| `EF.Functions.Protocol(url)` | `protocol(url)` | `string` | Extract protocol (e.g., "https") |
| `EF.Functions.UrlPath(url)` | `path(url)` | `string` | Extract URL path |
| `EF.Functions.ExtractURLParameter(url, name)` | `extractURLParameter(url, name)` | `string` | Extract named query parameter |
| `EF.Functions.ExtractURLParameters(url)` | `extractURLParameters(url)` | `string[]` | Extract all query parameters |
| `EF.Functions.CutURLParameter(url, name)` | `cutURLParameter(url, name)` | `string` | Remove named query parameter |
| `EF.Functions.DecodeURLComponent(s)` | `decodeURLComponent(s)` | `string` | URL-decode a string |
| `EF.Functions.EncodeURLComponent(s)` | `encodeURLComponent(s)` | `string` | URL-encode a string |

```csharp
// Analyze referrer URLs
var referrerStats = await context.PageViews
    .GroupBy(v => EF.Functions.DomainWithoutWWW(v.ReferrerUrl))
    .Select(g => new
    {
        Domain = g.Key,                            // domainWithoutWWW(ReferrerUrl)
        ViewCount = g.Count()
    })
    .OrderByDescending(x => x.ViewCount)
    .Take(20)
    .ToListAsync();

// Extract campaign tracking parameters
var campaigns = await context.PageViews
    .Select(v => new
    {
        Url = v.PageUrl,
        Campaign = EF.Functions.ExtractURLParameter(v.PageUrl, "utm_campaign"),
        Source = EF.Functions.ExtractURLParameter(v.PageUrl, "utm_source"),
        Path = EF.Functions.UrlPath(v.PageUrl)
    })
    .Where(x => x.Campaign != "")
    .ToListAsync();
```

---

## Hash Functions

Cryptographic and non-cryptographic hash functions from `ClickHouseHashDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.CityHash64(value)` | `cityHash64(value)` | `ulong` | Google CityHash 64-bit |
| `EF.Functions.SipHash64(value)` | `sipHash64(value)` | `ulong` | SipHash 64-bit |
| `EF.Functions.XxHash64(value)` | `xxHash64(value)` | `ulong` | xxHash 64-bit |
| `EF.Functions.MurmurHash3_64(value)` | `murmurHash3_64(value)` | `ulong` | MurmurHash3 64-bit |
| `EF.Functions.FarmHash64(value)` | `farmHash64(value)` | `ulong` | Google FarmHash 64-bit |
| `EF.Functions.Md5(value)` | `hex(MD5(value))` | `string` | MD5 hash as hex string |
| `EF.Functions.Sha256(value)` | `hex(SHA256(value))` | `string` | SHA-256 hash as hex string |
| `EF.Functions.ConsistentHash(hash, buckets)` | `yandexConsistentHash(hash, buckets)` | `uint` | Consistent hashing to N buckets |

```csharp
// Compute content fingerprints
var results = await context.Documents
    .Select(d => new
    {
        d.Id,
        ContentHash = EF.Functions.Sha256(d.Content),         // hex(SHA256(Content))
        QuickHash = EF.Functions.CityHash64(d.Content),       // cityHash64(Content)
        Bucket = EF.Functions.ConsistentHash(                  // yandexConsistentHash(cityHash64(Id), 16)
            EF.Functions.CityHash64(d.Id), 16u)
    })
    .ToListAsync();

// Shard assignment
var shardAssignment = await context.Users
    .Select(u => new
    {
        u.Id,
        Shard = EF.Functions.ConsistentHash(
            EF.Functions.SipHash64(u.Id), 8u)                 // yandexConsistentHash(sipHash64(Id), 8)
    })
    .ToListAsync();
```

---

## Formatting Functions

Human-readable formatting and date parsing from `ClickHouseFormatDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.FormatDateTime(dt, fmt)` | `formatDateTime(dt, fmt)` | `string` | Format DateTime with pattern |
| `EF.Functions.FormatReadableSize(bytes)` | `formatReadableSize(bytes)` | `string` | Bytes as "1.00 GiB" |
| `EF.Functions.FormatReadableDecimalSize(bytes)` | `formatReadableDecimalSize(bytes)` | `string` | Bytes as "1.00 GB" |
| `EF.Functions.FormatReadableQuantity(n)` | `formatReadableQuantity(n)` | `string` | Number as "1.00 million" |
| `EF.Functions.FormatReadableTimeDelta(seconds)` | `formatReadableTimeDelta(seconds)` | `string` | Seconds as "1 hour, 30 minutes" |
| `EF.Functions.ParseDateTime(s, fmt)` | `parseDateTime(s, fmt)` | `DateTime` | Parse string to DateTime |

```csharp
var results = await context.Downloads
    .Select(d => new
    {
        d.FileName,
        FileSize = EF.Functions.FormatReadableSize(d.SizeBytes),       // formatReadableSize(SizeBytes)
        DecimalSize = EF.Functions.FormatReadableDecimalSize(d.SizeBytes), // formatReadableDecimalSize(SizeBytes)
        DateLabel = EF.Functions.FormatDateTime(d.CreatedAt, "%Y-%m-%d"),  // formatDateTime(CreatedAt, '%Y-%m-%d')
        DownloadCount = EF.Functions.FormatReadableQuantity(              // formatReadableQuantity(TotalDownloads)
            Convert.ToDouble(d.TotalDownloads))
    })
    .ToListAsync();

// Parse date strings
var parsed = await context.RawLogs
    .Select(l => new
    {
        Timestamp = EF.Functions.ParseDateTime(l.DateString, "%Y/%m/%d %H:%i:%s")
    })
    .ToListAsync();
```

---

## IP Functions

IP address operations from `ClickHouseIpDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.IPv4NumToString(ip)` | `IPv4NumToString(ip)` | `string` | UInt32 to dotted-decimal string |
| `EF.Functions.IPv4StringToNum(s)` | `IPv4StringToNum(s)` | `uint` | Dotted-decimal string to UInt32 |
| `EF.Functions.IsIPAddressInRange(addr, cidr)` | `isIPAddressInRange(addr, cidr)` | `bool` | Check if IP is within CIDR range |
| `EF.Functions.IsIPv4String(s)` | `isIPv4String(s)` | `bool` | Validate IPv4 string format |
| `EF.Functions.IsIPv6String(s)` | `isIPv6String(s)` | `bool` | Validate IPv6 string format |

```csharp
// Filter requests by IP range
var internalRequests = await context.AccessLogs
    .Where(l => EF.Functions.IsIPAddressInRange(l.ClientIp, "10.0.0.0/8"))
    .ToListAsync();

// Analyze IP types
var ipStats = await context.AccessLogs
    .Select(l => new
    {
        l.ClientIp,
        IsV4 = EF.Functions.IsIPv4String(l.ClientIp),    // isIPv4String(ClientIp)
        IsV6 = EF.Functions.IsIPv6String(l.ClientIp),    // isIPv6String(ClientIp)
        InRange = EF.Functions.IsIPAddressInRange(         // isIPAddressInRange(ClientIp, '192.168.0.0/16')
            l.ClientIp, "192.168.0.0/16")
    })
    .ToListAsync();
```

---

## Encoding Functions

Base64 and hexadecimal encoding from `ClickHouseEncodingDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.Base64Encode(s)` | `base64Encode(s)` | `string` | Encode string as Base64 |
| `EF.Functions.Base64Decode(s)` | `base64Decode(s)` | `string` | Decode Base64-encoded string |
| `EF.Functions.Hex(value)` | `hex(value)` | `string` | Hexadecimal representation |
| `EF.Functions.Unhex(s)` | `unhex(s)` | `string` | Convert hex string back to bytes |

```csharp
var results = await context.Tokens
    .Select(t => new
    {
        Encoded = EF.Functions.Base64Encode(t.Value),      // base64Encode(Value)
        Decoded = EF.Functions.Base64Decode(t.Encoded),    // base64Decode(Encoded)
        HexValue = EF.Functions.Hex(t.BinaryData),         // hex(BinaryData)
        Restored = EF.Functions.Unhex(t.HexString)         // unhex(HexString)
    })
    .ToListAsync();
```

---

## Type Checking Functions

Floating-point validation functions from `ClickHouseTypeCheckDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.IsNaN(value)` | `isNaN(value)` | `bool` | True if value is Not-a-Number |
| `EF.Functions.IsFinite(value)` | `isFinite(value)` | `bool` | True if value is not infinite and not NaN |
| `EF.Functions.IsInfinite(value)` | `isInfinite(value)` | `bool` | True if value is infinite |

```csharp
// Filter out invalid measurements
var validReadings = await context.SensorReadings
    .Where(r => EF.Functions.IsFinite(r.Value))            // isFinite(Value)
    .Where(r => !EF.Functions.IsNaN(r.Value))              // NOT isNaN(Value)
    .ToListAsync();

// Flag problematic values
var flagged = await context.Calculations
    .Select(c => new
    {
        c.Id,
        c.Result,
        IsValid = EF.Functions.IsFinite(c.Result),         // isFinite(Result)
        IsNaN = EF.Functions.IsNaN(c.Result),              // isNaN(Result)
        IsInf = EF.Functions.IsInfinite(c.Result)          // isInfinite(Result)
    })
    .ToListAsync();
```

---

## UUID Generation

UUID v7 generation from `ClickHouseUuidDbFunctionsExtensions`.

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|----------------|-------------|-------------|
| `EF.Functions.NewGuidV7()` | `generateUUIDv7()` | `Guid` | Time-sortable UUID v7 |

UUID v7 values are time-sortable, making them suitable for use as primary keys in time-ordered data.

```csharp
var results = await context.Events
    .Select(e => new
    {
        NewId = EF.Functions.NewGuidV7(),                  // generateUUIDv7()
        e.EventType,
        e.Timestamp
    })
    .ToListAsync();
```

Note: Standard `Guid.NewGuid()` translates to `generateUUIDv4()` (random UUID). Use `EF.Functions.NewGuidV7()` when you need time-sortable identifiers.

---

## See Also

- [String Functions](string-functions.md) -- string manipulation, pattern matching, and split/join operations
- [DateTime Functions](datetime-functions.md) -- date truncation and formatting
- [Math Functions](math-functions.md) -- mathematical operations and type casting
- [Aggregate Functions](aggregate-functions.md) -- aggregate functions that compose with utility functions
