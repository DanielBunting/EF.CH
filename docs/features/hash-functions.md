# Hash Functions

ClickHouse provides fast, non-cryptographic hash functions optimized for analytics (bucketing, sampling, deduplication) and cryptographic hash functions for integrity checking. EF.CH exposes these as `EF.Functions` extensions.

## Available Functions

### Non-Cryptographic Hashes

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `CityHash64(value)` | `cityHash64(x)` | `ulong` | Google CityHash — fast, good distribution |
| `SipHash64(value)` | `sipHash64(x)` | `ulong` | SipHash — fast with collision resistance |
| `XxHash64(value)` | `xxHash64(x)` | `ulong` | xxHash — very fast for large inputs |
| `MurmurHash3_64(value)` | `murmurHash3_64(x)` | `ulong` | MurmurHash3 — widely used |
| `FarmHash64(value)` | `farmHash64(x)` | `ulong` | Google FarmHash — successor to CityHash |

### Cryptographic Hashes

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `Md5(value)` | `hex(MD5(x))` | `string` | MD5 as hex string |
| `Sha256(value)` | `hex(SHA256(x))` | `string` | SHA-256 as hex string |

### Consistent Hashing

| C# Method | ClickHouse SQL | Return Type | Description |
|-----------|---------------|-------------|-------------|
| `ConsistentHash(hash, buckets)` | `yandexConsistentHash(hash, n)` | `uint` | Map hash to bucket (0..n-1) |

All hash functions accept a generic `T` parameter — you can hash strings, numbers, dates, or any column type.

## Usage Examples

### Data Bucketing / Sharding

```csharp
using EF.CH.Extensions;

// Assign users to one of 100 buckets for A/B testing
var bucketed = await context.Users
    .Select(u => new
    {
        u.Id,
        u.Name,
        Bucket = EF.Functions.ConsistentHash(EF.Functions.CityHash64(u.Id), 100)
    })
    .ToListAsync();

// Filter to bucket 0 (1% of users)
var testGroup = await context.Users
    .Where(u => EF.Functions.ConsistentHash(EF.Functions.CityHash64(u.Id), 100) == 0)
    .ToListAsync();
```

### Deduplication Keys

```csharp
// Create a hash-based deduplication key from multiple columns
var withHash = await context.Events
    .Select(e => new
    {
        e.Id,
        DedupeKey = EF.Functions.SipHash64(e.UserId + e.EventType + e.Timestamp.ToString())
    })
    .ToListAsync();
```

### Data Integrity

```csharp
// Compute content hashes for verification
var hashes = await context.Documents
    .Select(d => new
    {
        d.Id,
        d.Title,
        ContentHash = EF.Functions.Sha256(d.Content)
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Id", "Title", hex(SHA256("Content")) AS "ContentHash"
FROM "Documents"
```

### Sampling by Hash

```csharp
// Deterministic 10% sample using hash modulo
var sample = await context.Events
    .Where(e => EF.Functions.CityHash64(e.Id) % 10 == 0)
    .ToListAsync();
```

## Choosing a Hash Function

| Function | Speed | Use Case |
|----------|-------|----------|
| `CityHash64` | Very fast | General-purpose bucketing, sampling |
| `SipHash64` | Fast | When collision resistance matters |
| `XxHash64` | Very fast | Large payloads, checksums |
| `FarmHash64` | Very fast | Similar to CityHash, newer |
| `MurmurHash3_64` | Fast | Compatibility with other systems using MurmurHash |
| `Md5` | Slower | Content fingerprinting (not for security) |
| `Sha256` | Slowest | Integrity verification, content addressing |

> **Note:** `Md5` and `Sha256` return hex strings (e.g. `"6384E2B2184BCB..."`). The non-cryptographic hashes return `ulong` values.

## Notes

- An existing `string.CityHash64()` instance extension also exists (from `ClickHouseFunctions`). The `EF.Functions.CityHash64<T>()` version is generic and works with any column type.
- `ConsistentHash` maps to `yandexConsistentHash`, which distributes hashes evenly across buckets with minimal redistribution when the bucket count changes.
- All hash functions accept any type — ClickHouse will convert the value to bytes internally.

## Learn More

- [ClickHouse Hash Functions](https://clickhouse.com/docs/en/sql-reference/functions/hash-functions)
