# Type System Overview

EF.CH maps CLR types to ClickHouse column types automatically. This page is the complete reference for all 40+ supported type mappings.

## Numeric Types

```
CLR Type          ClickHouse Type
---------         ---------------
sbyte         --> Int8
short         --> Int16
int           --> Int32
long          --> Int64
Int128        --> Int128
byte          --> UInt8
ushort        --> UInt16
uint          --> UInt32
ulong         --> UInt64
UInt128       --> UInt128
BigInteger    --> Int256 (default) or UInt256
float         --> Float32
double        --> Float64
decimal       --> Decimal(18,4) (default)
```

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `sbyte` | `Int8` | Signed 8-bit integer |
| `short` | `Int16` | Signed 16-bit integer |
| `int` | `Int32` | Signed 32-bit integer |
| `long` | `Int64` | Signed 64-bit integer |
| `Int128` | `Int128` | .NET 7+ native 128-bit signed integer |
| `byte` | `UInt8` | Unsigned 8-bit integer |
| `ushort` | `UInt16` | Unsigned 16-bit integer |
| `uint` | `UInt32` | Unsigned 32-bit integer |
| `ulong` | `UInt64` | Unsigned 64-bit integer |
| `UInt128` | `UInt128` | .NET 7+ native 128-bit unsigned integer |
| `BigInteger` | `Int256` | Default mapping; use `HasColumnType("UInt256")` for unsigned |
| `float` | `Float32` | IEEE 754 single-precision; supports `nan`, `inf`, `-inf` |
| `double` | `Float64` | IEEE 754 double-precision; supports `nan`, `inf`, `-inf` |
| `decimal` | `Decimal(18,4)` | Configurable precision/scale; also `Decimal32(S)`, `Decimal64(S)`, `Decimal128(S)` |

### Decimal Variants

```csharp
// Default: Decimal(18, 4)
entity.Property(x => x.Amount);

// Custom precision and scale
entity.Property(x => x.Amount).HasColumnType("Decimal(38, 10)");

// Fixed-width variants
entity.Property(x => x.Price).HasColumnType("Decimal32(2)");   // 9 digits, Int32 storage
entity.Property(x => x.Total).HasColumnType("Decimal64(4)");   // 18 digits, Int64 storage
entity.Property(x => x.Big).HasColumnType("Decimal128(6)");    // 38 digits, Int128 storage
```

## String Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `string` | `String` | Variable length, UTF-8 encoded |
| `string` | `FixedString(N)` | Fixed length in bytes, null-padded; set via `HasColumnType("FixedString(3)")` |

## Boolean and GUID

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `bool` | `Bool` | Stored as UInt8 internally (0 or 1) |
| `Guid` | `UUID` | 128-bit universally unique identifier |

```csharp
// Guid.NewGuid() translates to generateUUIDv4()
var query = context.Events.Select(e => new { Id = Guid.NewGuid() });
// SQL: SELECT generateUUIDv4() AS "Id"
```

## Date/Time Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `DateTime` | `DateTime64(3)` | Configurable precision 0-9; optional timezone |
| `DateTimeOffset` | `DateTime64(3, 'TZ')` | Stores UTC, reads with timezone offset; IANA timezone required |
| `DateOnly` | `Date` | Range: 1970-01-01 to 2149-06-06 |
| `DateOnly` | `Date32` | Extended range: 1900-01-01 to 2299-12-31 |
| `TimeOnly` | `Time` | Nanosecond precision time-of-day |
| `TimeSpan` | `Int64` | Stored as nanoseconds via built-in value converter |

```csharp
// DateTime with custom precision
entity.Property(x => x.CreatedAt).HasPrecision(6); // microseconds

// DateTimeOffset with timezone
entity.Property(x => x.EventTime).HasTimeZone("America/New_York");

// DateOnly with extended range
entity.Property(x => x.BirthDate).HasColumnType("Date32");
```

## Collection Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `T[]`, `List<T>`, `IList<T>`, `ICollection<T>`, `IReadOnlyList<T>` | `Array(T)` | Any mappable element type; 1-based indexing in ClickHouse |
| `Dictionary<K,V>`, `IDictionary<K,V>`, `IReadOnlyDictionary<K,V>` | `Map(K, V)` | Keys cannot be Nullable; stored as `Array(Tuple(K, V))` internally |
| `ValueTuple<T1,...>`, `Tuple<T1,...>` | `Tuple(T1, T2, ...)` | 1-8+ elements; supports named and unnamed tuples |
| `List<TRecord>`, `TRecord[]` | `Nested(field1 T1, ...)` | Record properties become parallel arrays in ClickHouse |

```csharp
// Array
public int[] Tags { get; set; }
// DDL: "Tags" Array(Int32)

// Map
public Dictionary<string, int> Attributes { get; set; }
// DDL: "Attributes" Map(String, Int32)

// Tuple
public (string Name, int Age) Info { get; set; }
// DDL: "Info" Tuple(String, Int32)

// Nested
public List<GoalRecord> Goals { get; set; }
// DDL: "Goals" Nested("Id" UInt32, "EventTime" DateTime64(3))
```

## Enum Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| Any C# `enum` | `Enum8(...)` | Auto-selected when values fit in [-128, 127] and count <= 256 |
| Any C# `enum` | `Enum16(...)` | Auto-selected when values exceed Enum8 range |

```csharp
public enum OrderStatus { Pending = 0, Shipped = 1, Delivered = 2 }
// DDL: Enum8('Pending' = 0, 'Shipped' = 1, 'Delivered' = 2)
```

## JSON Type

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `JsonElement` | `JSON` | Requires ClickHouse 24.8+; native subcolumn access |
| `JsonDocument` | `JSON` | Same mapping; optional `max_dynamic_paths`, `max_dynamic_types` |

```csharp
entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasMaxDynamicPaths(2048)
    .HasMaxDynamicTypes(64);
```

## IP Address Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `ClickHouseIPv4` | `IPv4` | Custom struct wrapping UInt32 |
| `ClickHouseIPv6` | `IPv6` | Custom struct wrapping 16 bytes |
| `IPAddress` | `IPv6` | `System.Net.IPAddress`; maps to IPv6 for maximum compatibility |

```csharp
public ClickHouseIPv4 ServerIp { get; set; }
// DDL: "ServerIp" IPv4

public IPAddress ClientIp { get; set; }
// DDL: "ClientIp" IPv6
```

## Aggregate Function Types

| CLR Type | ClickHouse Type | Notes |
|----------|-----------------|-------|
| `byte[]` | `AggregateFunction(func, T)` | Opaque binary intermediate state for -State/-Merge pattern |
| `T` (direct value) | `SimpleAggregateFunction(func, T)` | Stores final result directly; for `max`, `min`, `sum`, `any`, `anyLast` |

```csharp
// AggregateFunction: stores opaque binary state
entity.Property(x => x.SumState)
    .HasAggregateFunction("sum", typeof(long));
// DDL: "SumState" AggregateFunction(sum, Int64)

// SimpleAggregateFunction: stores result directly
entity.Property(x => x.MaxValue)
    .HasSimpleAggregateFunction("max");
// DDL: "MaxValue" SimpleAggregateFunction(max, Float64)
```

## Special Type Wrappers

### Nullable(T)

Nullable CLR types (`int?`, `string?`, etc.) are automatically wrapped in `Nullable(T)` in ClickHouse. ClickHouse nullable columns carry a per-row bitmask overhead.

```csharp
public int? Score { get; set; }
// DDL: "Score" Nullable(Int32)
```

> **Note:** For columns where NULL is rare or a sentinel value is acceptable, use `HasDefaultForNull()` to avoid the Nullable overhead:
>
> ```csharp
> entity.Property(x => x.Score).HasDefaultForNull(0);
> // DDL: "Score" Int32 DEFAULT 0
> ```

### LowCardinality(T)

Dictionary-encoded storage optimization for columns with low cardinality (typically fewer than 10,000 unique values).

```csharp
entity.Property(x => x.Status).HasLowCardinality();
// DDL: "Status" LowCardinality(String)

entity.Property(x => x.CountryCode).HasLowCardinalityFixedString(2);
// DDL: "CountryCode" LowCardinality(FixedString(2))
```

For nullable properties, the mapping automatically nests:

```csharp
entity.Property(x => x.Category).HasLowCardinality(); // nullable string
// DDL: "Category" LowCardinality(Nullable(String))
```

## See Also

- [Arrays](arrays.md)
- [Maps](maps.md)
- [Tuples](tuples.md)
- [Nested](nested.md)
- [Enums](enums.md)
- [JSON](json.md)
- [DateTime](datetime.md)
- [IP Addresses](ip-addresses.md)
- [Large Integers](large-integers.md)
