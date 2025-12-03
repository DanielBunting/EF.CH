# Type Mappings Overview

EF.CH automatically maps .NET types to ClickHouse types. This page provides a complete reference.

## Basic Type Mappings

### Numeric Types

| .NET Type | ClickHouse Type | Range |
|-----------|-----------------|-------|
| `sbyte` | `Int8` | -128 to 127 |
| `short` | `Int16` | -32,768 to 32,767 |
| `int` | `Int32` | -2B to 2B |
| `long` | `Int64` | -9Q to 9Q |
| `byte` | `UInt8` | 0 to 255 |
| `ushort` | `UInt16` | 0 to 65,535 |
| `uint` | `UInt32` | 0 to 4B |
| `ulong` | `UInt64` | 0 to 18Q |
| `float` | `Float32` | IEEE 754 single |
| `double` | `Float64` | IEEE 754 double |
| `decimal` | `Decimal(18, 4)` | Fixed precision |

### String and Boolean

| .NET Type | ClickHouse Type | Notes |
|-----------|-----------------|-------|
| `string` | `String` | Variable length |
| `bool` | `Bool` | True/False |

### Date and Time

| .NET Type | ClickHouse Type | Notes |
|-----------|-----------------|-------|
| `DateTime` | `DateTime64(3)` | Millisecond precision |
| `DateTimeOffset` | `DateTime64(3)` | With timezone |
| `DateOnly` | `Date` | Date only (no time) |
| `TimeOnly` | `Time` | Time only (no date) |
| `TimeSpan` | `Int64` | Stored as nanoseconds |

### Identifiers

| .NET Type | ClickHouse Type | Notes |
|-----------|-----------------|-------|
| `Guid` | `UUID` | 128-bit UUID |

## Nullable Types

Nullable .NET types map to `Nullable(T)` in ClickHouse:

```csharp
public class Order
{
    public int? OptionalQuantity { get; set; }     // Nullable(Int32)
    public DateTime? CompletedAt { get; set; }     // Nullable(DateTime64(3))
    public string? Notes { get; set; }             // Nullable(String)
}
```

**Performance Note:** Nullable columns have overhead. For high-volume tables, consider using [HasDefaultForNull](../features/default-for-null.md) to avoid Nullable types.

## Complex Types

### Arrays

Arrays and lists map to `Array(T)`:

```csharp
public class Product
{
    public Guid Id { get; set; }
    public string[] Tags { get; set; } = [];           // Array(String)
    public int[] PriceTiers { get; set; } = [];        // Array(Int32)
    public List<string> Categories { get; set; } = []; // Array(String)
}
```

See [Arrays](arrays.md) for LINQ operations.

### Maps (Dictionaries)

Dictionaries map to `Map(K, V)`:

```csharp
public class Event
{
    public Guid Id { get; set; }
    public Dictionary<string, string> Metadata { get; set; } = new();  // Map(String, String)
    public Dictionary<string, int> Counters { get; set; } = new();     // Map(String, Int32)
}
```

See [Maps](maps.md) for LINQ operations.

### Nested Types

For columnar arrays of structured data:

```csharp
public record LineItem
{
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
}

public class Order
{
    public Guid Id { get; set; }
    public List<LineItem> Items { get; set; } = [];  // Nested(ProductId UUID, Quantity Int32, Price Decimal)
}
```

See [Nested Types](nested.md) for details.

### Tuples

Value tuples map to `Tuple(...)`:

```csharp
public class Location
{
    public Guid Id { get; set; }
    public (double Lat, double Lon) Coordinates { get; set; }  // Tuple(Float64, Float64)
}
```

See [Tuples](../features/tuples.md) for details.

## Enum Types

C# enums map to `Enum8` or `Enum16` (auto-selected based on value range):

```csharp
public enum OrderStatus
{
    Pending = 0,
    Processing = 1,
    Shipped = 2,
    Delivered = 3,
    Cancelled = 4
}

public class Order
{
    public Guid Id { get; set; }
    public OrderStatus Status { get; set; }  // Enum8('Pending'=0, 'Processing'=1, ...)
}
```

See [Enums](enums.md) for scaffolding and edge cases.

## IP Address Types

Special types for network addresses:

```csharp
using EF.CH.Types;

public class AccessLog
{
    public Guid Id { get; set; }
    public ClickHouseIPv4 ClientIPv4 { get; set; }  // IPv4
    public ClickHouseIPv6 ClientIPv6 { get; set; }  // IPv6
    public IPAddress ClientIP { get; set; }         // IPv6 (stores both v4 and v6)
}
```

See [IP Addresses](ip-addresses.md) for details.

## Large Integers

For values beyond `long`:

| .NET Type | ClickHouse Type |
|-----------|-----------------|
| `Int128` | `Int128` |
| `UInt128` | `UInt128` |
| `BigInteger` | `Int256` |

## JSON Types

For semi-structured data:

```csharp
using System.Text.Json;

public class Event
{
    public Guid Id { get; set; }
    public JsonElement Payload { get; set; }    // JSON
    public JsonDocument Document { get; set; }  // JSON
}
```

## Type Optimization

### LowCardinality

For string columns with few unique values:

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.Property(e => e.Status)
        .HasLowCardinality();  // LowCardinality(String)

    entity.Property(e => e.CountryCode)
        .HasLowCardinalityFixedString(2);  // LowCardinality(FixedString(2))
});
```

See [LowCardinality](../features/low-cardinality.md).

### DefaultForNull

Avoid Nullable overhead:

```csharp
modelBuilder.Entity<Metric>(entity =>
{
    entity.Property(e => e.Value)
        .HasDefaultForNull(0);  // Int32 with default 0 instead of Nullable(Int32)
});
```

See [DefaultForNull](../features/default-for-null.md).

## Scaffolding

When reverse-engineering an existing ClickHouse database:

| ClickHouse Type | Generated .NET Type |
|-----------------|---------------------|
| `Int32` | `int` |
| `Nullable(Int32)` | `int?` |
| `String` | `string` |
| `Array(String)` | `string[]` |
| `Map(String, Int32)` | `Dictionary<string, int>` |
| `Enum8(...)` | Generated C# enum |
| `IPv4` | `ClickHouseIPv4` |

See [Scaffolding](../scaffolding.md) for details.

## See Also

- [Arrays](arrays.md) - Array operations in LINQ
- [Maps](maps.md) - Dictionary operations
- [Nested Types](nested.md) - Columnar nested structures
- [Enums](enums.md) - Enum handling and scaffolding
- [IP Addresses](ip-addresses.md) - IPv4/IPv6 types
- [DateTime](datetime.md) - DateTime handling and functions
