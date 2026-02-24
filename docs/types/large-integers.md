# Large Integer Types

## CLR to ClickHouse Mapping

```
Int128      --> Int128
UInt128     --> UInt128
BigInteger  --> Int256  (default)
BigInteger  --> UInt256 (via HasColumnType)
```

## Int128

Maps directly to ClickHouse `Int128`. Available in .NET 7+.

```csharp
public class LargeCounter
{
    public uint Id { get; set; }
    public Int128 TotalBytes { get; set; }
}
```

```sql
CREATE TABLE "LargeCounters" (
    "Id" UInt32,
    "TotalBytes" Int128
) ENGINE = MergeTree() ORDER BY ("Id")
```

**Range**: -2^127 to 2^127 - 1 (approximately -1.7 x 10^38 to 1.7 x 10^38)

## UInt128

Maps directly to ClickHouse `UInt128`. Available in .NET 7+.

```csharp
public class HashRecord
{
    public uint Id { get; set; }
    public UInt128 HashValue { get; set; }
}
```

```sql
CREATE TABLE "HashRecords" (
    "Id" UInt32,
    "HashValue" UInt128
) ENGINE = MergeTree() ORDER BY ("Id")
```

**Range**: 0 to 2^128 - 1 (approximately 3.4 x 10^38)

## BigInteger (Int256 / UInt256)

`System.Numerics.BigInteger` maps to `Int256` by default. Use `HasColumnType("UInt256")` for the unsigned variant.

```csharp
using System.Numerics;

public class CryptoRecord
{
    public uint Id { get; set; }
    public BigInteger SignedValue { get; set; }     // Int256
    public BigInteger UnsignedValue { get; set; }   // UInt256
}
```

```csharp
modelBuilder.Entity<CryptoRecord>(entity =>
{
    // Default: Int256
    entity.Property(x => x.SignedValue);

    // Explicit: UInt256
    entity.Property(x => x.UnsignedValue)
        .HasColumnType("UInt256");
});
```

```sql
CREATE TABLE "CryptoRecords" (
    "Id" UInt32,
    "SignedValue" Int256,
    "UnsignedValue" UInt256
) ENGINE = MergeTree() ORDER BY ("Id")
```

### Int256 Range

- **Min**: -2^255 (approximately -5.8 x 10^76)
- **Max**: 2^255 - 1 (approximately 5.8 x 10^76)

### UInt256 Range

- **Min**: 0
- **Max**: 2^256 - 1 (approximately 1.2 x 10^77)

> **Note:** `BigInteger` in .NET supports arbitrary precision, but when mapped to ClickHouse Int256 or UInt256, values must be within the respective range. The `UInt256` mapping will throw an `ArgumentException` if a negative `BigInteger` is provided.

## SQL Literal Format

Large integer literals are generated as plain numeric strings without quotes:

```sql
170141183460469231731687303715884105727   -- Int128 max
340282366920938463463374607431768211455   -- UInt128 max
```

## Nested Type Support

Large integers can be used as fields within Nested types:

```csharp
public class MeasurementField
{
    public Int128 Value { get; set; }
    public UInt128 Checksum { get; set; }
}

public class DataRecord
{
    public uint Id { get; set; }
    public List<MeasurementField> Measurements { get; set; } = [];
}
```

```sql
"Measurements" Nested("Value" Int128, "Checksum" UInt128)
```

## See Also

- [Type System Overview](overview.md)
