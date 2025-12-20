# Compression Codecs

Configure per-column compression algorithms for optimal storage and query performance. Proper codec selection can reduce storage by 10-50% and improve query performance.

## Why Use Compression Codecs?

1. **Storage Reduction**: Different data patterns compress better with specialized codecs
2. **Query Performance**: Less data to read from disk means faster queries
3. **Cost Savings**: Reduced storage costs, especially for large datasets
4. **Codec Chaining**: Combine codecs (e.g., Delta + ZSTD) for maximum compression

## Configuration

### Fluent API with Codec Chain Builder

```csharp
modelBuilder.Entity<SensorReading>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.SensorId });

    // Fluent codec builder
    entity.Property(e => e.Timestamp)
        .HasCodec(c => c.DoubleDelta().LZ4());

    entity.Property(e => e.SensorId)
        .HasCodec(c => c.Delta().ZSTD(3));  // With compression level
});
```

### Fluent API with Raw String

```csharp
entity.Property(e => e.Value)
    .HasCodec("Gorilla, ZSTD(1)");
```

### Convenience Methods

```csharp
entity.Property(e => e.RawPayload).HasHighCompressionCodec();  // ZSTD(9)
entity.Property(e => e.UncompressedData).HasNoCompression();   // CODEC(NONE)
entity.Property(e => e.Timestamp).HasTimestampCodec();         // DoubleDelta, LZ4
entity.Property(e => e.SensorId).HasSequentialCodec();         // Delta, ZSTD
entity.Property(e => e.Value).HasFloatCodec();                 // Gorilla, ZSTD(1)
entity.Property(e => e.SparseInt).HasIntegerCodec();           // T64, LZ4
```

### Attribute Decorators

```csharp
public class SensorReading
{
    public Guid Id { get; set; }

    [TimestampCodec]                    // DoubleDelta, LZ4
    public DateTime Timestamp { get; set; }

    [SequentialCodec]                   // Delta, ZSTD
    public long SensorId { get; set; }

    [FloatCodec]                        // Gorilla, ZSTD(1)
    public double Value { get; set; }

    [HighCompressionCodec]              // ZSTD(9)
    public string RawPayload { get; set; } = string.Empty;

    [NoCompression]                     // None
    public byte[] UncompressedData { get; set; } = [];

    [ClickHouseCodec("Delta, ZSTD(5)")] // Custom codec spec
    public int Counter { get; set; }
}
```

## Available Codecs

| Codec | Best For | Notes |
|-------|----------|-------|
| `LZ4` | General purpose | Fast compression, moderate ratio |
| `ZSTD(level)` | General purpose | Better ratio, level 1-22 (default: 1) |
| `Delta` | Sequential integers | Stores differences between consecutive values |
| `DoubleDelta` | Timestamps | Stores difference of differences (ideal for time-series) |
| `Gorilla` | Float/Double | XOR-based encoding, excellent for sensor data |
| `T64` | Sparse integers | Block transformation for integers with gaps |
| `FPC(level)` | Float64 | Level 1-28 (default: 12) |
| `NONE` | Pre-compressed data | Disables compression entirely |

## Convenience Attributes

| Attribute | Codec Spec | Use Case |
|-----------|------------|----------|
| `[TimestampCodec]` | `DoubleDelta, LZ4` | DateTime columns |
| `[SequentialCodec]` | `Delta, ZSTD` | Auto-increment, counters, monotonic values |
| `[FloatCodec]` | `Gorilla, ZSTD(1)` | Sensor/metric floating-point values |
| `[HighCompressionCodec]` | `ZSTD(9)` | Large text/binary data |
| `[IntegerCodec]` | `T64, LZ4` | Sparse integer values |
| `[NoCompression]` | `NONE` | Already compressed data (images, encrypted) |

## Complete Example

```csharp
public class MetricData
{
    public Guid Id { get; set; }

    [TimestampCodec]
    public DateTime Timestamp { get; set; }

    [SequentialCodec]
    public long SensorId { get; set; }

    [FloatCodec]
    public double Value { get; set; }

    [HighCompressionCodec]
    public string Metadata { get; set; } = string.Empty;
}

modelBuilder.Entity<MetricData>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.SensorId });
    entity.HasPartitionByMonth(x => x.Timestamp);
});
```

## Generated DDL

```sql
CREATE TABLE "MetricData" (
    "Id" UUID,
    "Timestamp" DateTime64(3) CODEC(DoubleDelta, LZ4),
    "SensorId" Int64 CODEC(Delta, ZSTD),
    "Value" Float64 CODEC(Gorilla, ZSTD(1)),
    "Metadata" String CODEC(ZSTD(9))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM("Timestamp")
ORDER BY ("Timestamp", "SensorId")
```

## Precedence

Fluent API configuration **overrides** attribute decorators (standard EF Core behavior):

```csharp
public class Example
{
    [HighCompressionCodec]  // Attribute says ZSTD(9)
    public string Data { get; set; } = string.Empty;
}

modelBuilder.Entity<Example>(entity =>
{
    // Fluent API wins - will use LZ4, not ZSTD(9)
    entity.Property(e => e.Data).HasCodec("LZ4");
});
```

## Codec Selection Guide

### By Data Type

| Data Pattern | Recommended Codec | Why |
|--------------|-------------------|-----|
| Timestamps | `DoubleDelta, LZ4` | Monotonically increasing, predictable deltas |
| Sequential IDs | `Delta, ZSTD` | Consecutive values with small differences |
| Floating-point metrics | `Gorilla, ZSTD(1)` | XOR-based, captures sensor noise well |
| Large text/JSON | `ZSTD(9)` | High compression ratio |
| Already compressed | `NONE` | Avoid double-compression overhead |
| Sparse integers | `T64, LZ4` | Block transformation for scattered values |

### By Use Case

| Use Case | Recommended Setup |
|----------|-------------------|
| Time-series metrics | `[TimestampCodec]` + `[FloatCodec]` |
| Event logging | `[TimestampCodec]` + `[HighCompressionCodec]` for payload |
| IoT sensor data | `[SequentialCodec]` for device IDs + `[FloatCodec]` for readings |
| Audit logs | `[TimestampCodec]` + `[HighCompressionCodec]` for JSON payload |

## Scaffolding

When reverse-engineering existing ClickHouse tables with `dotnet ef dbcontext scaffold`, codec configurations are automatically discovered from `system.columns` and applied to the generated model.

## Codec Chaining

ClickHouse applies codecs in order during compression and reverses the order during decompression:

```csharp
// First Delta encodes, then ZSTD compresses
entity.Property(e => e.Value).HasCodec(c => c.Delta().ZSTD(3));
```

**Typical chains:**
- `DoubleDelta, LZ4` - Fast timestamp compression
- `Delta, ZSTD` - Sequential integers with good compression
- `Gorilla, ZSTD(1)` - Floating-point with light compression
- `T64, LZ4` - Sparse integers with fast decompression

## Level Validation

Compression levels are validated at configuration time:

| Codec | Valid Levels |
|-------|--------------|
| `ZSTD` | 1-22 |
| `FPC` | 1-28 |

```csharp
// Throws ArgumentOutOfRangeException
entity.Property(e => e.Value).HasCodec(c => c.ZSTD(0));   // Invalid
entity.Property(e => e.Value).HasCodec(c => c.ZSTD(23)); // Invalid
```

## Limitations

- **No Runtime Validation**: Codec-type compatibility is validated by ClickHouse at query time, not at model build time
- **Immutable After Creation**: Changing codecs requires `ALTER TABLE MODIFY COLUMN`
- **Chaining Order Matters**: Codec order affects compression ratio and speed

## Best Practices

### Match Codec to Data Pattern

```csharp
// Good: DoubleDelta for timestamps
entity.Property(e => e.Timestamp).HasCodec(c => c.DoubleDelta().LZ4());

// Less optimal: Generic ZSTD for timestamps
entity.Property(e => e.Timestamp).HasCodec(c => c.ZSTD(3));
```

### Use Attributes for Consistency

```csharp
// Apply consistent codec strategy across entities
public class Event
{
    [TimestampCodec]
    public DateTime Timestamp { get; set; }
}

public class Metric
{
    [TimestampCodec]
    public DateTime Timestamp { get; set; }
}
```

### Avoid Over-Compression

Higher ZSTD levels (9-22) provide diminishing returns with significantly increased CPU usage:

```csharp
// Good: ZSTD(3) for balance of speed and ratio
entity.Property(e => e.Data).HasCodec(c => c.ZSTD(3));

// Avoid: ZSTD(22) unless storage is critical constraint
entity.Property(e => e.Data).HasCodec(c => c.ZSTD(22));
```

## See Also

- [Partitioning](partitioning.md)
- [TTL (Time-To-Live)](ttl.md)
- [MergeTree Engine](../engines/mergetree.md)
- [ClickHouse Compression Codecs](https://clickhouse.com/docs/en/sql-reference/statements/create/table#column_compression_codec)
