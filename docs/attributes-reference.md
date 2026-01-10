# Attributes Reference

This is a complete reference for all EF.CH attributes. All attributes are in the `EF.CH.Metadata.Attributes` namespace.

```csharp
using EF.CH.Metadata.Attributes;
```

---

## Computed Column Attributes

### MaterializedColumn

Marks a property as a `MATERIALIZED` column. The expression is computed on INSERT and stored on disk.

```csharp
[MaterializedColumn("expression")]
```

| Property | Type | Description |
|----------|------|-------------|
| `Expression` | `string` | ClickHouse SQL expression |

**Behavior:**
- Computed on INSERT, stored on disk
- Not returned by `SELECT *` - use explicit column selection
- Cannot be inserted directly
- Automatically configured with `ValueGenerated.OnAdd`

**Example:**
```csharp
public class Order
{
    public decimal Amount { get; set; }
    public decimal TaxRate { get; set; }

    [MaterializedColumn("\"Amount\" * \"TaxRate\"")]
    public decimal TaxAmount { get; set; }

    [MaterializedColumn("toYear(\"CreatedAt\")")]
    public int CreatedYear { get; set; }
}
```

**Fluent equivalent:** `.HasMaterializedExpression("expr")`

---

### AliasColumn

Marks a property as an `ALIAS` column. The expression is computed at query time and not stored.

```csharp
[AliasColumn("expression")]
```

| Property | Type | Description |
|----------|------|-------------|
| `Expression` | `string` | ClickHouse SQL expression |

**Behavior:**
- Computed on every read
- No storage cost
- Cannot be inserted into
- Automatically configured with `ValueGenerated.OnAddOrUpdate`

**Example:**
```csharp
public class Person
{
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;

    [AliasColumn("concat(\"FirstName\", ' ', \"LastName\")")]
    public string FullName { get; set; } = string.Empty;
}
```

**Fluent equivalent:** `.HasAliasExpression("expr")`

---

### DefaultExpression

Specifies a `DEFAULT` expression for a column. The expression is evaluated if no value is provided on INSERT.

```csharp
[DefaultExpression("expression")]
```

| Property | Type | Description |
|----------|------|-------------|
| `Expression` | `string` | ClickHouse SQL expression |

**Behavior:**
- Expression is only evaluated when no value is provided
- Column can be explicitly set during INSERT
- Value is stored on disk

**Example:**
```csharp
public class Event
{
    public Guid Id { get; set; }

    [DefaultExpression("now()")]
    public DateTime CreatedAt { get; set; }

    [DefaultExpression("generateUUIDv4()")]
    public Guid TraceId { get; set; }

    [DefaultExpression("'pending'")]
    public string Status { get; set; } = string.Empty;
}
```

**Fluent equivalent:** `.HasDefaultExpression("expr")`

---

## Compression Codec Attributes

### ClickHouseCodec

Specifies a custom compression codec for a column.

```csharp
[ClickHouseCodec("codecSpec")]
```

| Property | Type | Description |
|----------|------|-------------|
| `CodecSpec` | `string` | Codec specification (e.g., "Delta, ZSTD(3)") |

**Codec types:**
- **Preprocessing:** `Delta`, `DoubleDelta`, `Gorilla`, `T64`
- **Compression:** `LZ4`, `LZ4HC`, `ZSTD(level)`, `NONE`

Chain preprocessing codecs before compression codecs.

**Example:**
```csharp
public class Metrics
{
    [ClickHouseCodec("DoubleDelta, LZ4")]
    public DateTime Timestamp { get; set; }

    [ClickHouseCodec("Delta, ZSTD(3)")]
    public long SensorId { get; set; }

    [ClickHouseCodec("ZSTD(9)")]
    public string RawPayload { get; set; } = string.Empty;
}
```

**Fluent equivalent:** `.HasCodec("codecSpec")` or `.HasCodec(c => c.Delta().ZSTD(3))`

---

### Preset Codec Attributes

Convenience attributes for common codec patterns:

| Attribute | Codec | Best For |
|-----------|-------|----------|
| `[TimestampCodec]` | `DoubleDelta, LZ4` | DateTime columns with regular intervals |
| `[SequentialCodec]` | `Delta, ZSTD` | Auto-incrementing IDs, monotonic counters |
| `[FloatCodec]` | `Gorilla, ZSTD(1)` | Sensor/metric float values |
| `[IntegerCodec]` | `T64, LZ4` | Integers with sparse value ranges |
| `[HighCompressionCodec]` | `ZSTD(9)` | Large text/binary data |
| `[NoCompression]` | `NONE` | Pre-compressed data |

**Example:**
```csharp
public class SensorReading
{
    [TimestampCodec]
    public DateTime Timestamp { get; set; }

    [SequentialCodec]
    public long ReadingId { get; set; }

    [FloatCodec]
    public double Temperature { get; set; }

    [FloatCodec]
    public double Humidity { get; set; }

    [HighCompressionCodec]
    public string RawData { get; set; } = string.Empty;
}
```

**Fluent equivalents:** `.HasTimestampCodec()`, `.HasSequentialCodec()`, `.HasFloatCodec()`, `.HasHighCompressionCodec()`, `.HasNoCompression()`

---

## Skip Index Attributes

All skip index attributes support these common properties:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Granularity` | `int` | 3 | Granules per index entry (1-1000) |
| `Name` | `string?` | null | Custom index name (auto-generated if null) |

### MinMaxIndex

Creates a minmax skip index for range queries.

```csharp
[MinMaxIndex(Granularity = 2)]
```

**Best for:** Range queries on numeric or date columns.

**Example:**
```csharp
public class Order
{
    [MinMaxIndex(Granularity = 2)]
    public DateTime CreatedAt { get; set; }

    [MinMaxIndex(Granularity = 4)]
    public decimal Amount { get; set; }
}
```

**Fluent equivalent:** `.HasIndex(x => x.Column).UseMinmax().HasGranularity(2)`

---

### BloomFilterIndex

Creates a bloom filter skip index for exact value matching.

```csharp
[BloomFilterIndex(FalsePositive = 0.025, Granularity = 3)]
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `FalsePositive` | `double` | 0.025 | False positive rate (0.001-0.5) |

**Best for:** Equality queries, array contains checks.

**Example:**
```csharp
public class Product
{
    [BloomFilterIndex(FalsePositive = 0.01, Granularity = 3)]
    public string Sku { get; set; } = string.Empty;

    [BloomFilterIndex(FalsePositive = 0.025, Granularity = 4)]
    public string[] Tags { get; set; } = [];
}
```

**Fluent equivalent:** `.HasIndex(x => x.Column).UseBloomFilter(falsePositive: 0.025).HasGranularity(3)`

---

### TokenBFIndex

Creates a token bloom filter for tokenized text search.

```csharp
[TokenBFIndex(Size = 10240, Hashes = 3, Seed = 0, Granularity = 4)]
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Size` | `int` | 10240 | Bloom filter size in bytes (256-1048576) |
| `Hashes` | `int` | 3 | Number of hash functions (1-10) |
| `Seed` | `int` | 0 | Random seed for hash functions |

**Best for:** Log analysis, URL parameter search, tokenized text.

**Example:**
```csharp
public class LogEntry
{
    [TokenBFIndex(Granularity = 4)]
    public string Message { get; set; } = string.Empty;

    [TokenBFIndex(Size = 20480, Hashes = 4, Granularity = 5)]
    public string RequestUrl { get; set; } = string.Empty;
}
```

**Fluent equivalent:** `.HasIndex(x => x.Column).UseTokenBF(size: 10240, hashes: 3, seed: 0).HasGranularity(4)`

---

### NgramBFIndex

Creates an n-gram bloom filter for fuzzy/substring text matching.

```csharp
[NgramBFIndex(NgramSize = 4, Size = 10240, Hashes = 3, Seed = 0, Granularity = 5)]
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `NgramSize` | `int` | 4 | N-gram size (1-10) |
| `Size` | `int` | 10240 | Bloom filter size in bytes (256-1048576) |
| `Hashes` | `int` | 3 | Number of hash functions (1-10) |
| `Seed` | `int` | 0 | Random seed for hash functions |

**Best for:** Substring search, partial text matching, LIKE queries.

**Example:**
```csharp
public class Article
{
    [NgramBFIndex(NgramSize = 3, Granularity = 5)]
    public string Title { get; set; } = string.Empty;

    [NgramBFIndex(NgramSize = 4, Size = 20480, Granularity = 6)]
    public string Description { get; set; } = string.Empty;
}
```

**Fluent equivalent:** `.HasIndex(x => x.Column).UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0).HasGranularity(5)`

---

### SetIndex

Creates a set skip index for low-cardinality exact matching.

```csharp
[SetIndex(MaxRows = 100, Granularity = 2)]
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxRows` | `int` | 100 | Maximum distinct values to store (1-100000) |

**Best for:** Low-cardinality columns with exact matching queries (status, type, category).

**Example:**
```csharp
public class Order
{
    [SetIndex(MaxRows = 10, Granularity = 2)]
    public string Status { get; set; } = string.Empty;

    [SetIndex(MaxRows = 50, Granularity = 3)]
    public string Category { get; set; } = string.Empty;
}
```

**Fluent equivalent:** `.HasIndex(x => x.Column).UseSet(maxRows: 100).HasGranularity(2)`

---

## Type Configuration Attributes

### ClickHouseJson

Configures a property as a native ClickHouse JSON column (requires ClickHouse 24.8+).

```csharp
[ClickHouseJson(MaxDynamicPaths = 1024, MaxDynamicTypes = 32, IsTyped = false)]
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxDynamicPaths` | `int` | -1 | Max paths as subcolumns (-1 uses default 1024) |
| `MaxDynamicTypes` | `int` | -1 | Max types per path (-1 uses default 32) |
| `IsTyped` | `bool` | false | Enable typed POCO navigation |

**Example:**
```csharp
public class Event
{
    public Guid Id { get; set; }

    // Untyped JSON with JsonElement
    [ClickHouseJson(MaxDynamicPaths = 2048)]
    public JsonElement Metadata { get; set; }

    // Typed JSON with POCO
    [ClickHouseJson(IsTyped = true, MaxDynamicPaths = 256)]
    public OrderDetails? Details { get; set; }
}
```

**Fluent equivalent:** `.HasColumnType("JSON").HasMaxDynamicPaths(2048).HasMaxDynamicTypes(32)`

---

### ClickHouseTimeZone

Specifies the timezone for a DateTimeOffset column.

```csharp
[ClickHouseTimeZone("timeZone")]
```

| Property | Type | Description |
|----------|------|-------------|
| `TimeZone` | `string` | IANA timezone name |

**Behavior:**
- Values are stored as UTC in ClickHouse
- Timezone determines offset calculation when reading
- DST transitions are handled automatically

**Example:**
```csharp
public class Event
{
    public Guid Id { get; set; }

    [ClickHouseTimeZone("America/New_York")]
    public DateTimeOffset CreatedAt { get; set; }

    [ClickHouseTimeZone("Europe/London")]
    public DateTimeOffset? ScheduledAt { get; set; }

    [ClickHouseTimeZone("Asia/Tokyo")]
    public DateTimeOffset ProcessedAt { get; set; }
}
```

**Fluent equivalent:** `.HasTimeZone("America/New_York")`

---

## Attribute Precedence

When both attributes and fluent API are used, **fluent API always wins**. This follows standard EF Core behavior.

```csharp
public class Order
{
    [ClickHouseCodec("LZ4")]  // Overridden by fluent API
    public string Data { get; set; } = string.Empty;
}

// In OnModelCreating:
entity.Property(x => x.Data).HasCodec("ZSTD(9)");  // This wins
```

---

## See Also

- [Compression Codecs](features/storage/compression-codecs.md) - Detailed codec guide
- [Skip Indices](features/storage/skip-indices.md) - Index tuning guide
- [Computed Columns](features/schema/computed-columns.md) - MATERIALIZED, ALIAS, DEFAULT
- [JSON Types](types/json.md) - Native JSON support
- [DateTime](types/datetime.md) - Timezone handling
