# Column Features

Column-level configuration for compression, computed expressions, storage optimization, aggregate functions, timezones, and JSON options.

All column features are configured via `PropertyBuilder` extension methods in `OnModelCreating`.

---

## Compression Codecs

ClickHouse supports codec chains that preprocess and compress column data. EF.CH exposes them via a single fluent builder that validates codec combinations and covers every ClickHouse codec.

```csharp
modelBuilder.Entity<SensorReading>(entity =>
{
    // Timestamps with regular intervals
    entity.Property(x => x.Timestamp)
        .HasCodec(c => c.DoubleDelta().LZ4());

    // Sequential IDs or counters
    entity.Property(x => x.SequenceNumber)
        .HasCodec(c => c.Delta().ZSTD());

    // Floating-point sensor values
    entity.Property(x => x.Temperature)
        .HasCodec(c => c.Gorilla().ZSTD(1));

    // Large text or binary payloads
    entity.Property(x => x.Payload)
        .HasCodec(c => c.ZSTD(9));

    // Integers with sparse value ranges
    entity.Property(x => x.StatusCode)
        .HasCodec(c => c.T64().LZ4());

    // Disable compression entirely
    entity.Property(x => x.PreCompressedData)
        .HasCodec(c => c.None());

    // Custom level
    entity.Property(x => x.Value)
        .HasCodec(c => c.FPC(12).ZSTD());
});
```

Available builder methods: `Delta()`, `DoubleDelta()`, `Gorilla()`, `T64()`, `FPC(level)`, `LZ4()`, `ZSTD(level)`, `None()`.

**Generated SQL:**

```sql
CREATE TABLE SensorReadings (
    Timestamp DateTime64(3) CODEC(DoubleDelta, LZ4),
    Temperature Float64 CODEC(Gorilla, ZSTD(1)),
    Payload String CODEC(ZSTD(9))
) ENGINE = MergeTree() ORDER BY Timestamp
```

### Codec Guidelines

- Preprocessing codecs (Delta, DoubleDelta, Gorilla, T64) go first, then compression (LZ4, ZSTD).
- Do not combine Gorilla with Delta or DoubleDelta.
- ZSTD levels range from 1 (fast) to 22 (maximum compression). Level 9 is a good balance for cold storage.
- FPC levels range from 1 to 28, with 12 as the default.

---

## Computed Columns

ClickHouse supports three types of computed columns that differ in when the expression is evaluated and whether the result is stored on disk.

### MATERIALIZED

Computed on INSERT and stored on disk. Not returned by `SELECT *` -- must be selected explicitly.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.Property(x => x.TotalWithTax)
        .HasMaterializedExpression("Amount * 1.1");

    entity.Property(x => x.OrderYear)
        .HasMaterializedExpression("toYear(OrderDate)");

    entity.Property(x => x.FullName)
        .HasMaterializedExpression("concat(FirstName, ' ', LastName)");
});
```

**Generated SQL:**

```sql
TotalWithTax Float64 MATERIALIZED Amount * 1.1,
OrderYear UInt16 MATERIALIZED toYear(OrderDate),
FullName String MATERIALIZED concat(FirstName, ' ', LastName)
```

The property is automatically configured with `ValueGeneratedOnAdd`, excluding it from INSERT statements.

### ALIAS

Computed at query time, not stored on disk. Zero storage cost.

```csharp
modelBuilder.Entity<LineItem>(entity =>
{
    entity.Property(x => x.LineTotal)
        .HasAliasExpression("Amount * Quantity");

    entity.Property(x => x.DiscountedTotal)
        .HasAliasExpression("Amount * Quantity * (1 - DiscountRate)");
});
```

**Generated SQL:**

```sql
LineTotal Float64 ALIAS Amount * Quantity,
DiscountedTotal Float64 ALIAS Amount * Quantity * (1 - DiscountRate)
```

The property is automatically configured with `ValueGeneratedOnAddOrUpdate`, excluding it from all modification statements.

### DEFAULT

Computed if no value is provided on INSERT. Can be overridden by providing an explicit value.

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.Property(x => x.CreatedAt)
        .HasDefaultExpression("now()");

    entity.Property(x => x.TraceId)
        .HasDefaultExpression("generateUUIDv4()");
});
```

**Generated SQL:**

```sql
CreatedAt DateTime64(3) DEFAULT now(),
TraceId UUID DEFAULT generateUUIDv4()
```

Unlike MATERIALIZED, DEFAULT columns can receive explicit values during INSERT.

#### Server-generated identifiers

For common identifier-generator defaults, EF.CH provides strongly-typed sugar that wraps `HasDefaultExpression` and also sets `ValueGeneratedOnAdd` (so EF omits the column from INSERTs):

| Helper | ClickHouse expression | CLR type |
|---|---|---|
| `HasSerialIDDefault("counter_name")` | `generateSerialID('counter_name')` | `ulong` (UInt64) — Keeper-backed, gap-free monotonic |
| `HasUuidV4Default()` | `generateUUIDv4()` | `Guid` — random |
| `HasUuidV7Default()` | `generateUUIDv7()` | `Guid` — time-sortable |
| `HasUlidDefault()` | `generateULID()` | `string` — 26-char sortable |
| `HasSnowflakeIDDefault()` | `generateSnowflakeID()` | `long` (Int64) |

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.Property(x => x.Id).HasSerialIDDefault("orders_counter");
    entity.Property(x => x.TraceId).HasUuidV7Default();
});
```

`generateSerialID` requires ClickHouse Keeper to be configured. See the [IdentifierDefaultsSample](../../samples/IdentifierDefaultsSample) for a runnable example.

### EPHEMERAL

Write-only input columns. Not stored on disk; used to feed DEFAULT or MATERIALIZED expressions on other columns.

```csharp
modelBuilder.Entity<Record>(entity =>
{
    // Raw value supplied at INSERT, never stored.
    entity.Property(x => x.UnhashedKey)
        .HasEphemeralExpression();

    // Derived column, computed server-side from the ephemeral input.
    entity.Property(x => x.HashedKey)
        .HasMaterializedExpression("sipHash64(\"UnhashedKey\")");

    // Optional default: used if the caller omits the column from INSERT.
    entity.Property(x => x.CreatedAt)
        .HasEphemeralExpression("now()");
});
```

**Generated SQL:**

```sql
UnhashedKey UInt64 EPHEMERAL,
HashedKey UInt64 MATERIALIZED sipHash64("UnhashedKey"),
CreatedAt DateTime64(3) EPHEMERAL now()
```

EPHEMERAL is mutually exclusive with MATERIALIZED, ALIAS, and DEFAULT on the same property. Compression codecs are not allowed (there is no storage to compress).

> **Caveat — ephemeral columns always read back as defaults.** ClickHouse rejects explicit SELECTs of ephemeral columns, so EF.CH rewrites every reference to an ephemeral column in generated SQL to `defaultValueOfTypeName('<StoreType>')`. Queries succeed, but the property always comes back as the CLR default (`0`, `null`, `Guid.Empty`, etc.). Use ephemeral columns only for inputs whose derived MATERIALIZED result is the thing you actually care about.

The property is configured with `BeforeSaveBehavior = Save`, `AfterSaveBehavior = Ignore`, and `ValueGenerated = Never` — included in INSERTs, excluded from UPDATEs, client-supplied.

---

## LowCardinality

Dictionary-encoded storage optimization for columns with fewer than ~10,000 unique values. Reduces storage and improves query performance for columns like status codes, country codes, and category names.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    // LowCardinality(String) or LowCardinality(Nullable(String)) based on nullability
    entity.Property(x => x.Status)
        .HasLowCardinality();

    entity.Property(x => x.CountryCode)
        .HasLowCardinality();

    // LowCardinality(FixedString(2)) for known fixed-length values
    entity.Property(x => x.IsoCode)
        .HasLowCardinalityFixedString(2);

    // LowCardinality(FixedString(3)) for currency codes
    entity.Property(x => x.CurrencyCode)
        .HasLowCardinalityFixedString(3);
});
```

**Generated SQL:**

```sql
Status LowCardinality(String),
CountryCode LowCardinality(String),
IsoCode LowCardinality(FixedString(2)),
CurrencyCode LowCardinality(FixedString(3))
```

For nullable properties, the type is automatically wrapped: `LowCardinality(Nullable(String))` or `LowCardinality(Nullable(FixedString(3)))`.

---

## DefaultForNull

Replaces `Nullable(T)` columns with a non-nullable column that uses a sentinel value to represent NULL. This eliminates the per-column null bitmask overhead that ClickHouse maintains for nullable columns.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    // Use 0 as sentinel for nullable int
    entity.Property(x => x.DiscountPercent)
        .HasDefaultForNull(0);

    // Use empty string for nullable string
    entity.Property(x => x.Notes)
        .HasDefaultForNull("");

    // Use Guid.Empty for nullable Guid
    entity.Property(x => x.ExternalId)
        .HasDefaultForNull(Guid.Empty);
});
```

**Generated SQL:**

```sql
DiscountPercent Int32 DEFAULT 0
```

### Query Behavior

Queries translate null checks to sentinel comparisons automatically:

```csharp
context.Orders.Where(o => o.DiscountPercent == null)
// SQL: WHERE DiscountPercent = 0
```

### Aggregate Exclusion

Sentinel values are automatically excluded from `OrNull` aggregate functions. When you call `Sum()` on a column with `HasDefaultForNull(0)`, the provider generates SQL that ignores rows where the value equals the sentinel, so they are treated as NULL rather than as zero.

### Limitations

- Use `== null` instead of `.HasValue` -- EF Core optimizes `.HasValue` away since the database column is non-nullable.
- Use conditional expressions instead of `??` coalesce: `e.Score == null ? fallback : e.Score` instead of `e.Score ?? fallback`.
- Raw SQL queries return the stored sentinel value (e.g., 0), not null.
- `Where(e => e.Score == 0)` matches both explicit zeros and null rows.

---

## SimpleAggregateFunction

For aggregates where the state equals the final result (max, min, sum, etc.). The value can be read directly without `-Merge` combinators.

```csharp
modelBuilder.Entity<DailyStats>(entity =>
{
    entity.UseAggregatingMergeTree(x => x.Date);

    entity.Property(x => x.MaxOrderValue)
        .HasSimpleAggregateFunction("max");

    entity.Property(x => x.TotalQuantity)
        .HasSimpleAggregateFunction("sum");

    entity.Property(x => x.LatestStatus)
        .HasSimpleAggregateFunction("anyLast");
});
```

**Generated SQL:**

```sql
MaxOrderValue SimpleAggregateFunction(max, Float64),
TotalQuantity SimpleAggregateFunction(sum, Int64),
LatestStatus SimpleAggregateFunction(anyLast, String)
```

Supported functions: `max`, `min`, `sum`, `any`, `anyLast`, `groupBitAnd`, `groupBitOr`, `groupBitXor`.

The CLR type is automatically mapped to the appropriate ClickHouse type (e.g., `double` to `Float64`, `long` to `Int64`).

---

## AggregateFunction

For complex aggregates that store opaque intermediate binary state, requiring `-State` and `-Merge` combinator patterns. The CLR property type must be `byte[]`.

### Typed Declaration

```csharp
public class HourlyStats
{
    public DateTime Hour { get; set; }
    public byte[] CountState { get; set; } = [];
    public byte[] SumAmountState { get; set; } = [];
    public byte[] AvgResponseTimeState { get; set; } = [];
}

modelBuilder.Entity<HourlyStats>(entity =>
{
    entity.UseAggregatingMergeTree(x => x.Hour);

    entity.Property(x => x.CountState)
        .HasAggregateFunction("count", typeof(ulong));

    entity.Property(x => x.SumAmountState)
        .HasAggregateFunction("sum", typeof(long));

    entity.Property(x => x.AvgResponseTimeState)
        .HasAggregateFunction("avg", typeof(double));
});
```

**Generated SQL:**

```sql
CountState AggregateFunction(count, UInt64),
SumAmountState AggregateFunction(sum, Int64),
AvgResponseTimeState AggregateFunction(avg, Float64)
```

### Raw Type Declaration

For complex ClickHouse types that don't have a direct CLR mapping:

```csharp
entity.Property(x => x.UniqueValuesState)
    .HasAggregateFunctionRaw("groupArray", "Array(String)");
```

**Generated SQL:**

```sql
UniqueValuesState AggregateFunction(groupArray, Array(String))
```

---

## DateTime Timezone

Configures IANA timezone for `DateTimeOffset` properties. Values are stored as UTC in ClickHouse; the timezone determines offset calculation when reading, including DST transitions.

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.Property(x => x.CreatedAt)
        .HasTimeZone("America/New_York");

    entity.Property(x => x.ScheduledAt)
        .HasTimeZone("Europe/London");
});
```

**Generated SQL:**

```sql
CreatedAt DateTime64(3, 'America/New_York'),
ScheduledAt DateTime64(3, 'Europe/London')
```

If no timezone is specified, UTC is used and all offsets are zero. Both `DateTimeOffset` and `DateTimeOffset?` are supported.

---

## JSON Column Options

Requires ClickHouse 24.8+ for native JSON type support.

### Dynamic Path and Type Limits

```csharp
entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasMaxDynamicPaths(2048);

entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasMaxDynamicTypes(64);

// Combined configuration
entity.Property(x => x.Metadata)
    .HasColumnType("JSON")
    .HasJsonOptions(maxDynamicPaths: 1024, maxDynamicTypes: 32);
```

### Typed JSON with POCO Mapping

```csharp
public class OrderMetadata
{
    public string CustomerName { get; set; } = "";
    public string ShippingAddress { get; set; } = "";
}

entity.Property(x => x.Metadata)
    .HasTypedJson<OrderMetadata>();
```

When marked as typed JSON, POCO property access in LINQ is translated to ClickHouse subcolumn syntax:

```csharp
context.Orders.Select(o => o.Metadata.CustomerName)
// SQL: SELECT "Metadata"."CustomerName" FROM ...
```

`HasTypedJson<T>()` automatically sets the column type to `JSON`.

---

## Complete Example

```csharp
modelBuilder.Entity<SensorReading>(entity =>
{
    entity.HasKey(x => x.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.DeviceId });

    // Compression codecs
    entity.Property(x => x.Timestamp)
        .HasCodec(c => c.DoubleDelta().LZ4());

    entity.Property(x => x.Temperature)
        .HasCodec(c => c.Gorilla().ZSTD(1));

    // LowCardinality for repeated string values
    entity.Property(x => x.DeviceType)
        .HasLowCardinality();

    entity.Property(x => x.Unit)
        .HasLowCardinalityFixedString(3);

    // Computed columns
    entity.Property(x => x.ReadingDate)
        .HasMaterializedExpression("toDate(Timestamp)");

    entity.Property(x => x.CreatedAt)
        .HasDefaultExpression("now()");

    // DefaultForNull with sentinel
    entity.Property(x => x.Humidity)
        .HasDefaultForNull(0.0);
});
```

---

## See Also

- [Skip Indices](skip-indices.md) -- data skipping indices for query acceleration
- [Projections](projections.md) -- pre-computed aggregations and alternative sort orders
- [Partitioning](partitioning.md) -- PARTITION BY strategies
- [TTL](ttl.md) -- automatic data expiration
