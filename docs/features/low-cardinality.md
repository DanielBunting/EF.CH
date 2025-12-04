# LowCardinality

LowCardinality is a storage optimization for string columns with few unique values. It uses dictionary encoding to reduce storage size and improve query performance.

## When to Use

Ideal for columns with limited unique values (typically <10,000):

- Status codes (`"pending"`, `"completed"`, `"failed"`)
- Country/region codes (`"US"`, `"UK"`, `"DE"`)
- Category names
- Environment names (`"prod"`, `"staging"`, `"dev"`)
- HTTP methods (`"GET"`, `"POST"`, `"PUT"`)

## Configuration

### Basic LowCardinality

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.Property(e => e.Status)
        .HasLowCardinality();

    entity.Property(e => e.CountryCode)
        .HasLowCardinality();
});
```

Generates:
```sql
"Status" LowCardinality(String)
"CountryCode" LowCardinality(String)
```

### With FixedString

For known fixed-length strings:

```csharp
modelBuilder.Entity<Country>(entity =>
{
    // ISO 2-letter codes: "US", "UK", "DE"
    entity.Property(e => e.IsoCode)
        .HasLowCardinalityFixedString(2);

    // Currency codes: "USD", "EUR", "GBP"
    entity.Property(e => e.CurrencyCode)
        .HasLowCardinalityFixedString(3);
});
```

Generates:
```sql
"IsoCode" LowCardinality(FixedString(2))
"CurrencyCode" LowCardinality(FixedString(3))
```

## Complete Example

```csharp
public class ApiRequest
{
    public Guid Id { get; set; }
    public DateTime Timestamp { get; set; }
    public string Method { get; set; } = string.Empty;      // GET, POST, PUT, DELETE
    public string Endpoint { get; set; } = string.Empty;
    public string StatusCategory { get; set; } = string.Empty;  // 2xx, 4xx, 5xx
    public int StatusCode { get; set; }
    public int ResponseTimeMs { get; set; }
}

modelBuilder.Entity<ApiRequest>(entity =>
{
    entity.HasKey(e => e.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Low cardinality strings
    entity.Property(e => e.Method)
        .HasLowCardinality();

    entity.Property(e => e.StatusCategory)
        .HasLowCardinality();
});
```

## Generated DDL

```sql
CREATE TABLE "ApiRequests" (
    "Id" UUID NOT NULL,
    "Timestamp" DateTime64(3) NOT NULL,
    "Method" LowCardinality(String) NOT NULL,
    "Endpoint" String NOT NULL,
    "StatusCategory" LowCardinality(String) NOT NULL,
    "StatusCode" Int32 NOT NULL,
    "ResponseTimeMs" Int32 NOT NULL
)
ENGINE = MergeTree
ORDER BY ("Timestamp", "Id")
```

## How It Works

1. ClickHouse builds a dictionary of unique values
2. Each row stores an index into the dictionary instead of the full string
3. Queries compare dictionary indices, not string bytes

```
Without LowCardinality:
Row 1: "pending"  (7 bytes)
Row 2: "pending"  (7 bytes)
Row 3: "complete" (8 bytes)

With LowCardinality:
Dictionary: { 0: "pending", 1: "complete" }
Row 1: 0  (index)
Row 2: 0  (index)
Row 3: 1  (index)
```

## Benefits

| Aspect | Impact |
|--------|--------|
| Storage | 10-100x smaller for low-cardinality columns |
| Memory | Less RAM during queries |
| Query Speed | Faster filtering and grouping |
| Compression | Better compression ratios |

## Nullable Columns

For nullable string properties, the type is automatically adjusted:

```csharp
public class Order
{
    public string? CouponCode { get; set; }  // Nullable
}

entity.Property(e => e.CouponCode)
    .HasLowCardinality();
```

Generates:
```sql
"CouponCode" LowCardinality(Nullable(String))
```

## Cardinality Guidelines

| Unique Values | Recommendation |
|---------------|----------------|
| < 1,000 | Excellent fit |
| 1,000 - 10,000 | Good fit |
| 10,000 - 100,000 | Consider testing |
| > 100,000 | Likely not beneficial |

## Don't Use For

- High-cardinality columns (user IDs, email addresses)
- Columns with many unique values
- Columns where values change frequently

```csharp
// Bad: High cardinality
entity.Property(e => e.UserId).HasLowCardinality();     // Millions of unique values
entity.Property(e => e.Email).HasLowCardinality();      // Unique per user
entity.Property(e => e.SessionId).HasLowCardinality();  // Unique per session

// Good: Low cardinality
entity.Property(e => e.Status).HasLowCardinality();        // ~5 values
entity.Property(e => e.Country).HasLowCardinality();       // ~200 values
entity.Property(e => e.DeviceType).HasLowCardinality();    // ~10 values
```

## Querying

Queries work normally - no special syntax needed:

```csharp
// Filtering
var postRequests = await context.ApiRequests
    .Where(r => r.Method == "POST")
    .ToListAsync();

// Grouping
var byMethod = await context.ApiRequests
    .GroupBy(r => r.Method)
    .Select(g => new { Method = g.Key, Count = g.Count() })
    .ToListAsync();
```

## Best Practices

### Combine with Partitioning

```csharp
entity.HasPartitionByMonth(x => x.Timestamp);
entity.Property(e => e.Status).HasLowCardinality();
```

### Use for Dimension Columns

In analytics, dimension columns (categories for grouping) are often low cardinality:

```csharp
entity.Property(e => e.Country).HasLowCardinality();
entity.Property(e => e.ProductCategory).HasLowCardinality();
entity.Property(e => e.Source).HasLowCardinality();
// Metrics columns don't need LowCardinality
// entity.Property(e => e.Revenue) - numeric, not string
```

## See Also

- [Type Mappings](../types/overview.md)
- [ClickHouse LowCardinality Docs](https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality)
