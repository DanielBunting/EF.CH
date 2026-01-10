# Data Skipping Indices

Data skipping indices (also called secondary indices) allow ClickHouse to skip reading granules that don't match query predicates. Unlike traditional B-tree indices, skip indices store aggregate information about data blocks and eliminate irrelevant blocks before reading.

## Why Use Skip Indices?

1. **Query Acceleration**: Skip granules that don't contain matching data
2. **Reduced I/O**: Read less data from disk for selective queries
3. **Flexible Index Types**: Different algorithms for different data patterns
4. **Low Overhead**: Indices are small compared to the data they index

## Index Types Overview

| Type | ClickHouse Syntax | Best For |
|------|-------------------|----------|
| `Minmax` | `TYPE minmax` | Numeric/datetime range queries |
| `BloomFilter` | `TYPE bloom_filter(fpp)` | Exact value matching, array membership |
| `TokenBF` | `TYPE tokenbf_v1(size, hashes, seed)` | Tokenized text search (logs, URLs) |
| `NgramBF` | `TYPE ngrambf_v1(n, size, hashes, seed)` | Fuzzy/substring text matching |
| `Set` | `TYPE set(max_rows)` | Low-cardinality exact matching |

## Configuration

### Fluent API

```csharp
modelBuilder.Entity<LogEvent>(entity =>
{
    entity.UseMergeTree(x => new { x.Timestamp, x.Id });

    // Minmax index for datetime ranges
    entity.HasIndex(x => x.Timestamp)
        .UseMinmax()
        .HasGranularity(4);

    // Bloom filter for array membership (has(Tags, 'error'))
    entity.HasIndex(x => x.Tags)
        .UseBloomFilter(falsePositive: 0.025)
        .HasGranularity(3);

    // Token bloom filter for log search (LIKE '%error%')
    entity.HasIndex(x => x.Message)
        .UseTokenBF(size: 10240, hashes: 3, seed: 0)
        .HasGranularity(4);

    // N-gram bloom filter for fuzzy matching
    entity.HasIndex(x => x.Description)
        .UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0)
        .HasGranularity(5);

    // Set index for low-cardinality columns
    entity.HasIndex(x => x.Status)
        .UseSet(maxRows: 100)
        .HasGranularity(2);
});
```

### Attribute Decorators

```csharp
public class LogEvent
{
    public Guid Id { get; set; }

    [MinMaxIndex(Granularity = 4)]
    public DateTime Timestamp { get; set; }

    [BloomFilterIndex(FalsePositive = 0.025, Granularity = 3)]
    public string[] Tags { get; set; } = [];

    [TokenBFIndex(Granularity = 4)]
    public string Message { get; set; } = string.Empty;

    [NgramBFIndex(NgramSize = 4, Granularity = 5)]
    public string Description { get; set; } = string.Empty;

    [SetIndex(MaxRows = 100, Granularity = 2)]
    public string Status { get; set; } = string.Empty;
}
```

### Custom Index Name

```csharp
// Fluent API with explicit name
entity.HasIndex(x => x.Tags, "idx_tags_bloom")
    .UseBloomFilter(falsePositive: 0.025)
    .HasGranularity(3);

// Attribute with explicit name
[BloomFilterIndex(Name = "idx_tags_bloom", FalsePositive = 0.025, Granularity = 3)]
public string[] Tags { get; set; } = [];
```

## Index Types In Detail

### Minmax

Stores minimum and maximum values per granule. Best for range queries on ordered data.

```csharp
// Fluent API
entity.HasIndex(x => x.CreatedAt)
    .UseMinmax()
    .HasGranularity(4);

// Attribute
[MinMaxIndex(Granularity = 4)]
public DateTime CreatedAt { get; set; }
```

**Use Cases:**
- Datetime range filtering (`WHERE Timestamp > '2024-01-01'`)
- Numeric range queries (`WHERE Amount BETWEEN 100 AND 500`)
- Ordered primary key columns

**Generated DDL:**
```sql
ALTER TABLE "log_events" ADD INDEX "IX_log_events_CreatedAt" ("CreatedAt") TYPE minmax GRANULARITY 4;
```

### Bloom Filter

Probabilistic data structure for set membership testing. Returns "definitely not in set" or "possibly in set".

```csharp
// Fluent API
entity.HasIndex(x => x.Tags)
    .UseBloomFilter(falsePositive: 0.025)  // 2.5% false positive rate
    .HasGranularity(3);

// Attribute
[BloomFilterIndex(FalsePositive = 0.025, Granularity = 3)]
public string[] Tags { get; set; } = [];
```

**Parameters:**
- `falsePositive` (0.001-0.5, default: 0.025): Probability of false positives. Lower = more accurate but larger index.

**Use Cases:**
- Array membership (`has(Tags, 'error')`)
- Exact string matching (`WHERE UserId = 'abc123'`)
- High-cardinality equality checks

**Generated DDL:**
```sql
ALTER TABLE "log_events" ADD INDEX "IX_log_events_Tags" ("Tags") TYPE bloom_filter(0.025) GRANULARITY 3;
```

### Token Bloom Filter (TokenBF)

Splits strings into tokens (words) and creates a bloom filter. Ideal for log search.

```csharp
// Fluent API
entity.HasIndex(x => x.ErrorMessage)
    .UseTokenBF(size: 10240, hashes: 3, seed: 0)
    .HasGranularity(4);

// Attribute
[TokenBFIndex(Size = 10240, Hashes = 3, Seed = 0, Granularity = 4)]
public string ErrorMessage { get; set; } = string.Empty;
```

**Parameters:**
- `size` (256-1048576, default: 10240): Bloom filter size in bits
- `hashes` (1-10, default: 3): Number of hash functions
- `seed` (default: 0): Random seed for hash functions

**Use Cases:**
- Log message search (`LIKE '%NullReferenceException%'`)
- URL path matching (`LIKE '%/api/users%'`)
- Error message filtering
- Any tokenized text search

**Generated DDL:**
```sql
ALTER TABLE "log_events" ADD INDEX "IX_log_events_ErrorMessage" ("ErrorMessage") TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4;
```

### N-gram Bloom Filter (NgramBF)

Creates bloom filter from character n-grams. Supports substring and fuzzy matching.

```csharp
// Fluent API
entity.HasIndex(x => x.Description)
    .UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0)
    .HasGranularity(5);

// Attribute
[NgramBFIndex(NgramSize = 4, Size = 10240, Hashes = 3, Seed = 0, Granularity = 5)]
public string Description { get; set; } = string.Empty;
```

**Parameters:**
- `ngramSize` (1-10, default: 4): Size of character n-grams
- `size` (256-1048576, default: 10240): Bloom filter size in bits
- `hashes` (1-10, default: 3): Number of hash functions
- `seed` (default: 0): Random seed for hash functions

**Use Cases:**
- Fuzzy text matching
- Substring search within words
- Partial string matching (`LIKE '%substr%'`)

**Generated DDL:**
```sql
ALTER TABLE "log_events" ADD INDEX "IX_log_events_Description" ("Description") TYPE ngrambf_v1(4, 10240, 3, 0) GRANULARITY 5;
```

### Set

Stores unique values per granule. Efficient for low-cardinality columns with exact matching.

```csharp
// Fluent API
entity.HasIndex(x => x.Status)
    .UseSet(maxRows: 100)
    .HasGranularity(2);

// Attribute
[SetIndex(MaxRows = 100, Granularity = 2)]
public string Status { get; set; } = string.Empty;
```

**Parameters:**
- `maxRows` (1-100000, default: 100): Maximum unique values to store per granule

**Use Cases:**
- Status/state columns (`WHERE Status = 'active'`)
- Enum-like columns with limited values
- Any low-cardinality equality filtering

**Generated DDL:**
```sql
ALTER TABLE "log_events" ADD INDEX "IX_log_events_Status" ("Status") TYPE set(100) GRANULARITY 2;
```

## Granularity

Granularity controls how many primary key granules are grouped together for the skip index.

```csharp
entity.HasIndex(x => x.Column)
    .UseMinmax()
    .HasGranularity(4);  // Index covers 4 granules at a time
```

**Guidelines:**
- **Lower granularity (1-2)**: More precise skipping, larger index, more overhead
- **Higher granularity (4-8)**: Less precise skipping, smaller index, less overhead
- **Default: 1** (if not specified)
- **Valid range: 1-1000**

**Choosing Granularity:**
| Data Selectivity | Recommended Granularity |
|------------------|------------------------|
| Very selective (< 1% match) | 1-2 |
| Moderately selective (1-10% match) | 3-4 |
| Less selective (> 10% match) | 4-8 |

## Attribute Reference

| Attribute | Parameters | Defaults |
|-----------|------------|----------|
| `[MinMaxIndex]` | `Granularity`, `Name` | Granularity=1 |
| `[BloomFilterIndex]` | `FalsePositive`, `Granularity`, `Name` | FalsePositive=0.025, Granularity=1 |
| `[TokenBFIndex]` | `Size`, `Hashes`, `Seed`, `Granularity`, `Name` | Size=10240, Hashes=3, Seed=0, Granularity=1 |
| `[NgramBFIndex]` | `NgramSize`, `Size`, `Hashes`, `Seed`, `Granularity`, `Name` | NgramSize=4, Size=10240, Hashes=3, Seed=0, Granularity=1 |
| `[SetIndex]` | `MaxRows`, `Granularity`, `Name` | MaxRows=100, Granularity=1 |

## Complete Example

```csharp
// Entity with multiple skip indices
public class ApplicationLog
{
    public Guid Id { get; set; }

    [MinMaxIndex(Granularity = 2)]
    public DateTime Timestamp { get; set; }

    [SetIndex(MaxRows = 20, Granularity = 2)]
    public string Level { get; set; } = string.Empty;  // Error, Warning, Info, Debug

    [TokenBFIndex(Granularity = 4)]
    public string Message { get; set; } = string.Empty;

    [BloomFilterIndex(FalsePositive = 0.01, Granularity = 3)]
    public string[] Tags { get; set; } = [];

    public string RequestPath { get; set; } = string.Empty;
    public int StatusCode { get; set; }
}

// DbContext configuration
public class LogDbContext : DbContext
{
    public DbSet<ApplicationLog> Logs => Set<ApplicationLog>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ApplicationLog>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.Timestamp, x.Id });
            entity.HasPartitionByDay(x => x.Timestamp);

            // Additional index via fluent API
            entity.HasIndex(x => x.RequestPath)
                .UseTokenBF(size: 10240, hashes: 3, seed: 0)
                .HasGranularity(4);
        });
    }
}
```

## Generated DDL

For the complete example above:

```sql
CREATE TABLE "ApplicationLog" (
    "Id" UUID,
    "Timestamp" DateTime64(3),
    "Level" String,
    "Message" String,
    "Tags" Array(String),
    "RequestPath" String,
    "StatusCode" Int32,
    INDEX "IX_ApplicationLog_Timestamp" ("Timestamp") TYPE minmax GRANULARITY 2,
    INDEX "IX_ApplicationLog_Level" ("Level") TYPE set(20) GRANULARITY 2,
    INDEX "IX_ApplicationLog_Message" ("Message") TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4,
    INDEX "IX_ApplicationLog_Tags" ("Tags") TYPE bloom_filter(0.01) GRANULARITY 3,
    INDEX "IX_ApplicationLog_RequestPath" ("RequestPath") TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD("Timestamp")
ORDER BY ("Timestamp", "Id")
```

## Precedence

Fluent API configuration **overrides** attribute decorators (standard EF Core behavior):

```csharp
public class Example
{
    [BloomFilterIndex(FalsePositive = 0.025)]  // Attribute
    public string[] Tags { get; set; } = [];
}

modelBuilder.Entity<Example>(entity =>
{
    // Fluent API wins - uses Set index, not BloomFilter
    entity.HasIndex(x => x.Tags)
        .UseSet(maxRows: 50)
        .HasGranularity(2);
});
```

## Index Selection Guide

### By Query Pattern

| Query Pattern | Recommended Index |
|---------------|-------------------|
| `WHERE timestamp > '2024-01-01'` | Minmax |
| `WHERE status = 'active'` | Set (if < 100 unique values) or BloomFilter |
| `has(tags, 'error')` | BloomFilter |
| `WHERE message LIKE '%exception%'` | TokenBF |
| `WHERE name LIKE '%john%'` | NgramBF |

### By Column Characteristics

| Column Type | Recommended Index |
|-------------|-------------------|
| Timestamps, numeric ranges | Minmax |
| Enum-like, status fields | Set |
| Arrays | BloomFilter |
| Log messages, URLs | TokenBF |
| Names, descriptions | NgramBF |

## Limitations

- **No Unique Indices**: ClickHouse doesn't support unique constraints via indices
- **Write Overhead**: Indices are updated on every insert, adding slight overhead
- **No Expression Indices**: Indices can only be on columns, not expressions (use `lower(column)` requires the column to store lowercased data)
- **Probabilistic**: Bloom filter indices may have false positives (but never false negatives)

## Best Practices

### 1. Start with Minmax

Minmax is the simplest and most efficient. Use it for any column with range queries.

```csharp
// Good first choice for datetime/numeric columns
entity.HasIndex(x => x.CreatedAt).UseMinmax();
```

### 2. Match Index to Query Pattern

Don't add indices speculatively. Add them for queries you actually run:

```csharp
// If you query: WHERE has(Tags, 'error')
entity.HasIndex(x => x.Tags).UseBloomFilter();

// If you query: WHERE Message LIKE '%exception%'
entity.HasIndex(x => x.Message).UseTokenBF();
```

### 3. Use Set for Low Cardinality

When a column has fewer than 100 unique values, Set is more efficient than BloomFilter:

```csharp
// Better than BloomFilter for status columns
entity.HasIndex(x => x.Status).UseSet(maxRows: 20);
```

### 4. Tune False Positive Rate

Lower false positive rates need larger indices:

```csharp
// Default 2.5% is usually fine
entity.HasIndex(x => x.Tags).UseBloomFilter(falsePositive: 0.025);

// Use 1% for critical queries where false positives are costly
entity.HasIndex(x => x.UserId).UseBloomFilter(falsePositive: 0.01);
```

### 5. Test with EXPLAIN

Verify indices are being used:

```sql
EXPLAIN indexes = 1
SELECT * FROM ApplicationLog WHERE has(Tags, 'error');
```

## Migration Support

Skip indices are included in EF Core migrations. When you add or modify indices, the migration will generate appropriate `ALTER TABLE ADD INDEX` statements.

```csharp
// Adding a new index generates:
// ALTER TABLE "logs" ADD INDEX "IX_logs_Tags" ("Tags") TYPE bloom_filter(0.025) GRANULARITY 3;
```

## See Also

- [Partitioning](partitioning.md)
- [MergeTree Engine](../engines/mergetree.md)
- [Compression Codecs](compression-codecs.md)
- [ClickHouse Data Skipping Indexes](https://clickhouse.com/docs/en/optimize/skipping-indexes)
