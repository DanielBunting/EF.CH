# Skip Indices

Data skipping indices accelerate queries by allowing ClickHouse to skip entire granules (blocks of rows) that cannot match a filter condition. Unlike traditional database indices that point to individual rows, skip indices store lightweight summary information per granule and eliminate irrelevant data blocks before any row-level processing.

All skip index types support `.HasGranularity(n)` to control how many granules are summarized per index entry.

---

## Index Types

### Minmax

Stores minimum and maximum values per granule. Best for range queries on numeric and date columns.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasIndex(x => x.Amount)
        .UseMinmax()
        .HasGranularity(3);

    entity.HasIndex(x => x.OrderDate)
        .UseMinmax()
        .HasGranularity(2);
});
```

**Generated SQL:**

```sql
INDEX ix_Amount Amount TYPE minmax GRANULARITY 3,
INDEX ix_OrderDate OrderDate TYPE minmax GRANULARITY 2
```

Effective for queries like:

```csharp
context.Orders.Where(o => o.Amount >= 100 && o.Amount <= 500)
context.Orders.Where(o => o.OrderDate >= startDate)
```

### Bloom Filter

Probabilistic data structure for exact value matching and array contains checks. The false positive rate controls the tradeoff between index size and accuracy.

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.HasIndex(x => x.EventType)
        .UseBloomFilter(falsePositive: 0.01)
        .HasGranularity(3);

    // Default false positive rate: 0.025 (2.5%)
    entity.HasIndex(x => x.UserId)
        .UseBloomFilter();
});
```

**Generated SQL:**

```sql
INDEX ix_EventType EventType TYPE bloom_filter(0.01) GRANULARITY 3,
INDEX ix_UserId UserId TYPE bloom_filter(0.025) GRANULARITY 1
```

Effective for queries like:

```csharp
context.Events.Where(e => e.EventType == "purchase")
context.Events.Where(e => e.Tags.Contains("urgent"))
```

Parameters:
- `falsePositive`: Rate between 0.001 and 0.5. Lower values use more memory but have fewer false positives. Default: 0.025.

### TokenBF

Tokenizes strings by non-alphanumeric separators and stores tokens in a bloom filter. Best for searching structured text like log messages, URLs, and delimited fields.

```csharp
modelBuilder.Entity<LogEntry>(entity =>
{
    entity.HasIndex(x => x.Message)
        .UseTokenBF(size: 10240, hashes: 3, seed: 0)
        .HasGranularity(4);
});
```

**Generated SQL:**

```sql
INDEX ix_Message Message TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
```

Effective for queries like:

```csharp
context.Logs.Where(l => EF.Functions.HasToken(l.Message, "error"))
context.Logs.Where(l => EF.Functions.HasToken(l.Message, "timeout"))
```

Parameters:
- `size`: Bloom filter size in bytes (256-1,048,576). Default: 10240.
- `hashes`: Number of hash functions (1-10). Default: 3.
- `seed`: Random seed for hash functions. Default: 0.

### NgramBF

Splits strings into overlapping n-character subsequences and stores them in a bloom filter. Best for substring search and fuzzy text matching.

```csharp
modelBuilder.Entity<Product>(entity =>
{
    entity.HasIndex(x => x.Description)
        .UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0)
        .HasGranularity(5);
});
```

**Generated SQL:**

```sql
INDEX ix_Description Description TYPE ngrambf_v1(4, 10240, 3, 0) GRANULARITY 5
```

Effective for queries like:

```csharp
context.Products.Where(p => p.Description.Contains("wireless"))
context.Products.Where(p => EF.Functions.NgramSearch(p.Description, "wireles") > 0.5f)
```

Parameters:
- `ngramSize`: Length of each n-gram (1-10). Default: 4.
- `size`: Bloom filter size in bytes (256-1,048,576). Default: 10240.
- `hashes`: Number of hash functions (1-10). Default: 3.
- `seed`: Random seed for hash functions. Default: 0.

### Set

Stores all distinct values per granule up to a configurable limit. Provides exact matching with no false positives when the number of distinct values stays below the limit.

```csharp
modelBuilder.Entity<Order>(entity =>
{
    entity.HasIndex(x => x.Status)
        .UseSet(maxRows: 100)
        .HasGranularity(2);
});
```

**Generated SQL:**

```sql
INDEX ix_Status Status TYPE set(100) GRANULARITY 2
```

Effective for queries like:

```csharp
context.Orders.Where(o => o.Status == "shipped")
```

Parameters:
- `maxRows`: Maximum distinct values to store per index block (1-100,000). Default: 100. If the block contains more distinct values than the limit, the index is not used for that block.

---

## Granularity

Granularity controls how many data granules (default 8,192 rows each) are summarized by a single index entry. A granularity of 3 means each index entry covers 3 granules (24,576 rows by default).

```csharp
entity.HasIndex(x => x.Timestamp)
    .UseMinmax()
    .HasGranularity(4);
```

Lower granularity provides finer-grained filtering but uses more memory. Higher granularity saves memory but skips data in larger chunks. Valid range: 1-1000. Default: 1 (when not explicitly set by EF.CH; ClickHouse server default is also 1).

---

## Choosing the Right Index Type

| Query Pattern | Index Type | Example |
|---|---|---|
| `WHERE x BETWEEN a AND b` | Minmax | Date ranges, numeric ranges |
| `WHERE x = value` | Bloom Filter or Set | Exact equality on high-cardinality columns |
| `WHERE hasToken(x, 'term')` | TokenBF | Log message search, URL parameters |
| `WHERE x LIKE '%substring%'` | NgramBF | Substring search, fuzzy matching |
| `WHERE x IN (a, b, c)` | Set | Low-cardinality columns (status, type) |
| `WHERE has(array, value)` | Bloom Filter | Array contains checks |

---

## Complete Example

```csharp
modelBuilder.Entity<LogEntry>(entity =>
{
    entity.HasKey(x => x.Id);
    entity.UseMergeTree(x => new { x.Timestamp, x.ServiceName });

    // Range filtering on timestamp
    entity.HasIndex(x => x.Timestamp)
        .UseMinmax()
        .HasGranularity(2);

    // Exact match on service name (low cardinality)
    entity.HasIndex(x => x.ServiceName)
        .UseSet(maxRows: 50)
        .HasGranularity(3);

    // Token search on log messages
    entity.HasIndex(x => x.Message)
        .UseTokenBF(size: 10240, hashes: 3, seed: 0)
        .HasGranularity(4);

    // Substring search on URL paths
    entity.HasIndex(x => x.RequestPath)
        .UseNgramBF(ngramSize: 4, size: 10240, hashes: 3, seed: 0)
        .HasGranularity(5);

    // Equality filter on trace ID (high cardinality)
    entity.HasIndex(x => x.TraceId)
        .UseBloomFilter(falsePositive: 0.01)
        .HasGranularity(3);
});
```

---

## See Also

- [Column Features](column-features.md) -- compression codecs, computed columns, LowCardinality
- [Partitioning](partitioning.md) -- PARTITION BY strategies for data organization
- [Projections](projections.md) -- pre-computed aggregations and alternative sort orders
