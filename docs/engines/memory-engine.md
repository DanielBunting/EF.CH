# Memory Engine

The Memory engine stores all data in RAM. Data is not persisted to disk and is lost when the ClickHouse server restarts.

## Basic Configuration

```csharp
modelBuilder.Entity<LookupTable>(entity =>
{
    entity.UseMemoryEngine();
});
```

```sql
CREATE TABLE "LookupTable" (
    "Id" UUID,
    "Code" String,
    "Description" String
)
ENGINE = Memory
```

> **Note:** The Memory engine does not require ORDER BY, PARTITION BY, or any other MergeTree clauses. No parentheses are appended to the engine name.

## When to Use

The Memory engine is a good choice when:

- You need a temporary lookup or reference table that fits in RAM
- You are running tests and do not need data to survive restarts
- You need fast read/write access to a small dataset that can be regenerated

It is not suitable when:

- Data must survive server restarts
- The dataset is too large to fit in RAM
- You need replication or fault tolerance

## Characteristics

| Property | Value |
|----------|-------|
| Stores data | Yes (in RAM only) |
| Persists to disk | No |
| ORDER BY required | No |
| Concurrent reads | Yes |
| Concurrent writes | Yes |
| Supports indices | No |

## See Also

- [Null Engine](null-engine.md) -- data discarded immediately (not stored at all)
- [Log Family](log-engine.md) -- simple disk-based storage for small tables
- [MergeTree](mergetree.md) -- general-purpose persistent storage
