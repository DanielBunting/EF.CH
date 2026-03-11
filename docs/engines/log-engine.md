# Log Family Engines

The Log family consists of three lightweight engines designed for small tables (up to approximately one million rows). They provide simple sequential storage without the overhead of MergeTree's background merges, sorting, or indexing.

## Engines

### Log

The most capable of the three. Each column is stored in a separate file, with a marks file that enables parallel reads. Supports concurrent reads but only sequential writes.

```csharp
modelBuilder.Entity<SmallTable>(entity =>
{
    entity.UseLogEngine();
});
```

```sql
CREATE TABLE "SmallTable" (
    "Id" UUID,
    "Name" String
)
ENGINE = Log
```

### TinyLog

The simplest engine. Each column is stored in a separate file with no marks file, so reads are sequential. Does not support concurrent data access — only one operation (read or write) at a time.

```csharp
modelBuilder.Entity<SmallTable>(entity =>
{
    entity.UseTinyLogEngine();
});
```

```sql
ENGINE = TinyLog
```

### StripeLog

All columns are stored in a single file, which reduces the number of file descriptors. Good for tables with many columns but few rows.

```csharp
modelBuilder.Entity<WideTable>(entity =>
{
    entity.UseStripeLogEngine();
});
```

```sql
ENGINE = StripeLog
```

## Comparison

| Property | Log | TinyLog | StripeLog |
|----------|-----|---------|-----------|
| Column storage | Separate files | Separate files | Single file |
| Marks file | Yes | No | Yes |
| Concurrent reads | Yes | No | Yes |
| Concurrent writes | No | No | No |
| Best for | General small tables | Minimal overhead | Many-column tables |

> **Note:** None of the Log family engines require ORDER BY, PARTITION BY, or any other MergeTree clauses. No parentheses are appended to the engine name.

## When to Use

The Log family is a good choice when:

- Your table has fewer than ~1 million rows
- You write data once and read many times
- You do not need sorting, indexing, or background merges
- You want minimal storage overhead

It is not suitable when:

- You need high-throughput concurrent writes
- Your table will grow beyond ~1 million rows
- You need ORDER BY, PARTITION BY, TTL, or skip indices

## See Also

- [Memory Engine](memory-engine.md) -- RAM-only storage, faster but non-persistent
- [Null Engine](null-engine.md) -- data discarded immediately
- [MergeTree](mergetree.md) -- general-purpose engine for larger tables
