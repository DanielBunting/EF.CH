# OPTIMIZE TABLE

`OPTIMIZE TABLE` triggers a manual merge of data parts in a MergeTree table. This forces ClickHouse to apply background merge operations immediately rather than waiting for the automatic merge scheduler. Use it to physically remove deleted rows, deduplicate data in ReplacingMergeTree, collapse rows in CollapsingMergeTree, or reclaim disk space after mutations.

All extension methods live in the `EF.CH.Extensions` namespace.

```csharp
using EF.CH.Extensions;
```

## Basic Optimize

Trigger a merge of data parts for a table:

```csharp
await context.Database.OptimizeTableAsync<Event>();
```

Generated SQL:

```sql
OPTIMIZE TABLE "Events"
```

This is a best-effort operation. ClickHouse merges parts if it determines a merge is beneficial, but it may choose not to merge if parts are already optimal.

## OPTIMIZE FINAL

Force a complete merge regardless of whether ClickHouse considers it necessary:

```csharp
await context.Database.OptimizeTableFinalAsync<Event>();
```

Generated SQL:

```sql
OPTIMIZE TABLE "Events" FINAL
```

FINAL guarantees all parts are merged into a single part (per partition). This is the most thorough but also the most expensive operation. Use it during maintenance windows, not during production traffic.

## Partition-Scoped Optimize

Optimize a single partition instead of the entire table:

```csharp
// Monthly partition
await context.Database.OptimizeTablePartitionAsync<Event>("202401");

// Daily partition
await context.Database.OptimizeTablePartitionAsync<Event>("20240115");
```

Generated SQL:

```sql
OPTIMIZE TABLE "Events" PARTITION '202401'
```

Partition-scoped optimization is faster than table-wide optimization and limits the I/O impact to a single partition.

## Partition with FINAL

```csharp
await context.Database.OptimizeTablePartitionFinalAsync<Event>("202401");
```

Generated SQL:

```sql
OPTIMIZE TABLE "Events" PARTITION '202401' FINAL
```

## Fluent Configuration

The `OptimizeTableAsync` overload with options provides full control over the operation:

```csharp
await context.Database.OptimizeTableAsync<Event>(o => o
    .WithFinal()
    .WithPartition("202401")
    .WithDeduplicate());
```

Generated SQL:

```sql
OPTIMIZE TABLE "Events" PARTITION '202401' FINAL DEDUPLICATE
```

### Deduplicate by Specific Columns

```csharp
await context.Database.OptimizeTableAsync<Event>(o => o
    .WithFinal()
    .WithDeduplicate("Id", "Timestamp"));
```

Generated SQL:

```sql
OPTIMIZE TABLE "Events" FINAL DEDUPLICATE BY "Id", "Timestamp"
```

When columns are specified, only those columns are compared for deduplication. Rows that match on all specified columns are collapsed into one. When no columns are specified, all columns are compared.

## Options Reference

| Method | Effect |
|--------|--------|
| `WithFinal()` | Forces a complete merge |
| `WithPartition(id)` | Limits optimization to one partition |
| `WithDeduplicate()` | Removes exact duplicate rows |
| `WithDeduplicate(columns)` | Removes duplicates by specified columns |

All options return the same `OptimizeTableOptions` instance for fluent chaining.

## Optimize by Table Name

If you need to optimize a table that is not mapped as an entity, use the string-based overloads:

```csharp
await context.Database.OptimizeTableAsync("raw_events");
await context.Database.OptimizeTableFinalAsync("raw_events");
await context.Database.OptimizeTablePartitionAsync("raw_events", "202401");
await context.Database.OptimizeTableAsync("raw_events", o => o
    .WithFinal()
    .WithDeduplicate());
```

## When to Optimize

| Scenario | Recommendation |
|----------|----------------|
| After bulk inserts | Optimize to merge small parts created by multiple batches |
| After DELETE mutations | Optimize FINAL to physically remove deleted rows |
| After UPDATE mutations | Optimize to apply pending mutations faster |
| ReplacingMergeTree dedup | Optimize FINAL to collapse duplicate rows |
| CollapsingMergeTree | Optimize FINAL to collapse +1/-1 pairs |
| Production traffic | Let ClickHouse auto-merge; manual optimization adds I/O load |

## See Also

- [Delete Operations](delete-operations.md) -- Lightweight and mutation deletes
- [Update Operations](update-operations.md) -- ALTER TABLE UPDATE mutations
- [Bulk Insert](bulk-insert.md) -- High-throughput inserts that create many small parts
