# KeeperMap Engine

The KeeperMap engine provides a linearly-consistent key-value store backed by ClickHouse Keeper (or ZooKeeper). Each row is addressed by a single PRIMARY KEY column, and inserting a row whose key already exists **replaces** the previous value atomically — only one row ever persists per key.

## Basic Configuration

```csharp
modelBuilder.Entity<KvItem>(entity =>
{
    entity.HasKey(e => e.Key);
    entity.UseKeeperMapEngine("/keeper_map_tables/kv", x => x.Key);
});
```

```sql
CREATE TABLE "KvItems" (
    "Key"   String,
    "Value" String
)
ENGINE = KeeperMap('/keeper_map_tables/kv')
PRIMARY KEY ("Key")
```

> **Note:** KeeperMap does not accept `ORDER BY`, `PARTITION BY`, `SAMPLE BY`, or `TTL`. It requires exactly one PRIMARY KEY column.

## Upsert Semantics

Unlike `ReplacingMergeTree` (which keeps all versions on disk and deduplicates during background merges), KeeperMap stores only one row per key at all times. `INSERT` on an existing key overwrites the previous row immediately — no merges, no `.Final()`, no version column.

```csharp
context.Items.Add(new KvItem { Key = "alpha", Value = "v1" });
await context.SaveChangesAsync();

await context.Database.ExecuteSqlRawAsync(
    @"INSERT INTO ""KvItems"" (""Key"", ""Value"") VALUES ('alpha', 'v2')");

await context.Database.ExecuteSqlRawAsync(
    @"INSERT INTO ""KvItems"" (""Key"", ""Value"") VALUES ('alpha', 'v3')");

var count = await context.Items.LongCountAsync();        // 1
var latest = await context.Items.SingleAsync();          // Value == "v3"
```

## Server-Side Requirements

KeeperMap requires two pieces of server configuration:

1. A reachable ClickHouse Keeper or ZooKeeper instance (an embedded Keeper on a single-node server is sufficient).
2. `<keeper_map_path_prefix>` set in `config.xml`, defining the namespace under which KeeperMap tables may create their trees.

```xml
<clickhouse>
    <zookeeper>
        <node><host>keeper-host</host><port>9181</port></node>
    </zookeeper>
    <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
```

The `rootPath` argument you pass to `UseKeeperMapEngine` must start with the configured `keeper_map_path_prefix`; otherwise ClickHouse rejects the `CREATE TABLE`.

## Optional Keys Limit

You can cap the number of keys a KeeperMap table will store. Inserts beyond the limit are rejected.

```csharp
entity.UseKeeperMapEngine("/keeper_map_tables/kv", x => x.Key, keysLimit: 10000);
```

```sql
ENGINE = KeeperMap('/keeper_map_tables/kv', 10000)
```

## When to Use

KeeperMap is a good fit when:

- You need a small, consistent key-value lookup (feature flags, config, rate-limit counters, distributed locks)
- You want atomic upserts without the complexity of `ReplacingMergeTree` + `.Final()`
- You need the same logical table visible from multiple ClickHouse nodes without replicating MergeTree parts

It is not suitable when:

- You need columnar analytics, large scans, or complex ORDER BY
- You have composite primary keys (KeeperMap supports exactly one PK column)
- Your dataset is large enough that holding every row in Keeper becomes expensive — KeeperMap scales with Keeper, not with MergeTree's disk-backed parts

## Characteristics

| Property              | Value                                |
|-----------------------|--------------------------------------|
| Stores data           | Yes (in Keeper / ZooKeeper)          |
| Persists across restarts | Yes (as long as Keeper persists)  |
| ORDER BY required     | No                                   |
| PRIMARY KEY required  | Yes (exactly one column)             |
| PARTITION BY / TTL    | Not supported                        |
| Upsert on duplicate key | Yes, atomic                        |
| Concurrent reads      | Yes                                  |
| Concurrent writes     | Yes, linearizable via Keeper         |
| Replication model     | Via shared Keeper path, not via MergeTree replication |

## See Also

- [Memory Engine](memory-engine.md) -- RAM-only key-value-like storage (non-persistent)
- [ReplacingMergeTree](replacing-mergetree.md) -- "latest row per key" via background merges
- [Null Engine](null-engine.md) -- discard inserts entirely
