# ReplacingMergeTree Engine

ReplacingMergeTree deduplicates rows with the same ORDER BY key during background merges. When multiple rows share the same key, only the row with the highest version column value is kept. Use this engine when you need the latest state of changing entities.

## Basic Configuration

```csharp
modelBuilder.Entity<User>(entity =>
{
    entity.UseReplacingMergeTree(
        x => x.Version,         // version column for deduplication
        x => new { x.Id });     // ORDER BY key
});
```

```sql
CREATE TABLE "Users" (
    "Id" UUID,
    "Name" String,
    "Version" Int64,
    ...
)
ENGINE = ReplacingMergeTree("Version")
ORDER BY ("Id")
```

During background merges, if multiple rows have the same `Id`, only the row with the highest `Version` value survives.

## Without Version Column

When no version column is specified, the last inserted row wins during deduplication.

```csharp
entity.UseReplacingMergeTree(x => new { x.UserId, x.Timestamp });
```

```sql
ENGINE = ReplacingMergeTree()
ORDER BY ("UserId", "Timestamp")
```

## Version with IsDeleted Column (ClickHouse 23.2+)

The `isDeleted` parameter enables physical deletion during merges. When the winning version has `IsDeleted = 1`, the row is removed entirely.

```csharp
modelBuilder.Entity<User>(entity =>
{
    entity.UseReplacingMergeTree(
        x => x.Version,       // version column
        x => x.IsDeleted,     // is_deleted column (UInt8)
        x => new { x.Id });   // ORDER BY key
});
```

```sql
ENGINE = ReplacingMergeTree("Version", "IsDeleted")
ORDER BY ("Id")
```

The `IsDeleted` column should be `byte` (mapped to UInt8), with `0` meaning active and `1` meaning deleted:

```csharp
public class User
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public long Version { get; set; }
    public byte IsDeleted { get; set; }  // UInt8: 0 = active, 1 = deleted
}
```

## String Overloads

```csharp
entity.UseReplacingMergeTree("Version", "Id");
```

```sql
ENGINE = ReplacingMergeTree("Version")
ORDER BY ("Id")
```

## FINAL Modifier

Deduplication only happens during background merges. Before a merge runs, queries may return duplicate rows. Use the `.Final()` query extension to get deduplicated results at query time:

```csharp
var users = await context.Users
    .Final()
    .Where(u => u.Active)
    .ToListAsync();
```

```sql
SELECT ... FROM "Users" FINAL WHERE ...
```

> **Note:** `FINAL` forces on-the-fly deduplication, which can be slower than reading merged data. For large tables, consider running `OPTIMIZE TABLE ... FINAL` periodically instead of using `FINAL` on every query.

When using the `isDeleted` column, `FINAL` automatically excludes rows where `IsDeleted = 1`.

## When to Use

ReplacingMergeTree is the right choice when:

- You need the latest state of entities that change over time (user profiles, product catalogs, configuration)
- You want upsert semantics -- insert a new version and the old one is removed during merges
- You need soft-delete behavior with the `isDeleted` column

It is not suitable when:

- You need immediate consistency (deduplication is eventual, use `FINAL` for on-the-fly dedup)
- You need to aggregate numeric columns automatically (use SummingMergeTree instead)

## Complete Example

```csharp
modelBuilder.Entity<Product>(entity =>
{
    entity.ToTable("products");
    entity.HasKey(e => e.Id);

    entity.UseReplacingMergeTree(
        x => x.Version,
        x => x.IsDeleted,
        x => new { x.Id })
        .HasPartitionByMonth(x => x.UpdatedAt);
});
```

```sql
CREATE TABLE "products" (
    "Id" UUID,
    "Name" String,
    "Price" Decimal(18, 4),
    "Version" Int64,
    "IsDeleted" UInt8,
    "UpdatedAt" DateTime64(3)
)
ENGINE = ReplacingMergeTree("Version", "IsDeleted")
PARTITION BY toYYYYMM("UpdatedAt")
ORDER BY ("Id")
```

Querying the latest state:

```csharp
var activeProducts = await context.Products
    .Final()
    .Where(p => p.Price > 0)
    .ToListAsync();
```

## See Also

- [MergeTree](mergetree.md) -- base engine without deduplication
- [CollapsingMergeTree](collapsing-mergetree.md) -- state tracking with sign column
- [VersionedCollapsingMergeTree](versioned-collapsing.md) -- out-of-order-aware collapsing
