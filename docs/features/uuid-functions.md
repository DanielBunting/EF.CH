# UUID Functions

EF.CH supports server-side UUID generation in ClickHouse. In addition to the standard `Guid.NewGuid()` (which maps to `generateUUIDv4()`), EF.CH provides `EF.Functions.NewGuidV7()` for time-sortable UUID v7 generation.

## Available Functions

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `Guid.NewGuid()` | `generateUUIDv4()` | Random UUID v4 (built-in EF Core) |
| `EF.Functions.NewGuidV7()` | `generateUUIDv7()` | Time-sortable UUID v7 |

## Why UUID v7?

UUID v4 is random — it provides no ordering guarantee. UUID v7 embeds a timestamp in the most significant bits, which means:

1. **Natural time ordering** — UUIDs sort chronologically without a separate timestamp column
2. **Better index performance** — sequential inserts avoid random I/O in B-tree indices
3. **Built-in timestamp** — the creation time is encoded in the UUID itself

## Usage Examples

### Server-Side UUID Generation

```csharp
using EF.CH.Extensions;

// Generate UUIDv7 in SELECT
var events = await context.Events
    .Select(e => new
    {
        e.Id,
        TraceId = EF.Functions.NewGuidV7()
    })
    .ToListAsync();
```

Generates:
```sql
SELECT "Id", generateUUIDv7() AS "TraceId"
FROM "Events"
```

### Default Column Values

For auto-generated IDs, use `HasDefaultExpression` instead:

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.Property(e => e.Id)
        .HasDefaultExpression("generateUUIDv7()");
});
```

This generates the UUID server-side during INSERT, which is typically more appropriate than generating it in SELECT queries.

### Comparing v4 and v7

```csharp
var comparison = await context.Events
    .Select(e => new
    {
        V4 = Guid.NewGuid(),         // generateUUIDv4() — random
        V7 = EF.Functions.NewGuidV7() // generateUUIDv7() — time-sorted
    })
    .Take(5)
    .ToListAsync();

// V7 UUIDs will sort chronologically:
// 019c6c34-397e-7acf-... (earlier)
// 019c6c34-397e-7acf-... (later)
```

## Notes

- `NewGuidV7()` generates a new UUID on every row — it's non-deterministic.
- UUID v7 requires ClickHouse 23.8+.
- For primary keys, prefer `HasDefaultExpression("generateUUIDv7()")` over generating in queries.
- The existing `Guid.NewGuid()` → `generateUUIDv4()` translation continues to work as before.

## Learn More

- [ClickHouse UUID Functions](https://clickhouse.com/docs/en/sql-reference/functions/uuid-functions)
- [RFC 9562 — UUID v7](https://www.rfc-editor.org/rfc/rfc9562)
- [Computed Columns](computed-columns.md) — for `HasDefaultExpression` usage
