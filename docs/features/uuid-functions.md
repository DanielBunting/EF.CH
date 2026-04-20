# Identifier Functions

EF.CH supports server-side identifier generation in ClickHouse — UUIDs (v4, v7), ULIDs, Snowflake IDs, and Keeper-backed serial counters. These come in two flavours:

- **LINQ scalars** — call from inside a query `Select`/`Where` (e.g. `EF.Functions.NewGuidV7()`)
- **Column `DEFAULT` helpers** — wire a generator as the column default so the server populates it on INSERT

## Available LINQ Functions

| C# Method | ClickHouse SQL | Description |
|-----------|---------------|-------------|
| `Guid.NewGuid()` | `generateUUIDv4()` | Random UUID v4 (built-in EF Core) |
| `EF.Functions.NewGuidV7()` | `generateUUIDv7()` | Time-sortable UUID v7 |
| `EF.Functions.GenerateSerialID("counter")` | `generateSerialID('counter')` | Keeper-backed monotonic UInt64 |

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

For auto-generated IDs, wire the generator as the column `DEFAULT`. EF.CH provides strongly-typed fluent helpers that also set `ValueGeneratedOnAdd`, so EF omits the column from INSERTs and the server populates it:

| Helper | ClickHouse expression | CLR type |
|---|---|---|
| `HasSerialIDDefault("counter")` | `generateSerialID('counter')` | `ulong` (UInt64) |
| `HasUuidV4Default()` | `generateUUIDv4()` | `Guid` |
| `HasUuidV7Default()` | `generateUUIDv7()` | `Guid` |
| `HasUlidDefault()` | `generateULID()` | `string` |
| `HasSnowflakeIDDefault()` | `generateSnowflakeID()` | `long` (Int64) |

```csharp
modelBuilder.Entity<Event>(entity =>
{
    entity.Property(e => e.Id).HasUuidV7Default();
    entity.Property(e => e.SerialNumber).HasSerialIDDefault("events_counter");
});
```

For a generator not in the table above, fall back to the raw form:

```csharp
entity.Property(e => e.Id).HasDefaultExpression("generateUUIDv7()");
```

A runnable end-to-end example lives in [samples/IdentifierDefaultsSample](../../samples/IdentifierDefaultsSample).

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

- `NewGuidV7()` and `GenerateSerialID(...)` generate a new value per row — they are non-deterministic.
- UUID v7 requires ClickHouse 23.8+.
- `generateSerialID` requires ClickHouse Keeper to be configured on the server.
- For primary keys, prefer the `Has*Default()` helpers over calling the generator in queries.
- The existing `Guid.NewGuid()` → `generateUUIDv4()` translation continues to work as before.

## Learn More

- [ClickHouse UUID Functions](https://clickhouse.com/docs/en/sql-reference/functions/uuid-functions)
- [RFC 9562 — UUID v7](https://www.rfc-editor.org/rfc/rfc9562)
- [Computed Columns](computed-columns.md) — for `HasDefaultExpression` usage
