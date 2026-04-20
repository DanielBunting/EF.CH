# Identifier Defaults Sample

Demonstrates five fluent helpers that wire ClickHouse's server-side identifier generators as column `DEFAULT` expressions, so EF omits the columns from `INSERT` and the server populates them.

## What it demonstrates

| Fluent helper | ClickHouse expression | CLR type | Notes |
|---|---|---|---|
| `HasSerialIDDefault("counter")` | `generateSerialID('counter')` | `ulong` (UInt64) | Gap-free monotonic, Keeper-backed |
| `HasUuidV4Default()` | `generateUUIDv4()` | `Guid` | Random |
| `HasUuidV7Default()` | `generateUUIDv7()` | `Guid` | Time-sortable |
| `HasUlidDefault()` | `generateULID()` | `string` | 26-char lexicographically sortable |
| `HasSnowflakeIDDefault()` | `generateSnowflakeID()` | `long` (Int64) | Timestamp + machine + sequence, no Keeper needed |

Each helper also sets `ValueGeneratedOnAdd`, so the column is excluded from `INSERT` statements. The sample also shows the scalar translator `EF.Functions.GenerateSerialID("counter")` being used inside a LINQ `Select`.

> **Note on string columns (ULID):** EF uses the CLR default as the "unset" sentinel. For `string` that sentinel is `null`, not `""` — so a ULID-defaulted column should be declared `string?`, otherwise EF will send an empty string on INSERT and override the server DEFAULT.

> **Note on `SerialId`:** ClickHouse Keeper counters start at **0**, not 1. If you also want the `SerialId` property as a primary key, clear the change tracker between insert batches — the first batch's row comes back with `SerialId = 0` (the same value new unsaved rows carry as the CLR default for `ulong`), which EF's identity map would reject as a duplicate key.

## Prerequisites

- [.NET SDK](https://dotnet.microsoft.com/download) matching the project's `TargetFramework`
- Docker (the sample launches a ClickHouse container with embedded Keeper automatically)

## How to run

```bash
dotnet run
```

## Expected output

```
Starting ClickHouse container (with embedded Keeper)...
ClickHouse container started.

=== EF.CH Identifier Defaults Sample ===

[1] Creating table with five server-generated ID columns...
    Table created.

[2] Inserting three orders with only Amount set...
    Inserted.

[3] Reading back the server-populated IDs:
    Amount=$19.99
      SerialId    (UInt64): 1
      UuidV4      (UUID)  : 7c0c...
      UuidV7      (UUID)  : 019c...
      Ulid        (String): 01JABCXYZ...
      SnowflakeId (Int64) : 7334...
    ...

[4] Inserting two more orders — SerialId continues from where it left off...
    SerialId sequence: [0, 1, 2, 3, 4]

[5] Calling generateSerialID from a LINQ projection...
    Amount=$19.99  AdHocSerial=0
    ...

=== Done ===
```

## Why Keeper?

Only `generateSerialID` needs ClickHouse Keeper — it persists the counter there so the sequence survives restarts and stays consistent across replicas. The UUID / ULID / Snowflake generators are stateless and work on any ClickHouse node.

## When to pick which

- **SerialID** — you need gap-free monotonic integers (invoice numbers, audit sequence)
- **UUIDv4** — you don't care about ordering, just uniqueness
- **UUIDv7** — you want a UUID that also sorts chronologically (good for primary keys on time-series tables)
- **ULID** — like UUIDv7 but as a human-typeable 26-char string
- **SnowflakeID** — you want a 64-bit sortable integer ID without the Keeper coordination cost
