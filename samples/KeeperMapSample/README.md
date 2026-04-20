# KeeperMap Sample

Demonstrates the `KeeperMap` engine — a linearly-consistent key-value store backed by ClickHouse Keeper — with a feature-flag style workload.

## What it demonstrates

- Configuring `UseKeeperMapEngine(rootPath, primaryKey)` on an entity
- **Upsert semantics**: inserting the same key multiple times leaves exactly one row (the last-written value wins, atomically)
- **Multi-key workflow**: three distinct keys produce three rows, and each key keeps the one-row-per-key invariant
- Updating a value by simply re-inserting the same primary key (no `ALTER TABLE UPDATE` required)
- Bootstrapping a self-contained single-node ClickHouse with **embedded Keeper** + `keeper_map_path_prefix` via `Testcontainers.ClickHouse`

## Prerequisites

- [.NET SDK](https://dotnet.microsoft.com/download) matching the project's `TargetFramework`
- Docker (the sample launches ClickHouse in a container automatically)

## How to run

```bash
dotnet run
```

## Expected output

```
Starting ClickHouse container (with embedded Keeper)...
ClickHouse container started.

=== EF.CH KeeperMap Sample ===

[1] Creating KeeperMap-backed table 'FeatureFlags'...
    Table created.

[2] Inserting key 'beta-search' three times with different values...
    INSERT  beta-search  Enabled=false  RolloutPct=0
    INSERT  beta-search  Enabled=true   RolloutPct=10
    INSERT  beta-search  Enabled=true   RolloutPct=100

    Total rows in table : 1   <-- three inserts, one row
    Final value         : Enabled=True  RolloutPct=100

[3] Seeding three more flags via EF Core tracking...
    Total rows in table : 4

    Current flag state:
      ai-suggest       Enabled=True   RolloutPct=25
      beta-search      Enabled=True   RolloutPct=100
      dark-mode        Enabled=True   RolloutPct=100
      new-onboarding   Enabled=False  RolloutPct=0

[4] Rolling 'ai-suggest' from 25% -> 75% via INSERT on the same key...
    ai-suggest now Enabled=True  RolloutPct=75
    Total rows still    : 4   <-- upsert, not append

=== Done ===
```

## Contrast with ReplacingMergeTree

`ReplacingMergeTree` gives similar "last write wins" behaviour, but stores every version on disk and only deduplicates during background merges — queries see duplicates until a merge runs, so you need `.Final()` or a version column. `KeeperMap` holds exactly one row per key at all times, backed by Keeper, with no merges and no `.Final()`.

Use `KeeperMap` for small, high-consistency key-value data (feature flags, config, distributed locks). Use `ReplacingMergeTree` when the dataset is too large to keep entirely in Keeper.
