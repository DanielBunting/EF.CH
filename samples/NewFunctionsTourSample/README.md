# New Functions Tour

Demonstrates every category of ClickHouse function added in the
missing-CH-functions rollout (Tiers 1–3 — ~95 new translators).

## Sections

| Tier | Functions exercised |
|---|---|
| 1a | `EF.Functions.MultiIf` |
| 1b | `EF.Functions.ILike` / `Match` / `ReplaceRegexAll` / `Position` |
| 1c | `EF.Functions.ToInt32OrNull` / `ParseDateTimeBestEffortOrNull` |
| 1d | `EF.Functions.DateTrunc` / `ToStartOfInterval` / `ToTimeZone` / `Now64` |
| 2a | `EF.Functions.BitCount` / `BitTest` / `BitHammingDistance` |
| 2b | `EF.Functions.ArrayDistinct` / `ArrayConcat` / `ArraySlice` / `IndexOf` |
| 2c | `EF.Functions.JSONExtractInt/String/Bool` / `JSONHas` / `JSONLength` / `IsValidJSON` |
| 2d | `EF.Functions.RandCanonical` / `RandUniform` |
| 2e | `EF.Functions.Version` / `HostName` / `CurrentDatabase` |
| 3a | `EF.Functions.DotProduct` (vector similarity, raw SQL) |
| 3b | `EF.Functions.IPv6StringToNum` / `IPv6NumToString` round-trip |
| 3c | `EF.Functions.DateTimeToUUIDv7` (client-side) / `UUIDv7ToDateTime` (server-side) |
| 3d | `EF.Functions.Pi` / `Factorial` / `Hypot` / `RoundBankers` |
| 3e | `EF.Functions.Left` / `Right` / `InitCap` / `Repeat` / `LeftPad` |

## Run

Requires ClickHouse listening on `localhost:8123` (the HTTP port — not
9000, which is the native protocol the driver doesn't speak). The sample
uses `Username=clickhouse` / `Password=clickhouse` by default; override
via the `CH_CONN` env var if your server uses different credentials.

The quickest path is Docker:

```bash
docker run --rm -d --name ch \
  -p 8123:8123 -p 9000:9000 \
  -e CLICKHOUSE_USER=clickhouse \
  -e CLICKHOUSE_PASSWORD=clickhouse \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  clickhouse/clickhouse-server:25.6
```

Then build and run:

```bash
dotnet build samples/NewFunctionsTourSample/
dotnet run --project samples/NewFunctionsTourSample/
```

The sample drops + recreates the `new_functions_tour` database on every run,
so it's safe to run repeatedly.

## Notes

- `DateTimeToUUIDv7` is evaluated client-side via .NET 9+'s
  `Guid.CreateVersion7(DateTimeOffset)` and bound as a query parameter, so it
  works on every CH version (the server-side `dateTimeToUUIDv7` builder is
  experimental and not in every stable release).
- `DotProduct` is shown via raw SQL because the sample schema doesn't
  declare a tuple column. A real vector-similarity workload would store
  embeddings as `Tuple(Float32, Float32, …)` or `Array(Float32)` and apply
  `DotProduct` over those columns.
- The seed data is two rows. Larger datasets are out of scope for the tour —
  the goal is "does this translator emit the right SQL and read back?", not
  perf measurement.
