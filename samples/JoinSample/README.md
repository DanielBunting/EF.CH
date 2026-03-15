# Join Sample

Demonstrates ARRAY JOIN and ASOF JOIN extensions for unnesting array columns and performing closest-match time-series joins in ClickHouse.

## Features Covered

1. **ARRAY JOIN** -- Explode array columns into individual rows, skipping empty arrays
2. **LEFT ARRAY JOIN** -- Preserve rows with empty arrays using default values
3. **ARRAY JOIN + WHERE** -- Filter on unnested column values after expansion
4. **Multiple ARRAY JOINs** -- Unnest two arrays simultaneously with positional alignment
5. **ASOF JOIN** -- Match events to the closest price at or before each event's timestamp
6. **ASOF LEFT JOIN** -- Preserve all left-side rows, even without matching prices
7. **ARRAY JOIN + FINAL** -- Combine deduplication with array unnesting

## Prerequisites

- Docker (for ClickHouse via Testcontainers)
- .NET 8.0 SDK

## Running

```bash
dotnet run --project samples/JoinSample/
```

## Key Concepts

- The unnested column alias comes from the parameter name in the result selector (e.g., `tag` in `(e, tag) => ...` produces `AS tag` in SQL)
- ASOF JOIN requires the inequality column to be part of the inner table's ORDER BY key
- LEFT variants preserve rows that have no match: LEFT ARRAY JOIN keeps empty arrays, ASOF LEFT JOIN keeps unmatched left rows
- ARRAY JOIN is positional when unnesting multiple arrays -- arrays are aligned element-wise, not as a cartesian product
- All join extensions are composable with other ClickHouse modifiers like FINAL and PREWHERE
